# Copyright (c) 2010-2012 OpenStack, LLC.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import bisect
import itertools
import math

from array import array
from collections import defaultdict
from random import randint, shuffle
from time import time

from swift.common import exceptions
from swift.common.ring import RingData
from swift.common.ring.utils import tiers_for_dev, build_tier_tree


class RingBuilder(object):
    """
    Used to build swift.common.RingData instances to be written to disk and
    used with swift.common.ring.Ring instances. See bin/ring-builder.py for
    example usage.

    The instance variable devs_changed indicates if the device information has
    changed since the last balancing. This can be used by tools to know whether
    a rebalance request is an isolated request or due to added, changed, or
    removed devices.

    :param part_power: number of partitions = 2**part_power
    :param replicas: number of replicas for each partition
    :param min_part_hours: minimum number of hours between partition changes
    """

    def __init__(self, part_power, replicas, min_part_hours):
        self.part_power = part_power
        self.replicas = replicas
        self.min_part_hours = min_part_hours
        self.parts = 2 ** self.part_power
        self.devs = []
        self.devs_changed = False
        self.version = 0

        # _replica2part2dev maps from replica number to partition number to
        # device id. So, for a three replica, 2**23 ring, it's an array of
        # three 2**23 arrays of device ids (unsigned shorts). This can work a
        # bit faster than the 2**23 array of triplet arrays of device ids in
        # many circumstances. Making one big 2**23 * 3 array didn't seem to
        # have any speed change; though you're welcome to try it again (it was
        # a while ago, code-wise, when I last tried it).
        self._replica2part2dev = None

        # _last_part_moves is a 2**23 array of unsigned bytes representing the
        # number of hours since a given partition was last moved. This is used
        # to guarantee we don't move a partition twice within a given number of
        # hours (24 is my usual test). Removing a device or setting its weight
        # to 0 overrides this behavior as it's assumed those actions are done
        # because of device failure.
        # _last_part_moves_epoch indicates the time the offsets in
        # _last_part_moves is based on.
        self._last_part_moves_epoch = None
        self._last_part_moves = None

        self._last_part_gather_start = 0
        self._remove_devs = []
        self._ring = None

    def weighted_parts(self):
        try:
            return self.parts * self.replicas / \
                     sum(d['weight'] for d in self._iter_devs())
        except ZeroDivisionError:
            raise exceptions.EmptyRingError('There are no devices in this '
                                            'ring, or all devices have been '
                                            'deleted')

    def copy_from(self, builder):
        if hasattr(builder, 'devs'):
            self.part_power = builder.part_power
            self.replicas = builder.replicas
            self.min_part_hours = builder.min_part_hours
            self.parts = builder.parts
            self.devs = builder.devs
            self.devs_changed = builder.devs_changed
            self.version = builder.version
            self._replica2part2dev = builder._replica2part2dev
            self._last_part_moves_epoch = builder._last_part_moves_epoch
            self._last_part_moves = builder._last_part_moves
            self._last_part_gather_start = builder._last_part_gather_start
            self._remove_devs = builder._remove_devs
        else:
            self.part_power = builder['part_power']
            self.replicas = builder['replicas']
            self.min_part_hours = builder['min_part_hours']
            self.parts = builder['parts']
            self.devs = builder['devs']
            self.devs_changed = builder['devs_changed']
            self.version = builder['version']
            self._replica2part2dev = builder['_replica2part2dev']
            self._last_part_moves_epoch = builder['_last_part_moves_epoch']
            self._last_part_moves = builder['_last_part_moves']
            self._last_part_gather_start = builder['_last_part_gather_start']
            self._remove_devs = builder['_remove_devs']
        self._ring = None

    def to_dict(self):
        return {'part_power': self.part_power,
                'replicas': self.replicas,
                'min_part_hours': self.min_part_hours,
                'parts': self.parts,
                'devs': self.devs,
                'devs_changed': self.devs_changed,
                'version': self.version,
                '_replica2part2dev': self._replica2part2dev,
                '_last_part_moves_epoch': self._last_part_moves_epoch,
                '_last_part_moves': self._last_part_moves,
                '_last_part_gather_start': self._last_part_gather_start,
                '_remove_devs': self._remove_devs}

    def change_min_part_hours(self, min_part_hours):
        """
        Changes the value used to decide if a given partition can be moved
        again. This restriction is to give the overall system enough time to
        settle a partition to its new location before moving it to yet another
        location. While no data would be lost if a partition is moved several
        times quickly, it could make that data unreachable for a short period
        of time.

        This should be set to at least the average full partition replication
        time. Starting it at 24 hours and then lowering it to what the
        replicator reports as the longest partition cycle is best.

        :param min_part_hours: new value for min_part_hours
        """
        self.min_part_hours = min_part_hours

    def get_ring(self):
        """
        Get the ring, or more specifically, the swift.common.ring.RingData.
        This ring data is the minimum required for use of the ring. The ring
        builder itself keeps additional data such as when partitions were last
        moved.
        """
        if not self._ring:
            devs = [None] * len(self.devs)
            for dev in self._iter_devs():
                devs[dev['id']] = dict((k, v) for k, v in dev.items()
                                       if k not in ('parts', 'parts_wanted'))
            if not self._replica2part2dev:
                self._ring = RingData([], devs, 32 - self.part_power)
            else:
                self._ring = \
                RingData([array('H', p2d) for p2d in self._replica2part2dev],
                         devs, 32 - self.part_power)
        return self._ring

    def add_dev(self, dev):
        """
        Add a device to the ring. This device dict should have a minimum of the
        following keys:

        ======  ===============================================================
        id      unique integer identifier amongst devices
        weight  a float of the relative weight of this device as compared to
                others; this indicates how many partitions the builder will try
                to assign to this device
        zone    integer indicating which zone the device is in; a given
                partition will not be assigned to multiple devices within the
                same zone
        ip      the ip address of the device
        port    the tcp port of the device
        device  the device's name on disk (sdb1, for example)
        meta    general use 'extra' field; for example: the online date, the
                hardware description
        ======  ===============================================================

        .. note::
            This will not rebalance the ring immediately as you may want to
            make multiple changes for a single rebalance.

        :param dev: device dict
        """
        if dev['id'] < len(self.devs) and self.devs[dev['id']] is not None:
            raise exceptions.DuplicateDeviceError(
                    'Duplicate device id: %d' % dev['id'])
        while dev['id'] >= len(self.devs):
            self.devs.append(None)
        dev['weight'] = float(dev['weight'])
        dev['parts'] = 0
        self.devs[dev['id']] = dev
        self._set_parts_wanted()
        self.devs_changed = True
        self.version += 1

    def set_dev_weight(self, dev_id, weight):
        """
        Set the weight of a device. This should be called rather than just
        altering the weight key in the device dict directly, as the builder
        will need to rebuild some internal state to reflect the change.

        .. note::
            This will not rebalance the ring immediately as you may want to
            make multiple changes for a single rebalance.

        :param dev_id: device id
        :param weight: new weight for device
        """
        self.devs[dev_id]['weight'] = weight
        self._set_parts_wanted()
        self.devs_changed = True
        self.version += 1

    def remove_dev(self, dev_id):
        """
        Remove a device from the ring.

        .. note::
            This will not rebalance the ring immediately as you may want to
            make multiple changes for a single rebalance.

        :param dev_id: device id
        """
        dev = self.devs[dev_id]
        dev['weight'] = 0
        self._remove_devs.append(dev)
        self._set_parts_wanted()
        self.devs_changed = True
        self.version += 1

    def rebalance(self):
        """
        Rebalance the ring.

        This is the main work function of the builder, as it will assign and
        reassign partitions to devices in the ring based on weights, distinct
        zones, recent reassignments, etc.

        The process doesn't always perfectly assign partitions (that'd take a
        lot more analysis and therefore a lot more time -- I had code that did
        that before). Because of this, it keeps rebalancing until the device
        skew (number of partitions a device wants compared to what it has) gets
        below 1% or doesn't change by more than 1% (only happens with ring that
        can't be balanced no matter what -- like with 3 zones of differing
        weights with replicas set to 3).
        """
        self._ring = None
        if self._last_part_moves_epoch is None:
            self._initial_balance()
            self.devs_changed = False
            return self.parts, self.get_balance()
        retval = 0
        self._update_last_part_moves()
        last_balance = 0
        while True:
            reassign_parts = self._gather_reassign_parts()
            self._reassign_parts(reassign_parts)
            retval += len(reassign_parts)
            while self._remove_devs:
                self.devs[self._remove_devs.pop()['id']] = None
            balance = self.get_balance()
            if balance < 1 or abs(last_balance - balance) < 1 or \
                    retval == self.parts:
                break
            last_balance = balance
        self.devs_changed = False
        self.version += 1
        return retval, balance

    def validate(self, stats=False):
        """
        Validate the ring.

        This is a safety function to try to catch any bugs in the building
        process. It ensures partitions have been assigned to distinct zones,
        aren't doubly assigned, etc. It can also optionally check the even
        distribution of partitions across devices.

        :param stats: if True, check distribution of partitions across devices
        :returns: if stats is True, a tuple of (device usage, worst stat), else
                  (None, None)
        :raises RingValidationError: problem was found with the ring.
        """
        if sum(d['parts'] for d in self._iter_devs()) != \
                self.parts * self.replicas:
            raise exceptions.RingValidationError(
                'All partitions are not double accounted for: %d != %d' %
                (sum(d['parts'] for d in self._iter_devs()),
                 self.parts * self.replicas))
        if stats:
            dev_usage = array('I', (0 for _junk in xrange(len(self.devs))))
            for part2dev in self._replica2part2dev:
                for dev_id in part2dev:
                    dev_usage[dev_id] += 1

        max_allowed_replicas = self._build_max_replicas_by_tier()

        for part in xrange(self.parts):
            replicas_at_tier = defaultdict(lambda: 0)
            for replica in xrange(self.replicas):
                dev = self.devs[self._replica2part2dev[replica][part]]
                for tier in tiers_for_dev(dev):
                    replicas_at_tier[tier] += 1

            for tier in replicas_at_tier:
                if replicas_at_tier[tier] > max_allowed_replicas[tier]:
                    raise exceptions.RingValidationError(
                        "Partition %d was in tier %r more than %d times; "
                        "it appeared %d times" %
                        (part, tier, max_allowed_replicas[tier],
                         replicas_at_tier[tier]))

        if stats:
            weighted_parts = self.weighted_parts()
            worst = 0
            for dev in self._iter_devs():
                if not dev['weight']:
                    if dev_usage[dev['id']]:
                        worst = 999.99
                        break
                    continue
                skew = abs(100.0 * dev_usage[dev['id']] /
                           (dev['weight'] * weighted_parts) - 100.0)
                if skew > worst:
                    worst = skew
            return dev_usage, worst
        return None, None

    def get_balance(self):
        """
        Get the balance of the ring. The balance value is the highest
        percentage off the desired amount of partitions a given device wants.
        For instance, if the "worst" device wants (based on its relative weight
        and its zone's relative weight) 123 partitions and it has 124
        partitions, the balance value would be 0.83 (1 extra / 123 wanted * 100
        for percentage).

        :returns: balance of the ring
        """
        balance = 0
        weighted_parts = self.weighted_parts()
        for dev in self._iter_devs():
            if not dev['weight']:
                if dev['parts']:
                    balance = 999.99
                    break
                continue
            dev_balance = abs(100.0 * dev['parts'] /
                              (dev['weight'] * weighted_parts) - 100.0)
            if dev_balance > balance:
                balance = dev_balance
        return balance

    def pretend_min_part_hours_passed(self):
        """
        Override min_part_hours by marking all partitions as having been moved
        255 hours ago. This can be used to force a full rebalance on the next
        call to rebalance.
        """
        for part in xrange(self.parts):
            self._last_part_moves[part] = 0xff

    def get_part_devices(self, part):
        """
        Get the devices that are responsible for the partition.

        :param part: partition to get devices for
        :returns: list of device dicts
        """
        return [self.devs[r[part]] for r in self._replica2part2dev]

    def _iter_devs(self):
        for dev in self.devs:
            if dev is not None:
                yield dev

    def _set_parts_wanted(self):
        """
        Sets the parts_wanted key for each of the devices to the number of
        partitions the device wants based on its relative weight. This key is
        used to sort the devices according to "most wanted" during rebalancing
        to best distribute partitions.
        """
        weighted_parts = self.weighted_parts()

        for dev in self._iter_devs():
            if not dev['weight']:
                dev['parts_wanted'] = self.parts * -2
            else:
                dev['parts_wanted'] = \
                    int(weighted_parts * dev['weight']) - dev['parts']

    def _initial_balance(self):
        """
        Initial partition assignment is the same as rebalancing an
        existing ring, but with some initial setup beforehand.
        """
        self._replica2part2dev = \
            [array('H', (0 for _junk in xrange(self.parts)))
                   for _junk in xrange(self.replicas)]

        replicas = range(self.replicas)
        self._last_part_moves = array('B', (0 for _junk in xrange(self.parts)))
        self._last_part_moves_epoch = int(time())
        self._reassign_parts((p, replicas) for p in xrange(self.parts))

    def _update_last_part_moves(self):
        """
        Updates how many hours ago each partition was moved based on the
        current time. The builder won't move a partition that has been moved
        more recently than min_part_hours.
        """
        elapsed_hours = int(time() - self._last_part_moves_epoch) / 3600
        for part in xrange(self.parts):
            self._last_part_moves[part] = \
                min(self._last_part_moves[part] + elapsed_hours, 0xff)
        self._last_part_moves_epoch = int(time())

    def _gather_reassign_parts(self):
        """
        Returns a list of (partition, replicas) pairs to be reassigned by
        gathering from removed devices, insufficiently-far-apart replicas, and
        overweight drives.
        """
        reassign_parts = defaultdict(list)
        spread_out_parts = defaultdict(list)
        removed_dev_parts = defaultdict(list)
        if self._remove_devs:
            dev_ids = [d['id'] for d in self._remove_devs if d['parts']]
            if dev_ids:
                for replica in xrange(self.replicas):
                    part2dev = self._replica2part2dev[replica]
                    for part in xrange(self.parts):
                        if part2dev[part] in dev_ids:
                            self._last_part_moves[part] = 0
                            removed_dev_parts[part].append(replica)

        max_allowed_replicas = self._build_max_replicas_by_tier()
        for part in xrange(self.parts):
            replicas_at_tier = defaultdict(lambda: 0)
            for replica in xrange(self.replicas):
                dev = self.devs[self._replica2part2dev[replica][part]]
                for tier in tiers_for_dev(dev):
                    replicas_at_tier[tier] += 1
            for replica in xrange(self.replicas):
                dev = self.devs[self._replica2part2dev[replica][part]]
                for tier in tiers_for_dev(dev):
                    if replicas_at_tier[tier] > max_allowed_replicas[tier]:
                        self._last_part_moves[part] = 0
                        spread_out_parts[part].append(replica)
                        dev['parts_wanted'] += 1
                        dev['parts'] -= 1
                        break

        start = self._last_part_gather_start / 4 + randint(0, self.parts / 2)
        self._last_part_gather_start = start
        for replica in xrange(self.replicas):
            part2dev = self._replica2part2dev[replica]
            for part in itertools.chain(xrange(start, self.parts),
                                        xrange(0, start)):
                if self._last_part_moves[part] < self.min_part_hours:
                    continue
                if part in removed_dev_parts:
                    continue
                dev = self.devs[part2dev[part]]
                if dev['parts_wanted'] < 0:
                    self._last_part_moves[part] = 0
                    dev['parts_wanted'] += 1
                    dev['parts'] -= 1
                    reassign_parts[part].append(replica)

        reassign_parts.update(spread_out_parts)
        reassign_parts.update(removed_dev_parts)

        reassign_parts_list = list(reassign_parts.iteritems())
        shuffle(reassign_parts_list)
        return reassign_parts_list

    def _reassign_parts(self, reassign_parts):
        """
        For an existing ring data set, partitions are reassigned similarly to
        the initial assignment. The devices are ordered by how many partitions
        they still want and kept in that order throughout the process. The
        gathered partitions are iterated through, assigning them to devices
        according to the "most wanted" while keeping the replicas as "far
        apart" as possible. Two different zones are considered the
        farthest-apart things, followed by different ip/port pairs within a
        zone; the least-far-apart things are different devices with the same
        ip/port pair in the same zone.

        If you want more replicas than devices, you won't get all your
        replicas.
        """
        for dev in self._iter_devs():
            dev['sort_key'] = self._sort_key_for(dev)
        available_devs = \
            sorted((d for d in self._iter_devs() if d['weight']),
                   key=lambda x: x['sort_key'])

        tier2children = build_tier_tree(available_devs)

        tier2devs = defaultdict(list)
        tier2sort_key = defaultdict(list)
        tiers_by_depth = defaultdict(set)
        for dev in available_devs:
            for tier in tiers_for_dev(dev):
                tier2devs[tier].append(dev)  # <-- starts out sorted!
                tier2sort_key[tier].append(dev['sort_key'])
                tiers_by_depth[len(tier)].add(tier)

        for part, replace_replicas in reassign_parts:
            other_replicas = defaultdict(lambda: 0)
            for replica in xrange(self.replicas):
                if replica not in replace_replicas:
                    dev = self.devs[self._replica2part2dev[replica][part]]
                    for tier in tiers_for_dev(dev):
                        other_replicas[tier] += 1

            def find_home_for_replica(replica, tier=(), depth=1):
                # Order the tiers by how many replicas of this
                # partition they already have. Then, of the ones
                # with the smallest number of replicas, pick the
                # tier with the hungriest drive and then continue
                # searching in that subtree.
                #
                # There are other strategies we could use here,
                # such as hungriest-tier (i.e. biggest
                # sum-of-parts-wanted) or picking one at random.
                # However, hungriest-drive is what was used here
                # before, and it worked pretty well in practice.
                #
                # Note that this allocator will balance things as
                # evenly as possible at each level of the device
                # layout. If your layout is extremely unbalanced,
                # this may produce poor results.
                candidate_tiers = tier2children[tier]
                min_count = min(other_replicas[t] for t in candidate_tiers)
                candidate_tiers = [t for t in candidate_tiers
                                   if other_replicas[t] == min_count]
                candidate_tiers.sort(
                    key=lambda t: tier2devs[t][-1]['parts_wanted'])

                if depth == max(tiers_by_depth.keys()):
                    return tier2devs[candidate_tiers[-1]][-1]

                return find_home_for_replica(replica,
                                             tier=candidate_tiers[-1],
                                             depth=depth + 1)

            for replica in replace_replicas:
                dev = find_home_for_replica(replica)
                dev['parts_wanted'] -= 1
                dev['parts'] += 1
                old_sort_key = dev['sort_key']
                new_sort_key = dev['sort_key'] = self._sort_key_for(dev)
                for tier in tiers_for_dev(dev):
                    other_replicas[tier] += 1

                    index = bisect.bisect_left(tier2sort_key[tier],
                                               old_sort_key)
                    tier2devs[tier].pop(index)
                    tier2sort_key[tier].pop(index)

                    new_index = bisect.bisect_left(tier2sort_key[tier],
                                                   new_sort_key)
                    tier2devs[tier].insert(new_index, dev)
                    tier2sort_key[tier].insert(new_index, new_sort_key)

                self._replica2part2dev[replica][part] = dev['id']

        for dev in self._iter_devs():
            del dev['sort_key']

    def _insert_dev_sorted(self, devs, dev):
        index = 0
        end = len(devs)
        while index < end:
            mid = (index + end) // 2
            if dev['sort_key'] < devs[mid]['sort_key']:
                end = mid
            else:
                index = mid + 1
        devs.insert(index, dev)

    def _sort_key_for(self, dev):
        return '%08x.%04x.%04x' % (self.parts + dev['parts_wanted'],
                                   randint(0, 0xffff),
                                   dev['id'])

    def _build_max_replicas_by_tier(self):
        tier2children = build_tier_tree(self._iter_devs())

        def walk_tree(tier, replica_count):
            mr = {tier: replica_count}
            if tier in tier2children:
                subtiers = tier2children[tier]
                for subtier in subtiers:
                    submax = math.ceil(float(replica_count) / len(subtiers))
                    mr.update(walk_tree(subtier, submax))
            return mr
        return walk_tree((), self.replicas)
