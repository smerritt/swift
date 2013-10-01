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

import array
import hashlib
import cPickle as pickle
from collections import defaultdict
from gzip import GzipFile
from os.path import getmtime
import struct
from time import time
import os
from io import BufferedReader
from itertools import chain

from swift.common.utils import json
from swift.common.ondisk import get_hasher, hash_path, validate_configuration
from swift.common.ring.utils import tiers_for_dev


class RingData(object):
    """Partitioned consistent hashing ring data (used for serialization)."""

    def __init__(self, replica2part2dev_id, devs, part_shift,
                 hash_algorithm=None):
        self.devs = devs
        self._replica2part2dev_id = replica2part2dev_id
        self._part_shift = part_shift
        # default of 'md5' is to handle migration of old rings
        self._hash_algorithm = hash_algorithm or 'md5'

        # fail fast: raise ValueError if algorithm is no good
        get_hasher(self._hash_algorithm)

        for dev in self.devs:
            if dev is not None:
                dev.setdefault("region", 1)

    @classmethod
    def deserialize_v1(cls, gz_file):
        json_len, = struct.unpack('!I', gz_file.read(4))
        ring_dict = json.loads(gz_file.read(json_len))
        ring_dict['replica2part2dev_id'] = []
        partition_count = 1 << (32 - ring_dict['part_shift'])
        for x in xrange(ring_dict['replica_count']):
            ring_dict['replica2part2dev_id'].append(
                array.array('H', gz_file.read(2 * partition_count)))
        return ring_dict

    @classmethod
    def load(cls, filename):
        """
        Load ring data from a file.

        :param filename: Path to a file serialized by the save() method.
        :returns: A RingData instance containing the loaded data.
        """
        gz_file = GzipFile(filename, 'rb')
        # Python 2.6 GzipFile doesn't support BufferedIO
        if hasattr(gz_file, '_checkReadable'):
            gz_file = BufferedReader(gz_file)

        # See if the file is in the new format
        magic = gz_file.read(4)
        if magic == 'R1NG':
            version, = struct.unpack('!H', gz_file.read(2))
            if version == 1:
                ring_data = cls.deserialize_v1(gz_file)
            else:
                raise Exception('Unknown ring format version %d' % version)
        else:
            # Assume old-style pickled ring
            gz_file.seek(0)
            ring_data = pickle.load(gz_file)
        if not hasattr(ring_data, 'devs'):
            ring_data = RingData(ring_data['replica2part2dev_id'],
                                 ring_data['devs'], ring_data['part_shift'],
                                 ring_data.get('hash_algorithm'))
        return ring_data

    def serialize_v1(self, file_obj):
        # Write out new-style serialization magic and version:
        file_obj.write(struct.pack('!4sH', 'R1NG', 1))
        ring = self.to_dict()
        json_encoder = json.JSONEncoder(sort_keys=True)
        json_text = json_encoder.encode(
            {'devs': ring['devs'], 'part_shift': ring['part_shift'],
             'hash_algorithm': ring['hash_algorithm'],
             'replica_count': len(ring['replica2part2dev_id'])})
        json_len = len(json_text)
        file_obj.write(struct.pack('!I', json_len))
        file_obj.write(json_text)
        for part2dev_id in ring['replica2part2dev_id']:
            file_obj.write(part2dev_id.tostring())

    def save(self, filename):
        """
        Serialize this RingData instance to disk.

        :param filename: File into which this instance should be serialized.
        """
        # Override the timestamp so that the same ring data creates
        # the same bytes on disk. This makes a checksum comparison a
        # good way to see if two rings are identical.
        #
        # This only works on Python 2.7; on 2.6, we always get the
        # current time in the gzip output.
        try:
            gz_file = GzipFile(filename, 'wb', mtime=1300507380.0)
        except TypeError:
            gz_file = GzipFile(filename, 'wb')
        self.serialize_v1(gz_file)
        gz_file.close()

    def to_dict(self):
        return {'devs': self.devs,
                'replica2part2dev_id': self._replica2part2dev_id,
                'hash_algorithm': self._hash_algorithm,
                'part_shift': self._part_shift}


class Ring(object):
    """
    Partitioned consistent hashing ring.

    :param serialized_path: path to serialized RingData instance
    :param reload_time: time interval in seconds to check for a ring change
    """

    def __init__(self, serialized_path, reload_time=15, ring_name=None):
        # Can't use the ring unless the on-disk configuration is valid
        validate_configuration()
        if ring_name:
            self.serialized_path = os.path.join(serialized_path,
                                                ring_name + '.ring.gz')
        else:
            self.serialized_path = os.path.join(serialized_path)
        self.reload_time = reload_time
        self._reload(force=True)

    def _reload(self, force=False):
        self._rtime = time() + self.reload_time
        if force or self.has_changed():
            ring_data = RingData.load(self.serialized_path)
            self._mtime = getmtime(self.serialized_path)
            self._devs = ring_data.devs
            # NOTE(akscram): Replication parameters like replication_ip
            #                and replication_port are required for
            #                replication process. An old replication
            #                ring doesn't contain this parameters into
            #                device.
            for dev in self._devs:
                if dev:
                    if 'ip' in dev:
                        dev.setdefault('replication_ip', dev['ip'])
                    if 'port' in dev:
                        dev.setdefault('replication_port', dev['port'])

            self.hash_algorithm = ring_data._hash_algorithm
            self._replica2part2dev_id = ring_data._replica2part2dev_id
            self._part_shift = ring_data._part_shift
            self._rebuild_tier_data()

            # Do this now, when we know the data has changed, rather then
            # doing it on every call to get_more_nodes().
            regions = set()
            zones = set()
            self._num_devs = 0
            for dev in self._devs:
                if dev:
                    regions.add(dev['region'])
                    zones.add((dev['region'], dev['zone']))
                    self._num_devs += 1
            self._num_regions = len(regions)
            self._num_zones = len(zones)

    def _rebuild_tier_data(self):
        self.tier2devs = defaultdict(list)
        for dev in self._devs:
            if not dev:
                continue
            for tier in tiers_for_dev(dev):
                self.tier2devs[tier].append(dev)

        tiers_by_length = defaultdict(list)
        for tier in self.tier2devs:
            tiers_by_length[len(tier)].append(tier)
        self.tiers_by_length = sorted(tiers_by_length.values(),
                                      key=lambda x: len(x[0]))
        for tiers in self.tiers_by_length:
            tiers.sort()

    @property
    def replica_count(self):
        """Number of replicas (full or partial) used in the ring."""
        return len(self._replica2part2dev_id)

    @property
    def partition_count(self):
        """Number of partitions in the ring."""
        return len(self._replica2part2dev_id[0])

    @property
    def devs(self):
        """devices in the ring"""
        if time() > self._rtime:
            self._reload()
        return self._devs

    def has_changed(self):
        """
        Check to see if the ring on disk is different than the current one in
        memory.

        :returns: True if the ring on disk has changed, False otherwise
        """
        return getmtime(self.serialized_path) != self._mtime

    def _get_part_nodes(self, part):
        part_nodes = []
        seen_ids = set()
        for r2p2d in self._replica2part2dev_id:
            if part < len(r2p2d):
                dev_id = r2p2d[part]
                if dev_id not in seen_ids:
                    part_nodes.append(self.devs[dev_id])
                seen_ids.add(dev_id)
        return part_nodes

    def get_part(self, account, container=None, obj=None):
        """
        Get the partition for an account/container/object.

        :param account: account name
        :param container: container name
        :param obj: object name
        :returns: the partition number
        """
        key = hash_path(account, container, obj, raw_digest=True,
                        hash_algorithm=self.hash_algorithm)
        if time() > self._rtime:
            self._reload()
        part = struct.unpack_from('>I', key)[0] >> self._part_shift
        return part

    def get_part_nodes(self, part):
        """
        Get the nodes that are responsible for the partition. If one
        node is responsible for more than one replica of the same
        partition, it will only appear in the output once.

        :param part: partition to get nodes for
        :returns: list of node dicts

        See :func:`get_nodes` for a description of the node dicts.
        """

        if time() > self._rtime:
            self._reload()
        return self._get_part_nodes(part)

    def get_nodes(self, account, container=None, obj=None):
        """
        Get the partition and nodes for an account/container/object.
        If a node is responsible for more than one replica, it will
        only appear in the output once.

        :param account: account name
        :param container: container name
        :param obj: object name
        :returns: a tuple of (partition, list of node dicts)

        Each node dict will have at least the following keys:

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
        """
        part = self.get_part(account, container, obj)
        return part, self._get_part_nodes(part)

    def get_more_nodes(self, part):
        """
        Generator to get extra nodes for a partition for hinted handoff.

        The handoff nodes will try to be in zones other than the
        primary zones, will take into account the device weights, and
        will usually keep the same sequences of handoffs even with
        ring changes.

        :param part: partition to get handoff nodes for
        :returns: generator of node dicts

        See :func:`get_nodes` for a description of the node dicts.
        """
        if time() > self._rtime:
            self._reload()
        primary_nodes = self._get_part_nodes(part)

        used = set(d['id'] for d in primary_nodes)
        same_regions = set(d['region'] for d in primary_nodes)
        same_zones = set((d['region'], d['zone']) for d in primary_nodes)

        parts = len(self._replica2part2dev_id[0])
        start = struct.unpack_from(
            '>I', hashlib.md5(str(part)).digest())[0] >> self._part_shift
        inc = int(parts / 65536) or 1
        # Multiple loops for execution speed; the checks and bookkeeping get
        # simpler as you go along
        hit_all_regions = len(same_regions) == self._num_regions
        for handoff_part in chain(xrange(start, parts, inc),
                                  xrange(inc - ((parts - start) % inc),
                                         start, inc)):
            if hit_all_regions:
                # At this point, there are no regions left untouched, so we
                # can stop looking.
                break
            for part2dev_id in self._replica2part2dev_id:
                if handoff_part < len(part2dev_id):
                    dev_id = part2dev_id[handoff_part]
                    dev = self._devs[dev_id]
                    region = dev['region']
                    zone = (region, dev['zone'])
                    if dev_id not in used and region not in same_regions:
                        yield dev
                        used.add(dev_id)
                        same_regions.add(region)
                        same_zones.add(zone)
                        if len(same_regions) == self._num_regions:
                            hit_all_regions = True
                            break

        hit_all_zones = len(same_zones) == self._num_zones
        for handoff_part in chain(xrange(start, parts, inc),
                                  xrange(inc - ((parts - start) % inc),
                                         start, inc)):
            if hit_all_zones:
                # Much like we stopped looking for fresh regions before, we
                # can now stop looking for fresh zones; there are no more.
                break
            for part2dev_id in self._replica2part2dev_id:
                if handoff_part < len(part2dev_id):
                    dev_id = part2dev_id[handoff_part]
                    dev = self._devs[dev_id]
                    zone = (dev['region'], dev['zone'])
                    if dev_id not in used and zone not in same_zones:
                        yield dev
                        used.add(dev_id)
                        same_zones.add(zone)
                        if len(same_zones) == self._num_zones:
                            hit_all_zones = True
                            break

        hit_all_devs = len(used) == self._num_devs
        for handoff_part in chain(xrange(start, parts, inc),
                                  xrange(inc - ((parts - start) % inc),
                                         start, inc)):
            if hit_all_devs:
                # We've used every device we have, so let's stop looking for
                # unused devices now.
                break
            for part2dev_id in self._replica2part2dev_id:
                if handoff_part < len(part2dev_id):
                    dev_id = part2dev_id[handoff_part]
                    if dev_id not in used:
                        yield self._devs[dev_id]
                        used.add(dev_id)
                        if len(used) == self._num_devs:
                            hit_all_devs = True
                            break
