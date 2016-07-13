# Copyright (c) 2016 OpenStack Foundation
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

import itertools
import json
from swift.common.ring.ring import BaseRing

#NOTES/TODO:
#
# stair-step the primaries from each sub-ring
#
# just load Ring objects in CompositeRing; then we can lose the loop
# detection
#
# making a composite out of non-disjoint rings is Not Supported(tm), so
# don't worry about dupe detection
#
# look at Kota's patch and think hard about device IDs and uniqueness
#
# make the replicators work (implement .devs property) (also think about ID
# uniqueness here)




class CompositeRing(BaseRing):
    def __init__(self, ring_loader, ring_filename, already_used):
        self._filename = ring_filename
        self._rings_and_replicas = []

        self._rtime = float("inf")  # fugly hack to disable reloading

        self._load(ring_loader, already_used)

    def _reload(self, *a, **kw):
        # ZOMG fix the class split, jackass
        pass

    def get_part(self, account, container=None, obj=None):
        # all our component rings have the same part power, so we can just
        # pick one to do the hashing
        return self._rings_and_replicas[0][0].get_part(account, container, obj)

    def _load(self, ring_loader, already_used):
        with open(self._filename) as fh:
            ring_spec = json.load(fh)
        # TODO: validate this stuff instead of just blindly charging forward
        #
        # first the basics, like "is this JSON even remotely sane"
        #
        # also stuff like "do part-powers match between component rings"
        #
        # "do these things have enough replicas"
        ring_names_and_replica_counts = sorted(
            # sort to avoid randomness, otherwise primaries might shift
            # around and royally hose EC
            ring_spec['component_rings'].items(),
            key = lambda kvpair: kvpair[0])

        for ring_name, replica_count in ring_names_and_replica_counts:
            ring = ring_loader.load_ring(ring_name, already_used)
            self._rings_and_replicas.append((ring, replica_count))

    def _get_part_nodes(self, part):
        """
        Get the primary nodes for a given partition.

        Note that this returns a list, not a generator. This is to match
        Ring._get_part_nodes().
        """
        nodes = []
        for ring, replica_count in self._rings_and_replicas:
            for node in self._take_n_nodes(ring, part, replica_count):
                nodes.append(node)
        return nodes

    def get_more_nodes(self, part):
        iter_idx = 0
        iters = [self._skip_n_nodes(ring, part, reps)
                 for ring, reps in self._rings_and_replicas]

        # XXX if the same device is in two rings (like on a SAIO), then you
        # can get the same handoff node coming out of here twice. I don't
        # know if we care enough to bother deduplicating this stuff, or if
        # we just tell operators to use disjoint rings.

        while iters:
            try:
                yield next(iters[iter_idx])
            except StopIteration:
                iters.pop(iter_idx)
            else:
                iter_idx = (iter_idx + 1) % len(iters)

    def _take_n_nodes(self, ring, part, nnodes):
        # We're using these as primary nodes in this CompositeRing, so we're
        # going to enforce that they are primary in their component rings.
        # Otherwise we're not guaranteed any sort of placement stability
        # (either on rebalance or ring-code change) so objects could end up
        # unavailable.
        primary_nodes = ring._get_part_nodes(part)
        if len(primary_nodes) < nnodes:
            raise ValueError("not enough primaries (got %d, wanted %d)" %
                             (len(primary_nodes), nnodes))

        for node in primary_nodes[:nnodes]:
            yield node

    def _skip_n_nodes(self, ring, part, to_skip):
        """
        Skip the first N nodes from (primary + handoffs), then return an
        iterator over the remainder.
        """
        all_nodes = itertools.chain(ring._get_part_nodes(part),
                                    ring.get_more_nodes(part))
        while to_skip > 0:
            next(all_nodes)
            to_skip -= 1
        return all_nodes
