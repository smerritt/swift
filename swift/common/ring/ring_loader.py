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


import os
from swift.common.ring.ring import Ring
from swift.common.ring.composite_ring import CompositeRing


class RingLoader(object):
    """
    Load rings as singletons.

    Ring objects are very costly; one single Ring can occupy megabytes of
    RAM and needs occasional disk IO to reload it. This class ensures that
    a given ring.gz is only loaded once; that is,

    >>> loader = RingLoader("/etc/swift")
    >>> r1 = loader.load_ring("account")
    >>> r2 = loader.load_ring("account")
    >>> r1 is r2
    True
    >>>

    To see the utility of this class, imagine a system with 4 storage
    policies:

      * policy A is 3 replicas in Tokyo (plain old ring)
      * policy B is 3 replicas in San Francisco (plain old ring)
      * policy C is 2 replicas in Tokyo, 1 in San Francisco (composite ring)
      * policy D is 1 replica in Tokyo, 2 in San Francisco (composite ring)

    With the cache in the RingLoader, a proxy server would need only two
    Ring objects: one for policy A, and one for policy B. The CompositeRing
    objects for C and D will reuse the same Ring objects. Without this
    caching, there would be six Ring objects: three of policy A's ring, and
    three of policy B.
    """

    _instance = None

    @classmethod
    def get_instance(cls, swift_dir):
        if not cls._instance:
            cls._instance = cls(swift_dir)
        return cls._instance

    def __init__(self, swift_dir):
        self._swift_dir = swift_dir
        self._ring_cache = {}

    def load_ring(self, ring_name, already_used=None):
        if already_used is None:
            already_used = set()

        if ring_name in already_used:
            raise ValueError("loop detected while loading ring %s" % ring_name)

        if ring_name in self._ring_cache:
            return self._ring_cache[ring_name]

        already_used.add(ring_name)

        real_ring_filename = os.path.join(
            self._swift_dir, ring_name + ".ring.gz")
        composite_ring_filename = os.path.join(
            self._swift_dir, ring_name + ".composite-ring.json")
        if os.path.exists(real_ring_filename):
            ring = Ring(real_ring_filename)
        elif os.path.exists(composite_ring_filename):
            ring = CompositeRing(self, composite_ring_filename, already_used)
        else:
            raise ValueError("no ring named %s found on disk" % ring_name)

        self._ring_cache[ring_name] = ring
        return ring
