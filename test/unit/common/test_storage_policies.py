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

""" Tests for swift.common.storage_policies """
import unittest
import StringIO
from ConfigParser import ConfigParser
from swift.common.utils import config_true_value
from swift.common import storage_policy


class TestStoragePolicies(unittest.TestCase):

    def _conf(self, conf_str):
        conf_str = "\n".join(line.strip() for line in conf_str.split("\n"))
        conf = ConfigParser()
        conf.readfp(StringIO.StringIO(conf_str))
        return conf

    def test_defaults(self):
        policies = storage_policy.get_policies()
        self.assert_(len(policies) > 0)
        default_policy = policies.default
        self.assert_(default_policy.is_default)
        zero_policy = policies.get_policy_0()
        self.assert_(zero_policy.idx == 0)

    def test_parse_storage_policies(self):
        conf = self._conf("""
        [storage-policy:0]
        name = zero
        [storage-policy:5]
        name = one
        default = yes
        [storage-policy:6]
        name = apple
        """)
        stor_pols = storage_policy.parse_storage_policies(conf)

        print repr(stor_pols.__dict__)

        self.assertEquals(stor_pols.get_default(), stor_pols.default)

        self.assertEquals(stor_pols.default.name, 'one')
        self.assertEquals(stor_pols.get_policy_0().name, 'zero')

        self.assertEquals("object", stor_pols.get_by_name("zero").ring_name)
        self.assertEquals("object-5", stor_pols.get_by_name("one").ring_name)
        self.assertEquals("object-6", stor_pols.get_by_name("apple").ring_name)

        self.assertEquals("objects", stor_pols.get_by_name("zero").data_dir)
        self.assertEquals("objects-5", stor_pols.get_by_name("one").data_dir)
        self.assertEquals("objects-6", stor_pols.get_by_name("apple").data_dir)

        self.assertEquals(0, stor_pols.get_by_name("zero").idx)
        self.assertEquals(5, stor_pols.get_by_name("one").idx)
        self.assertEquals(6, stor_pols.get_by_name("apple").idx)

        self.assertEquals("zero", stor_pols.get_by_index(0).name)
        self.assertEquals("zero", stor_pols.get_by_index("0").name)
        self.assertEquals("one", stor_pols.get_by_index(5).name)
        self.assertEquals("apple", stor_pols.get_by_index(6).name)
        self.assertEquals("zero", stor_pols.get_by_index(None).name)

        self.assertRaises(ValueError, stor_pols.get_by_index, "")
        self.assertRaises(ValueError, stor_pols.get_by_index, "ein")

    def test_parse_storage_policies_malformed(self):
        conf = self._conf("""
        [storage-policy:chicken]
        [storage-policy:1]
        """)
        self.assertRaises(
            ValueError, storage_policy.parse_storage_policies, conf)

        conf1 = self._conf("""
        [storage-policy:]
        """)
        self.assertRaises(
            ValueError, storage_policy.parse_storage_policies, conf)

        conf = self._conf("""
        [storage-policy:0]
        name = zero
        [storage-policy:1]
        name = zero
        """)
        self.assertRaises(
            ValueError, storage_policy.parse_storage_policies, conf)

    def test_multiple_defaults_is_error(self):
        conf = self._conf("""
        [storage-policy:1]
        default = yes
        [storage-policy:2]
        default = yes
        [storage-policy:3]
        """)
        self.assertRaises(
            ValueError, storage_policy.parse_storage_policies, conf)

    def test_no_default_specified(self):
        conf = self._conf("""
        [storage-policy:1]
        [storage-policy:2]
        """)
        stor_pols = storage_policy.parse_storage_policies(conf)

        self.assertEquals(stor_pols.get_policy_0().idx, 0)

        conf = self._conf("""
        [storage-policy:0]
        name = thisOne
        [storage-policy:2]
        """)
        stor_pols = storage_policy.parse_storage_policies(conf)
        self.assertEqual(stor_pols.default.name, 'thisOne')

if __name__ == '__main__':
    unittest.main()
