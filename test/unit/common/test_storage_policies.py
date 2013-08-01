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
from swift.common.storage_policy import get_stor_pols, \
    parse_storage_policies, StoragePolicy, StoragePolicyCollection

class TestStoragePolicies(unittest.TestCase):

    def _conf(self, conf_str):
        conf_str = "\n".join(line.strip() for line in conf_str.split("\n"))
        conf = ConfigParser()
        conf.readfp(StringIO.StringIO(conf_str))
        return conf

    def test_defaults(self):
        policies = get_stor_pols()
        self.assert_(len(policies) > 0)
        default_policy = policies.get_def_policy()
        self.assert_(default_policy.is_default)
        zero_policy = policies.get_policy_0()
        self.assert_(zero_policy.idx == '0')

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
        stor_pols = parse_storage_policies(conf)

        self.assertEquals(stor_pols.get_def_policy(),
                          stor_pols.default)

        self.assert_(stor_pols.get_def_policy().name == 'one')
        self.assert_(stor_pols.get_policy_0().name == 'zero')

        self.assertEquals("object", stor_pols.get("zero").ring_name)
        self.assertEquals("object-5", stor_pols.get("one").ring_name)
        self.assertEquals("object-6", stor_pols.get("apple").ring_name)

        self.assertEquals("objects", stor_pols.get("zero").data_dir)
        self.assertEquals("objects-5", stor_pols.get("one").data_dir)
        self.assertEquals("objects-6", stor_pols.get("apple").data_dir)

        self.assertEquals("0", stor_pols.get("zero").idx)
        self.assertEquals("5", stor_pols.get("one").idx)
        self.assertEquals("6", stor_pols.get("apple").idx)

        self.assertEquals("0", stor_pols.policy_to_index("zero"))
        self.assertEquals("5", stor_pols.policy_to_index("one"))
        self.assertEquals("6", stor_pols.policy_to_index("apple"))
        self.assertEquals(None, stor_pols.policy_to_index("notThere"))
        self.assertEquals("0", stor_pols.policy_to_index(""))

        self.assertEquals("zero", stor_pols.index_to_policy("0").name)
        self.assertEquals("one", stor_pols.index_to_policy("5").name)
        self.assertEquals("apple", stor_pols.index_to_policy("6").name)
        self.assertEquals("zero", stor_pols.index_to_policy(None).name)

        self.assertRaises(ValueError, stor_pols.index_to_policy,"99")
        self.assertRaises(ValueError, stor_pols.index_to_policy,"")

    def test_parse_storage_policies_malformed(self):
        conf = self._conf("""
        [storage-policy:chicken]
        [storage-policy:1]
        """)
        self.assertRaises(ValueError, parse_storage_policies, conf)

        conf1 = self._conf("""
        [storage-policy:]
        """)
        self.assertRaises(ValueError, parse_storage_policies, conf)

        conf = self._conf("""
        [storage-policy:0]
        name = zero
        [storage-policy:1]
        name = zero
        """)
        self.assertRaises(ValueError, parse_storage_policies, conf)

    def test_multiple_defaults_is_error(self):
        conf = self._conf("""
        [storage-policy:1]
        default = yes
        [storage-policy:2]
        default = yes
        [storage-policy:3]
        """)
        self.assertRaises(ValueError, parse_storage_policies, conf)

    def test_no_default_specified(self):
        conf = self._conf("""
        [storage-policy:1]
        [storage-policy:2]
        """)
        stor_pols = parse_storage_policies(conf)

        self.assert_(stor_pols.get_policy_0().idx == '0')
        conf = self._conf("""
        [storage-policy:0]
        name = thisOne
        [storage-policy:2]
        """)
        stor_pols = parse_storage_policies(conf)

        self.assert_(stor_pols.get_def_policy().name == 'thisOne')

if __name__ == '__main__':
    unittest.main()
