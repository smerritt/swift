# Copyright (c) 2010-2012 OpenStack Foundation
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

import unittest
from ConfigParser import ConfigParser
from test.unit import patch_policies
from swift.container import replicator
from swift.common.utils import normalize_timestamp
from swift.common.storage_policy import StoragePolicy, parse_storage_policies


class TestReplicator(unittest.TestCase):

    def setUp(self):
        self.orig_ring = replicator.db_replicator.ring.Ring
        replicator.db_replicator.ring.Ring = lambda *args, **kwargs: None

    def tearDown(self):
        replicator.db_replicator.ring.Ring = self.orig_ring

    def test_report_up_to_date(self):
        repl = replicator.ContainerReplicator({})
        info = {'put_timestamp': normalize_timestamp(1),
                'delete_timestamp': normalize_timestamp(0),
                'object_count': 0,
                'bytes_used': 0,
                'reported_put_timestamp': normalize_timestamp(1),
                'reported_delete_timestamp': normalize_timestamp(0),
                'reported_object_count': 0,
                'reported_bytes_used': 0}
        self.assertTrue(repl.report_up_to_date(info))
        info['delete_timestamp'] = normalize_timestamp(2)
        self.assertFalse(repl.report_up_to_date(info))
        info['reported_delete_timestamp'] = normalize_timestamp(2)
        self.assertTrue(repl.report_up_to_date(info))
        info['object_count'] = 1
        self.assertFalse(repl.report_up_to_date(info))
        info['reported_object_count'] = 1
        self.assertTrue(repl.report_up_to_date(info))
        info['bytes_used'] = 1
        self.assertFalse(repl.report_up_to_date(info))
        info['reported_bytes_used'] = 1
        self.assertTrue(repl.report_up_to_date(info))
        info['put_timestamp'] = normalize_timestamp(3)
        self.assertFalse(repl.report_up_to_date(info))
        info['reported_put_timestamp'] = normalize_timestamp(3)
        self.assertTrue(repl.report_up_to_date(info))

    def test_sync_args(self):
        repl = replicator.ContainerReplicator({})
        replication_info = {
            'max_row': '_max_row', 'hash': '_hash', 'id': '_id',
            'created_at': '_created_at', 'put_timestamp': '_put_timestamp',
            'delete_timestamp': '_delete_timestamp', 'metadata': '_metadata',
            'storage_policy_index': '_storage_policy_index',
            'otherstuff': '_otherstuff'}

        # simulate cluster without storage policies
        pols = parse_storage_policies(ConfigParser())
        with patch_policies(pols):
            sync_args = repl.sync_args_from_replication_info(replication_info)
            self.assertEqual(sync_args, [
                '_max_row', '_hash', '_id', '_created_at', '_put_timestamp',
                '_delete_timestamp', '_metadata'])

        # with some storage policies defined, we send the policy index
        pols = [StoragePolicy(0, 'zero'),
                StoragePolicy(1, 'one', is_default=True)]
        with patch_policies(pols):
            sync_args = repl.sync_args_from_replication_info(replication_info)
            self.assertEqual(sync_args, [
                '_max_row', '_hash', '_id', '_created_at', '_put_timestamp',
                '_delete_timestamp', '_metadata', '_storage_policy_index'])


if __name__ == '__main__':
    unittest.main()
