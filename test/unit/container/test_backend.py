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

""" Tests for swift.container.backend """

import hashlib
import os
import pickle
import unittest
from shutil import rmtree
from tempfile import mkdtemp
from time import sleep, time
from uuid import uuid4
import itertools

from swift.container.backend import ContainerBroker
from swift.common.utils import normalize_timestamp
from swift.common.storage_policy import POLICIES

import mock

from test.unit import patch_policies


class TestContainerBroker(unittest.TestCase):
    """Tests for ContainerBroker"""

    def test_creation(self):
        # Test ContainerBroker.__init__
        broker = ContainerBroker(':memory:', account='a', container='c')
        self.assertEqual(broker.db_file, ':memory:')
        broker.initialize(normalize_timestamp('1'), 0)
        with broker.get() as conn:
            curs = conn.cursor()
            curs.execute('SELECT 1')
            self.assertEqual(curs.fetchall()[0][0], 1)

    @patch_policies
    def test_storage_policy_property(self):
        ts = itertools.count(1)
        for policy in POLICIES:
            broker = ContainerBroker(':memory:', account='a',
                                     container='policy_%s' % policy.name)
            broker.initialize(normalize_timestamp(ts.next()), policy.idx)
            with broker.get() as conn:
                try:
                    conn.execute('''SELECT storage_policy_index
                                    FROM container_stat''')
                except Exception:
                    is_migrated = False
                else:
                    is_migrated = True
            if not is_migrated:
                # pre spi tests don't set policy on initialize
                broker.set_storage_policy_index(policy.idx)
            self.assertEqual(policy.idx, broker.storage_policy_index)
            # make sure it's cached
            with mock.patch.object(broker, 'get'):
                self.assertEqual(policy.idx, broker.storage_policy_index)

    def test_exception(self):
        # Test ContainerBroker throwing a conn away after
        # unhandled exception
        first_conn = None
        broker = ContainerBroker(':memory:', account='a', container='c')
        broker.initialize(normalize_timestamp('1'), 0)
        with broker.get() as conn:
            first_conn = conn
        try:
            with broker.get() as conn:
                self.assertEquals(first_conn, conn)
                raise Exception('OMG')
        except Exception:
            pass
        self.assert_(broker.conn is None)

    def test_empty(self):
        # Test ContainerBroker.empty
        broker = ContainerBroker(':memory:', account='a', container='c')
        broker.initialize(normalize_timestamp('1'), 0)
        self.assert_(broker.empty())
        broker.put_object('o', normalize_timestamp(time()), 0, 'text/plain',
                          'd41d8cd98f00b204e9800998ecf8427e', 0)
        self.assert_(not broker.empty())
        sleep(.00001)
        broker.delete_object('o', normalize_timestamp(time()), 0)
        self.assert_(broker.empty())

    def test_reclaim(self):
        broker = ContainerBroker(':memory:', account='test_account',
                                 container='test_container')
        broker.initialize(normalize_timestamp('1'), 0)
        broker.put_object('o', normalize_timestamp(time()), 0, 'text/plain',
                          'd41d8cd98f00b204e9800998ecf8427e', 0)
        with broker.get() as conn:
            self.assertEquals(conn.execute(
                "SELECT count(*) FROM object "
                "WHERE deleted = 0").fetchone()[0], 1)
            self.assertEquals(conn.execute(
                "SELECT count(*) FROM object "
                "WHERE deleted = 1").fetchone()[0], 0)
        broker.reclaim(normalize_timestamp(time() - 999), time())
        with broker.get() as conn:
            self.assertEquals(conn.execute(
                "SELECT count(*) FROM object "
                "WHERE deleted = 0").fetchone()[0], 1)
            self.assertEquals(conn.execute(
                "SELECT count(*) FROM object "
                "WHERE deleted = 1").fetchone()[0], 0)
        sleep(.00001)
        broker.delete_object('o', normalize_timestamp(time()), 0)
        with broker.get() as conn:
            self.assertEquals(conn.execute(
                "SELECT count(*) FROM object "
                "WHERE deleted = 0").fetchone()[0], 0)
            self.assertEquals(conn.execute(
                "SELECT count(*) FROM object "
                "WHERE deleted = 1").fetchone()[0], 1)
        broker.reclaim(normalize_timestamp(time() - 999), time())
        with broker.get() as conn:
            self.assertEquals(conn.execute(
                "SELECT count(*) FROM object "
                "WHERE deleted = 0").fetchone()[0], 0)
            self.assertEquals(conn.execute(
                "SELECT count(*) FROM object "
                "WHERE deleted = 1").fetchone()[0], 1)
        sleep(.00001)
        broker.reclaim(normalize_timestamp(time()), time())
        with broker.get() as conn:
            self.assertEquals(conn.execute(
                "SELECT count(*) FROM object "
                "WHERE deleted = 0").fetchone()[0], 0)
            self.assertEquals(conn.execute(
                "SELECT count(*) FROM object "
                "WHERE deleted = 1").fetchone()[0], 0)
        # Test the return values of reclaim()
        broker.put_object('w', normalize_timestamp(time()), 0, 'text/plain',
                          'd41d8cd98f00b204e9800998ecf8427e', 0)
        broker.put_object('x', normalize_timestamp(time()), 0, 'text/plain',
                          'd41d8cd98f00b204e9800998ecf8427e', 0)
        broker.put_object('y', normalize_timestamp(time()), 0, 'text/plain',
                          'd41d8cd98f00b204e9800998ecf8427e', 0)
        broker.put_object('z', normalize_timestamp(time()), 0, 'text/plain',
                          'd41d8cd98f00b204e9800998ecf8427e', 0)
        # Test before deletion
        broker.reclaim(normalize_timestamp(time()), time())
        broker.delete_db(normalize_timestamp(time()))

    def test_delete_object(self):
        # Test ContainerBroker.delete_object
        broker = ContainerBroker(':memory:', account='a', container='c')
        broker.initialize(normalize_timestamp('1'), 0)
        broker.put_object('o', normalize_timestamp(time()), 0, 'text/plain',
                          'd41d8cd98f00b204e9800998ecf8427e', 0)
        with broker.get() as conn:
            self.assertEquals(conn.execute(
                "SELECT count(*) FROM object "
                "WHERE deleted = 0").fetchone()[0], 1)
            self.assertEquals(conn.execute(
                "SELECT count(*) FROM object "
                "WHERE deleted = 1").fetchone()[0], 0)
        sleep(.00001)
        broker.delete_object('o', normalize_timestamp(time()), 0)
        with broker.get() as conn:
            self.assertEquals(conn.execute(
                "SELECT count(*) FROM object "
                "WHERE deleted = 0").fetchone()[0], 0)
            self.assertEquals(conn.execute(
                "SELECT count(*) FROM object "
                "WHERE deleted = 1").fetchone()[0], 1)

    def test_delete_misplaced_object_makes_cleanups(self):
        broker = ContainerBroker(':memory:', account='a', container='c')
        broker.initialize(normalize_timestamp('1'), 0)
        broker.put_object('o', normalize_timestamp('2'), 0, 'text/plain',
                          'd41d8cd98f00b204e9800998ecf8427e', 0)
        self.assertEqual(len(broker.list_cleanups(10000)), 0)  # sanity check

        # now it's been deleted, but from a different policy than it started
        # in, so we'll note that someone has to go clean up the old object
        broker.delete_object('o', normalize_timestamp('3'), 282)
        cleanups = broker.list_cleanups(10000)
        self.assertEqual(len(cleanups), 1)
        self.assertEqual(cleanups[0]['name'], 'o')
        self.assertEqual(cleanups[0]['created_at'], normalize_timestamp('2'))
        self.assertEqual(cleanups[0]['storage_policy_index'], 0)

        # deleting an already-deleted object doesn't make cleanup records
        broker.delete_object('o', normalize_timestamp('4'), 148)
        cleanups = broker.list_cleanups(10000)
        self.assertEqual(len(cleanups), 1)

        # if a PUT from the past in a different policy comes in, make a
        # cleanup record
        broker.put_object('o2', normalize_timestamp('10'), 0, 'text/plain',
                          'd41d8cd98f00b204e9800998ecf8427e', 1)
        broker.put_object('o2', normalize_timestamp('9.99'), 0, 'text/plain',
                          'd41d8cd98f00b204e9800998ecf8427e', 0)
        cleanups = broker.list_cleanups(10000)
        self.assertEqual(len(cleanups), 2)

        # DELETEs from the past don't make cleanups
        broker.put_object('o3', normalize_timestamp('10'), 0, 'text/plain',
                          'd41d8cd98f00b204e9800998ecf8427e', 1)
        broker.delete_object('o3', normalize_timestamp('9.99'), 0)
        self.assertEqual(len(cleanups), 2)

    def test_put_object(self):
        # Test ContainerBroker.put_object
        broker = ContainerBroker(':memory:', account='a', container='c')
        broker.initialize(normalize_timestamp('1'), 0)

        # Create initial object
        timestamp = normalize_timestamp(time())
        broker.put_object('"{<object \'&\' name>}"', timestamp, 123,
                          'application/x-test',
                          '5af83e3196bf99f440f31f2e1a6c9afe', 0)
        with broker.get() as conn:
            self.assertEquals(conn.execute(
                "SELECT name FROM object").fetchone()[0],
                '"{<object \'&\' name>}"')
            self.assertEquals(conn.execute(
                "SELECT created_at FROM object").fetchone()[0], timestamp)
            self.assertEquals(conn.execute(
                "SELECT size FROM object").fetchone()[0], 123)
            self.assertEquals(conn.execute(
                "SELECT content_type FROM object").fetchone()[0],
                'application/x-test')
            self.assertEquals(conn.execute(
                "SELECT etag FROM object").fetchone()[0],
                '5af83e3196bf99f440f31f2e1a6c9afe')
            self.assertEquals(conn.execute(
                "SELECT deleted FROM object").fetchone()[0], 0)

        # Reput same event
        broker.put_object('"{<object \'&\' name>}"', timestamp, 123,
                          'application/x-test',
                          '5af83e3196bf99f440f31f2e1a6c9afe', 0)
        with broker.get() as conn:
            self.assertEquals(conn.execute(
                "SELECT name FROM object").fetchone()[0],
                '"{<object \'&\' name>}"')
            self.assertEquals(conn.execute(
                "SELECT created_at FROM object").fetchone()[0], timestamp)
            self.assertEquals(conn.execute(
                "SELECT size FROM object").fetchone()[0], 123)
            self.assertEquals(conn.execute(
                "SELECT content_type FROM object").fetchone()[0],
                'application/x-test')
            self.assertEquals(conn.execute(
                "SELECT etag FROM object").fetchone()[0],
                '5af83e3196bf99f440f31f2e1a6c9afe')
            self.assertEquals(conn.execute(
                "SELECT deleted FROM object").fetchone()[0], 0)

        # Put new event
        sleep(.00001)
        timestamp = normalize_timestamp(time())
        broker.put_object('"{<object \'&\' name>}"', timestamp, 124,
                          'application/x-test',
                          'aa0749bacbc79ec65fe206943d8fe449', 0)
        with broker.get() as conn:
            self.assertEquals(conn.execute(
                "SELECT name FROM object").fetchone()[0],
                '"{<object \'&\' name>}"')
            self.assertEquals(conn.execute(
                "SELECT created_at FROM object").fetchone()[0], timestamp)
            self.assertEquals(conn.execute(
                "SELECT size FROM object").fetchone()[0], 124)
            self.assertEquals(conn.execute(
                "SELECT content_type FROM object").fetchone()[0],
                'application/x-test')
            self.assertEquals(conn.execute(
                "SELECT etag FROM object").fetchone()[0],
                'aa0749bacbc79ec65fe206943d8fe449')
            self.assertEquals(conn.execute(
                "SELECT deleted FROM object").fetchone()[0], 0)

        # Put old event
        otimestamp = normalize_timestamp(float(timestamp) - 1)
        broker.put_object('"{<object \'&\' name>}"', otimestamp, 124,
                          'application/x-test',
                          'aa0749bacbc79ec65fe206943d8fe449', 0)
        with broker.get() as conn:
            self.assertEquals(conn.execute(
                "SELECT name FROM object").fetchone()[0],
                '"{<object \'&\' name>}"')
            self.assertEquals(conn.execute(
                "SELECT created_at FROM object").fetchone()[0], timestamp)
            self.assertEquals(conn.execute(
                "SELECT size FROM object").fetchone()[0], 124)
            self.assertEquals(conn.execute(
                "SELECT content_type FROM object").fetchone()[0],
                'application/x-test')
            self.assertEquals(conn.execute(
                "SELECT etag FROM object").fetchone()[0],
                'aa0749bacbc79ec65fe206943d8fe449')
            self.assertEquals(conn.execute(
                "SELECT deleted FROM object").fetchone()[0], 0)

        # Put old delete event
        dtimestamp = normalize_timestamp(float(timestamp) - 1)
        broker.put_object('"{<object \'&\' name>}"', dtimestamp, 0, '', '',
                          deleted=1, storage_policy_index=0)
        with broker.get() as conn:
            self.assertEquals(conn.execute(
                "SELECT name FROM object").fetchone()[0],
                '"{<object \'&\' name>}"')
            self.assertEquals(conn.execute(
                "SELECT created_at FROM object").fetchone()[0], timestamp)
            self.assertEquals(conn.execute(
                "SELECT size FROM object").fetchone()[0], 124)
            self.assertEquals(conn.execute(
                "SELECT content_type FROM object").fetchone()[0],
                'application/x-test')
            self.assertEquals(conn.execute(
                "SELECT etag FROM object").fetchone()[0],
                'aa0749bacbc79ec65fe206943d8fe449')
            self.assertEquals(conn.execute(
                "SELECT deleted FROM object").fetchone()[0], 0)

        # Put new delete event
        sleep(.00001)
        timestamp = normalize_timestamp(time())
        broker.put_object('"{<object \'&\' name>}"', timestamp, 0, '', '',
                          deleted=1, storage_policy_index=0)
        with broker.get() as conn:
            self.assertEquals(conn.execute(
                "SELECT name FROM object").fetchone()[0],
                '"{<object \'&\' name>}"')
            self.assertEquals(conn.execute(
                "SELECT created_at FROM object").fetchone()[0], timestamp)
            self.assertEquals(conn.execute(
                "SELECT deleted FROM object").fetchone()[0], 1)

        # Put new event
        sleep(.00001)
        timestamp = normalize_timestamp(time())
        broker.put_object('"{<object \'&\' name>}"', timestamp, 123,
                          'application/x-test',
                          '5af83e3196bf99f440f31f2e1a6c9afe', 0)
        with broker.get() as conn:
            self.assertEquals(conn.execute(
                "SELECT name FROM object").fetchone()[0],
                '"{<object \'&\' name>}"')
            self.assertEquals(conn.execute(
                "SELECT created_at FROM object").fetchone()[0], timestamp)
            self.assertEquals(conn.execute(
                "SELECT size FROM object").fetchone()[0], 123)
            self.assertEquals(conn.execute(
                "SELECT content_type FROM object").fetchone()[0],
                'application/x-test')
            self.assertEquals(conn.execute(
                "SELECT etag FROM object").fetchone()[0],
                '5af83e3196bf99f440f31f2e1a6c9afe')
            self.assertEquals(conn.execute(
                "SELECT deleted FROM object").fetchone()[0], 0)

        # We'll use this later
        sleep(.0001)
        in_between_timestamp = normalize_timestamp(time())

        # New post event
        sleep(.0001)
        previous_timestamp = timestamp
        timestamp = normalize_timestamp(time())
        with broker.get() as conn:
            self.assertEquals(conn.execute(
                "SELECT name FROM object").fetchone()[0],
                '"{<object \'&\' name>}"')
            self.assertEquals(conn.execute(
                "SELECT created_at FROM object").fetchone()[0],
                previous_timestamp)
            self.assertEquals(conn.execute(
                "SELECT size FROM object").fetchone()[0], 123)
            self.assertEquals(conn.execute(
                "SELECT content_type FROM object").fetchone()[0],
                'application/x-test')
            self.assertEquals(conn.execute(
                "SELECT etag FROM object").fetchone()[0],
                '5af83e3196bf99f440f31f2e1a6c9afe')
            self.assertEquals(conn.execute(
                "SELECT deleted FROM object").fetchone()[0], 0)

        # Put event from after last put but before last post
        timestamp = in_between_timestamp
        broker.put_object('"{<object \'&\' name>}"', timestamp, 456,
                          'application/x-test3',
                          '6af83e3196bf99f440f31f2e1a6c9afe', 0)
        with broker.get() as conn:
            self.assertEquals(conn.execute(
                "SELECT name FROM object").fetchone()[0],
                '"{<object \'&\' name>}"')
            self.assertEquals(conn.execute(
                "SELECT created_at FROM object").fetchone()[0], timestamp)
            self.assertEquals(conn.execute(
                "SELECT size FROM object").fetchone()[0], 456)
            self.assertEquals(conn.execute(
                "SELECT content_type FROM object").fetchone()[0],
                'application/x-test3')
            self.assertEquals(conn.execute(
                "SELECT etag FROM object").fetchone()[0],
                '6af83e3196bf99f440f31f2e1a6c9afe')
            self.assertEquals(conn.execute(
                "SELECT deleted FROM object").fetchone()[0], 0)

    def test_put_object_writes_cleanup_on_policy_index_change(self):
        right_policy = 0
        wrong_policy = 5
        broker = ContainerBroker(':memory:', account='a', container='c')
        broker.initialize(normalize_timestamp('1'), right_policy)

        timestamp = normalize_timestamp(100)
        size_bytes = 7149
        etag = 'cf71b384ada9f0fb31dda69470635b2d'

        # initial put -> no cleanup needed
        broker.put_object('obj', timestamp, size_bytes, 'text/plain',
                          etag, right_policy)
        self.assertEqual(broker.list_cleanups(10000), [])

        # policy index stays the same -> no cleanup needed
        timestamp = normalize_timestamp(200)
        broker.put_object('obj', timestamp, size_bytes, 'text/plain',
                          etag, right_policy)
        self.assertEqual(broker.list_cleanups(10000), [])

        # policy index changes -> cleanup needed
        timestamp = normalize_timestamp(300)
        broker.put_object('obj', timestamp, size_bytes, 'text/plain',
                          etag, wrong_policy)
        self.assertEqual(
            broker.list_cleanups(10000),
            [{'name': 'obj', 'created_at': normalize_timestamp(200),
              'storage_policy_index': right_policy}])

        # policy index becomes right -> another cleanup
        timestamp = normalize_timestamp(400)
        broker.put_object('obj', timestamp, size_bytes, 'text/plain',
                          etag, right_policy)
        self.assertEqual(
            broker.list_cleanups(10000),
            [{'name': 'obj', 'created_at': normalize_timestamp(200),
              'storage_policy_index': right_policy},
             {'name': 'obj', 'created_at': normalize_timestamp(300),
              'storage_policy_index': wrong_policy}])

        # and we can delete the cleanups
        broker.delete_cleanup({'name': 'obj',
                               'created_at': normalize_timestamp(200),
                               'storage_policy_index': right_policy})
        self.assertEqual(
            broker.list_cleanups(10000),
            [{'name': 'obj', 'created_at': normalize_timestamp(300),
              'storage_policy_index': wrong_policy}])

        broker.delete_cleanup({'name': 'obj',
                               'created_at': normalize_timestamp(300),
                               'storage_policy_index': wrong_policy})
        self.assertEqual(broker.list_cleanups(10000), [])

    def test_setting_policy_delayed(self):
        # in-memory DBs take a different code path to get to merge_items than
        # on-disk ones do, so we set up an on-disk DB
        tempdir = mkdtemp()
        broker_path = os.path.join(tempdir, 'test-set-policy.db')
        try:
            broker = ContainerBroker(broker_path, account='real',
                                     container='ondisk')
            broker.initialize(normalize_timestamp(1), 0)
            broker.put_object('o', normalize_timestamp(time()), 0,
                              'text/plain', 'etag', 12)
            broker._commit_puts()
            with broker.get() as conn:
                results = list(conn.execute('''
                    SELECT name, storage_policy_index FROM object
                '''))
            self.assertEqual(len(results), 1)
            self.assertEqual(dict(results[0]),
                             {'name': 'o', 'storage_policy_index': 12})
        finally:
            rmtree(tempdir)

    def test_load_old_pending_puts(self):
        # pending puts from pre-storage-policy container brokers won't contain
        # the storage policy index
        tempdir = mkdtemp()
        broker_path = os.path.join(tempdir, 'test-load-old.db')
        try:
            broker = ContainerBroker(broker_path, account='real',
                                     container='ondisk')
            broker.initialize(normalize_timestamp(1), 0)
            with open(broker_path + '.pending', 'a+b') as pending:
                pending.write(':')
                pending.write(pickle.dumps(
                    # name, timestamp, size, content type, etag, deleted
                    ('oldobj', normalize_timestamp(200), 0,
                     'text/plain', 'some-etag', 0)).encode('base64'))

            broker._commit_puts()
            with broker.get() as conn:
                results = list(conn.execute('''
                    SELECT name, storage_policy_index FROM object
                '''))
            self.assertEqual(len(results), 1)
            self.assertEqual(dict(results[0]),
                             {'name': 'oldobj', 'storage_policy_index': 0})
        finally:
            rmtree(tempdir)

    def test_get_info(self):
        # Test ContainerBroker.get_info
        broker = ContainerBroker(':memory:', account='test1',
                                 container='test2')
        broker.initialize(normalize_timestamp('1'), 0)

        info = broker.get_info()
        self.assertEquals(info['account'], 'test1')
        self.assertEquals(info['container'], 'test2')
        self.assertEquals(info['hash'], '00000000000000000000000000000000')

        info = broker.get_info()
        self.assertEquals(info['object_count'], 0)
        self.assertEquals(info['bytes_used'], 0)

        broker.put_object('o1', normalize_timestamp(time()), 123, 'text/plain',
                          '5af83e3196bf99f440f31f2e1a6c9afe', 0)
        info = broker.get_info()
        self.assertEquals(info['object_count'], 1)
        self.assertEquals(info['bytes_used'], 123)

        sleep(.00001)
        broker.put_object('o2', normalize_timestamp(time()), 123, 'text/plain',
                          '5af83e3196bf99f440f31f2e1a6c9afe', 0)
        info = broker.get_info()
        self.assertEquals(info['object_count'], 2)
        self.assertEquals(info['bytes_used'], 246)

        sleep(.00001)
        broker.put_object('o2', normalize_timestamp(time()), 1000,
                          'text/plain', '5af83e3196bf99f440f31f2e1a6c9afe', 0)
        info = broker.get_info()
        self.assertEquals(info['object_count'], 2)
        self.assertEquals(info['bytes_used'], 1123)

        sleep(.00001)
        broker.delete_object('o1', normalize_timestamp(time()), 0)
        info = broker.get_info()
        self.assertEquals(info['object_count'], 1)
        self.assertEquals(info['bytes_used'], 1000)

        sleep(.00001)
        broker.delete_object('o2', normalize_timestamp(time()), 0)
        info = broker.get_info()
        self.assertEquals(info['object_count'], 0)
        self.assertEquals(info['bytes_used'], 0)

        info = broker.get_info()
        self.assertEquals(info['x_container_sync_point1'], -1)
        self.assertEquals(info['x_container_sync_point2'], -1)

    def test_set_x_syncs(self):
        broker = ContainerBroker(':memory:', account='test1',
                                 container='test2')
        broker.initialize(normalize_timestamp('1'), 0)

        info = broker.get_info()
        self.assertEquals(info['x_container_sync_point1'], -1)
        self.assertEquals(info['x_container_sync_point2'], -1)

        broker.set_x_container_sync_points(1, 2)
        info = broker.get_info()
        self.assertEquals(info['x_container_sync_point1'], 1)
        self.assertEquals(info['x_container_sync_point2'], 2)

    def test_get_report_info(self):
        broker = ContainerBroker(':memory:', account='test1',
                                 container='test2')
        broker.initialize(normalize_timestamp('1'), 0)

        info = broker.get_info()
        self.assertEquals(info['account'], 'test1')
        self.assertEquals(info['container'], 'test2')
        self.assertEquals(info['object_count'], 0)
        self.assertEquals(info['bytes_used'], 0)
        self.assertEquals(info['reported_object_count'], 0)
        self.assertEquals(info['reported_bytes_used'], 0)

        broker.put_object('o1', normalize_timestamp(time()), 123, 'text/plain',
                          '5af83e3196bf99f440f31f2e1a6c9afe', 0)
        info = broker.get_info()
        self.assertEquals(info['object_count'], 1)
        self.assertEquals(info['bytes_used'], 123)
        self.assertEquals(info['reported_object_count'], 0)
        self.assertEquals(info['reported_bytes_used'], 0)

        sleep(.00001)
        broker.put_object('o2', normalize_timestamp(time()), 123, 'text/plain',
                          '5af83e3196bf99f440f31f2e1a6c9afe', 0)
        info = broker.get_info()
        self.assertEquals(info['object_count'], 2)
        self.assertEquals(info['bytes_used'], 246)
        self.assertEquals(info['reported_object_count'], 0)
        self.assertEquals(info['reported_bytes_used'], 0)

        sleep(.00001)
        broker.put_object('o2', normalize_timestamp(time()), 1000,
                          'text/plain', '5af83e3196bf99f440f31f2e1a6c9afe', 0)
        info = broker.get_info()
        self.assertEquals(info['object_count'], 2)
        self.assertEquals(info['bytes_used'], 1123)
        self.assertEquals(info['reported_object_count'], 0)
        self.assertEquals(info['reported_bytes_used'], 0)

        put_timestamp = normalize_timestamp(time())
        sleep(.001)
        delete_timestamp = normalize_timestamp(time())
        broker.reported(put_timestamp, delete_timestamp, 2, 1123)
        info = broker.get_info()
        self.assertEquals(info['object_count'], 2)
        self.assertEquals(info['bytes_used'], 1123)
        self.assertEquals(info['reported_put_timestamp'], put_timestamp)
        self.assertEquals(info['reported_delete_timestamp'], delete_timestamp)
        self.assertEquals(info['reported_object_count'], 2)
        self.assertEquals(info['reported_bytes_used'], 1123)

        sleep(.00001)
        broker.delete_object('o1', normalize_timestamp(time()), 0)
        info = broker.get_info()
        self.assertEquals(info['object_count'], 1)
        self.assertEquals(info['bytes_used'], 1000)
        self.assertEquals(info['reported_object_count'], 2)
        self.assertEquals(info['reported_bytes_used'], 1123)

        sleep(.00001)
        broker.delete_object('o2', normalize_timestamp(time()), 0)
        info = broker.get_info()
        self.assertEquals(info['object_count'], 0)
        self.assertEquals(info['bytes_used'], 0)
        self.assertEquals(info['reported_object_count'], 2)
        self.assertEquals(info['reported_bytes_used'], 1123)

    def test_list_objects_iter(self):
        # Test ContainerBroker.list_objects_iter
        broker = ContainerBroker(':memory:', account='a', container='c')
        broker.initialize(normalize_timestamp('1'), 0)
        for obj1 in xrange(4):
            for obj2 in xrange(125):
                broker.put_object('%d/%04d' % (obj1, obj2),
                                  normalize_timestamp(time()), 0, 'text/plain',
                                  'd41d8cd98f00b204e9800998ecf8427e', 0)
        for obj in xrange(125):
            broker.put_object('2/0051/%04d' % obj,
                              normalize_timestamp(time()), 0, 'text/plain',
                              'd41d8cd98f00b204e9800998ecf8427e', 0)

        for obj in xrange(125):
            broker.put_object('3/%04d/0049' % obj,
                              normalize_timestamp(time()), 0, 'text/plain',
                              'd41d8cd98f00b204e9800998ecf8427e', 0)

        listing = broker.list_objects_iter(100, '', None, None, '')
        self.assertEquals(len(listing), 100)
        self.assertEquals(listing[0][0], '0/0000')
        self.assertEquals(listing[-1][0], '0/0099')

        listing = broker.list_objects_iter(100, '', '0/0050', None, '')
        self.assertEquals(len(listing), 50)
        self.assertEquals(listing[0][0], '0/0000')
        self.assertEquals(listing[-1][0], '0/0049')

        listing = broker.list_objects_iter(100, '0/0099', None, None, '')
        self.assertEquals(len(listing), 100)
        self.assertEquals(listing[0][0], '0/0100')
        self.assertEquals(listing[-1][0], '1/0074')

        listing = broker.list_objects_iter(55, '1/0074', None, None, '')
        self.assertEquals(len(listing), 55)
        self.assertEquals(listing[0][0], '1/0075')
        self.assertEquals(listing[-1][0], '2/0004')

        listing = broker.list_objects_iter(10, '', None, '0/01', '')
        self.assertEquals(len(listing), 10)
        self.assertEquals(listing[0][0], '0/0100')
        self.assertEquals(listing[-1][0], '0/0109')

        listing = broker.list_objects_iter(10, '', None, '0/', '/')
        self.assertEquals(len(listing), 10)
        self.assertEquals(listing[0][0], '0/0000')
        self.assertEquals(listing[-1][0], '0/0009')

        # Same as above, but using the path argument.
        listing = broker.list_objects_iter(10, '', None, None, '', '0')
        self.assertEquals(len(listing), 10)
        self.assertEquals(listing[0][0], '0/0000')
        self.assertEquals(listing[-1][0], '0/0009')

        listing = broker.list_objects_iter(10, '', None, '', '/')
        self.assertEquals(len(listing), 4)
        self.assertEquals([row[0] for row in listing],
                          ['0/', '1/', '2/', '3/'])

        listing = broker.list_objects_iter(10, '2', None, None, '/')
        self.assertEquals(len(listing), 2)
        self.assertEquals([row[0] for row in listing], ['2/', '3/'])

        listing = broker.list_objects_iter(10, '2/', None, None, '/')
        self.assertEquals(len(listing), 1)
        self.assertEquals([row[0] for row in listing], ['3/'])

        listing = broker.list_objects_iter(10, '2/0050', None, '2/', '/')
        self.assertEquals(len(listing), 10)
        self.assertEquals(listing[0][0], '2/0051')
        self.assertEquals(listing[1][0], '2/0051/')
        self.assertEquals(listing[2][0], '2/0052')
        self.assertEquals(listing[-1][0], '2/0059')

        listing = broker.list_objects_iter(10, '3/0045', None, '3/', '/')
        self.assertEquals(len(listing), 10)
        self.assertEquals([row[0] for row in listing],
                          ['3/0045/', '3/0046', '3/0046/', '3/0047',
                           '3/0047/', '3/0048', '3/0048/', '3/0049',
                           '3/0049/', '3/0050'])

        broker.put_object('3/0049/', normalize_timestamp(time()), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e', 0)
        listing = broker.list_objects_iter(10, '3/0048', None, None, None)
        self.assertEquals(len(listing), 10)
        self.assertEquals(
            [row[0] for row in listing],
            ['3/0048/0049', '3/0049', '3/0049/',
             '3/0049/0049', '3/0050', '3/0050/0049', '3/0051', '3/0051/0049',
             '3/0052', '3/0052/0049'])

        listing = broker.list_objects_iter(10, '3/0048', None, '3/', '/')
        self.assertEquals(len(listing), 10)
        self.assertEquals(
            [row[0] for row in listing],
            ['3/0048/', '3/0049', '3/0049/', '3/0050',
             '3/0050/', '3/0051', '3/0051/', '3/0052', '3/0052/', '3/0053'])

        listing = broker.list_objects_iter(10, None, None, '3/0049/', '/')
        self.assertEquals(len(listing), 2)
        self.assertEquals(
            [row[0] for row in listing],
            ['3/0049/', '3/0049/0049'])

        listing = broker.list_objects_iter(10, None, None, None, None,
                                           '3/0049')
        self.assertEquals(len(listing), 1)
        self.assertEquals([row[0] for row in listing], ['3/0049/0049'])

        listing = broker.list_objects_iter(2, None, None, '3/', '/')
        self.assertEquals(len(listing), 2)
        self.assertEquals([row[0] for row in listing], ['3/0000', '3/0000/'])

        listing = broker.list_objects_iter(2, None, None, None, None, '3')
        self.assertEquals(len(listing), 2)
        self.assertEquals([row[0] for row in listing], ['3/0000', '3/0001'])

    def test_list_objects_iter_non_slash(self):
        # Test ContainerBroker.list_objects_iter using a
        # delimiter that is not a slash
        broker = ContainerBroker(':memory:', account='a', container='c')
        broker.initialize(normalize_timestamp('1'), 0)
        for obj1 in xrange(4):
            for obj2 in xrange(125):
                broker.put_object('%d:%04d' % (obj1, obj2),
                                  normalize_timestamp(time()), 0, 'text/plain',
                                  'd41d8cd98f00b204e9800998ecf8427e', 0)
        for obj in xrange(125):
            broker.put_object('2:0051:%04d' % obj,
                              normalize_timestamp(time()), 0, 'text/plain',
                              'd41d8cd98f00b204e9800998ecf8427e', 0)

        for obj in xrange(125):
            broker.put_object('3:%04d:0049' % obj,
                              normalize_timestamp(time()), 0, 'text/plain',
                              'd41d8cd98f00b204e9800998ecf8427e', 0)

        listing = broker.list_objects_iter(100, '', None, None, '')
        self.assertEquals(len(listing), 100)
        self.assertEquals(listing[0][0], '0:0000')
        self.assertEquals(listing[-1][0], '0:0099')

        listing = broker.list_objects_iter(100, '', '0:0050', None, '')
        self.assertEquals(len(listing), 50)
        self.assertEquals(listing[0][0], '0:0000')
        self.assertEquals(listing[-1][0], '0:0049')

        listing = broker.list_objects_iter(100, '0:0099', None, None, '')
        self.assertEquals(len(listing), 100)
        self.assertEquals(listing[0][0], '0:0100')
        self.assertEquals(listing[-1][0], '1:0074')

        listing = broker.list_objects_iter(55, '1:0074', None, None, '')
        self.assertEquals(len(listing), 55)
        self.assertEquals(listing[0][0], '1:0075')
        self.assertEquals(listing[-1][0], '2:0004')

        listing = broker.list_objects_iter(10, '', None, '0:01', '')
        self.assertEquals(len(listing), 10)
        self.assertEquals(listing[0][0], '0:0100')
        self.assertEquals(listing[-1][0], '0:0109')

        listing = broker.list_objects_iter(10, '', None, '0:', ':')
        self.assertEquals(len(listing), 10)
        self.assertEquals(listing[0][0], '0:0000')
        self.assertEquals(listing[-1][0], '0:0009')

        # Same as above, but using the path argument, so nothing should be
        # returned since path uses a '/' as a delimiter.
        listing = broker.list_objects_iter(10, '', None, None, '', '0')
        self.assertEquals(len(listing), 0)

        listing = broker.list_objects_iter(10, '', None, '', ':')
        self.assertEquals(len(listing), 4)
        self.assertEquals([row[0] for row in listing],
                          ['0:', '1:', '2:', '3:'])

        listing = broker.list_objects_iter(10, '2', None, None, ':')
        self.assertEquals(len(listing), 2)
        self.assertEquals([row[0] for row in listing], ['2:', '3:'])

        listing = broker.list_objects_iter(10, '2:', None, None, ':')
        self.assertEquals(len(listing), 1)
        self.assertEquals([row[0] for row in listing], ['3:'])

        listing = broker.list_objects_iter(10, '2:0050', None, '2:', ':')
        self.assertEquals(len(listing), 10)
        self.assertEquals(listing[0][0], '2:0051')
        self.assertEquals(listing[1][0], '2:0051:')
        self.assertEquals(listing[2][0], '2:0052')
        self.assertEquals(listing[-1][0], '2:0059')

        listing = broker.list_objects_iter(10, '3:0045', None, '3:', ':')
        self.assertEquals(len(listing), 10)
        self.assertEquals([row[0] for row in listing],
                          ['3:0045:', '3:0046', '3:0046:', '3:0047',
                           '3:0047:', '3:0048', '3:0048:', '3:0049',
                           '3:0049:', '3:0050'])

        broker.put_object('3:0049:', normalize_timestamp(time()), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e', 0)
        listing = broker.list_objects_iter(10, '3:0048', None, None, None)
        self.assertEquals(len(listing), 10)
        self.assertEquals(
            [row[0] for row in listing],
            ['3:0048:0049', '3:0049', '3:0049:',
             '3:0049:0049', '3:0050', '3:0050:0049', '3:0051', '3:0051:0049',
             '3:0052', '3:0052:0049'])

        listing = broker.list_objects_iter(10, '3:0048', None, '3:', ':')
        self.assertEquals(len(listing), 10)
        self.assertEquals(
            [row[0] for row in listing],
            ['3:0048:', '3:0049', '3:0049:', '3:0050',
             '3:0050:', '3:0051', '3:0051:', '3:0052', '3:0052:', '3:0053'])

        listing = broker.list_objects_iter(10, None, None, '3:0049:', ':')
        self.assertEquals(len(listing), 2)
        self.assertEquals(
            [row[0] for row in listing],
            ['3:0049:', '3:0049:0049'])

        # Same as above, but using the path argument, so nothing should be
        # returned since path uses a '/' as a delimiter.
        listing = broker.list_objects_iter(10, None, None, None, None,
                                           '3:0049')
        self.assertEquals(len(listing), 0)

        listing = broker.list_objects_iter(2, None, None, '3:', ':')
        self.assertEquals(len(listing), 2)
        self.assertEquals([row[0] for row in listing], ['3:0000', '3:0000:'])

        listing = broker.list_objects_iter(2, None, None, None, None, '3')
        self.assertEquals(len(listing), 0)

    def test_list_objects_iter_prefix_delim(self):
        # Test ContainerBroker.list_objects_iter
        broker = ContainerBroker(':memory:', account='a', container='c')
        broker.initialize(normalize_timestamp('1'), 0)

        broker.put_object(
            '/pets/dogs/1', normalize_timestamp(0), 0,
            'text/plain', 'd41d8cd98f00b204e9800998ecf8427e', 0)
        broker.put_object(
            '/pets/dogs/2', normalize_timestamp(0), 0,
            'text/plain', 'd41d8cd98f00b204e9800998ecf8427e', 0)
        broker.put_object(
            '/pets/fish/a', normalize_timestamp(0), 0,
            'text/plain', 'd41d8cd98f00b204e9800998ecf8427e', 0)
        broker.put_object(
            '/pets/fish/b', normalize_timestamp(0), 0,
            'text/plain', 'd41d8cd98f00b204e9800998ecf8427e', 0)
        broker.put_object(
            '/pets/fish_info.txt', normalize_timestamp(0), 0,
            'text/plain', 'd41d8cd98f00b204e9800998ecf8427e', 0)
        broker.put_object(
            '/snakes', normalize_timestamp(0), 0,
            'text/plain', 'd41d8cd98f00b204e9800998ecf8427e', 0)

        #def list_objects_iter(self, limit, marker, prefix, delimiter,
        #                      path=None, format=None):
        listing = broker.list_objects_iter(100, None, None, '/pets/f', '/')
        self.assertEquals([row[0] for row in listing],
                          ['/pets/fish/', '/pets/fish_info.txt'])
        listing = broker.list_objects_iter(100, None, None, '/pets/fish', '/')
        self.assertEquals([row[0] for row in listing],
                          ['/pets/fish/', '/pets/fish_info.txt'])
        listing = broker.list_objects_iter(100, None, None, '/pets/fish/', '/')
        self.assertEquals([row[0] for row in listing],
                          ['/pets/fish/a', '/pets/fish/b'])

    def test_double_check_trailing_delimiter(self):
        # Test ContainerBroker.list_objects_iter for a
        # container that has an odd file with a trailing delimiter
        broker = ContainerBroker(':memory:', account='a', container='c')
        broker.initialize(normalize_timestamp('1'), 0)
        broker.put_object('a', normalize_timestamp(time()), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e', 0)
        broker.put_object('a/', normalize_timestamp(time()), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e', 0)
        broker.put_object('a/a', normalize_timestamp(time()), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e', 0)
        broker.put_object('a/a/a', normalize_timestamp(time()), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e', 0)
        broker.put_object('a/a/b', normalize_timestamp(time()), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e', 0)
        broker.put_object('a/b', normalize_timestamp(time()), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e', 0)
        broker.put_object('b', normalize_timestamp(time()), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e', 0)
        broker.put_object('b/a', normalize_timestamp(time()), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e', 0)
        broker.put_object('b/b', normalize_timestamp(time()), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e', 0)
        broker.put_object('c', normalize_timestamp(time()), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e', 0)
        broker.put_object('a/0', normalize_timestamp(time()), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e', 0)
        broker.put_object('0', normalize_timestamp(time()), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e', 0)
        broker.put_object('0/', normalize_timestamp(time()), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e', 0)
        broker.put_object('00', normalize_timestamp(time()), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e', 0)
        broker.put_object('0/0', normalize_timestamp(time()), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e', 0)
        broker.put_object('0/00', normalize_timestamp(time()), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e', 0)
        broker.put_object('0/1', normalize_timestamp(time()), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e', 0)
        broker.put_object('0/1/', normalize_timestamp(time()), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e', 0)
        broker.put_object('0/1/0', normalize_timestamp(time()), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e', 0)
        broker.put_object('1', normalize_timestamp(time()), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e', 0)
        broker.put_object('1/', normalize_timestamp(time()), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e', 0)
        broker.put_object('1/0', normalize_timestamp(time()), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e', 0)
        listing = broker.list_objects_iter(25, None, None, None, None)
        self.assertEquals(len(listing), 22)
        self.assertEquals(
            [row[0] for row in listing],
            ['0', '0/', '0/0', '0/00', '0/1', '0/1/', '0/1/0', '00', '1', '1/',
             '1/0', 'a', 'a/', 'a/0', 'a/a', 'a/a/a', 'a/a/b', 'a/b', 'b',
             'b/a', 'b/b', 'c'])
        listing = broker.list_objects_iter(25, None, None, '', '/')
        self.assertEquals(len(listing), 10)
        self.assertEquals(
            [row[0] for row in listing],
            ['0', '0/', '00', '1', '1/', 'a', 'a/', 'b', 'b/', 'c'])
        listing = broker.list_objects_iter(25, None, None, 'a/', '/')
        self.assertEquals(len(listing), 5)
        self.assertEquals(
            [row[0] for row in listing],
            ['a/', 'a/0', 'a/a', 'a/a/', 'a/b'])
        listing = broker.list_objects_iter(25, None, None, '0/', '/')
        self.assertEquals(len(listing), 5)
        self.assertEquals(
            [row[0] for row in listing],
            ['0/', '0/0', '0/00', '0/1', '0/1/'])
        listing = broker.list_objects_iter(25, None, None, '0/1/', '/')
        self.assertEquals(len(listing), 2)
        self.assertEquals(
            [row[0] for row in listing],
            ['0/1/', '0/1/0'])
        listing = broker.list_objects_iter(25, None, None, 'b/', '/')
        self.assertEquals(len(listing), 2)
        self.assertEquals([row[0] for row in listing], ['b/a', 'b/b'])

    def test_double_check_trailing_delimiter_non_slash(self):
        # Test ContainerBroker.list_objects_iter for a
        # container that has an odd file with a trailing delimiter
        broker = ContainerBroker(':memory:', account='a', container='c')
        broker.initialize(normalize_timestamp('1'), 0)
        broker.put_object('a', normalize_timestamp(time()), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e',
                          0, 0)
        broker.put_object('a:', normalize_timestamp(time()), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e',
                          0, 0)
        broker.put_object('a:a', normalize_timestamp(time()), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e',
                          0, 0)
        broker.put_object('a:a:a', normalize_timestamp(time()), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e',
                          0, 0)
        broker.put_object('a:a:b', normalize_timestamp(time()), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e',
                          0, 0)
        broker.put_object('a:b', normalize_timestamp(time()), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e',
                          0, 0)
        broker.put_object('b', normalize_timestamp(time()), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e',
                          0, 0)
        broker.put_object('b:a', normalize_timestamp(time()), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e',
                          0, 0)
        broker.put_object('b:b', normalize_timestamp(time()), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e',
                          0, 0)
        broker.put_object('c', normalize_timestamp(time()), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e',
                          0, 0)
        broker.put_object('a:0', normalize_timestamp(time()), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e',
                          0, 0)
        broker.put_object('0', normalize_timestamp(time()), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e',
                          0, 0)
        broker.put_object('0:', normalize_timestamp(time()), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e',
                          0, 0)
        broker.put_object('00', normalize_timestamp(time()), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e',
                          0, 0)
        broker.put_object('0:0', normalize_timestamp(time()), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e',
                          0, 0)
        broker.put_object('0:00', normalize_timestamp(time()), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e',
                          0, 0)
        broker.put_object('0:1', normalize_timestamp(time()), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e',
                          0, 0)
        broker.put_object('0:1:', normalize_timestamp(time()), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e',
                          0, 0)
        broker.put_object('0:1:0', normalize_timestamp(time()), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e',
                          0, 0)
        broker.put_object('1', normalize_timestamp(time()), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e',
                          0, 0)
        broker.put_object('1:', normalize_timestamp(time()), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e',
                          0, 0)
        broker.put_object('1:0', normalize_timestamp(time()), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e',
                          0, 0)
        listing = broker.list_objects_iter(25, None, None, None, None)
        self.assertEquals(len(listing), 22)
        self.assertEquals(
            [row[0] for row in listing],
            ['0', '00', '0:', '0:0', '0:00', '0:1', '0:1:', '0:1:0', '1', '1:',
             '1:0', 'a', 'a:', 'a:0', 'a:a', 'a:a:a', 'a:a:b', 'a:b', 'b',
             'b:a', 'b:b', 'c'])
        listing = broker.list_objects_iter(25, None, None, '', ':')
        self.assertEquals(len(listing), 10)
        self.assertEquals(
            [row[0] for row in listing],
            ['0', '00', '0:', '1', '1:', 'a', 'a:', 'b', 'b:', 'c'])
        listing = broker.list_objects_iter(25, None, None, 'a:', ':')
        self.assertEquals(len(listing), 5)
        self.assertEquals(
            [row[0] for row in listing],
            ['a:', 'a:0', 'a:a', 'a:a:', 'a:b'])
        listing = broker.list_objects_iter(25, None, None, '0:', ':')
        self.assertEquals(len(listing), 5)
        self.assertEquals(
            [row[0] for row in listing],
            ['0:', '0:0', '0:00', '0:1', '0:1:'])
        listing = broker.list_objects_iter(25, None, None, '0:1:', ':')
        self.assertEquals(len(listing), 2)
        self.assertEquals(
            [row[0] for row in listing],
            ['0:1:', '0:1:0'])
        listing = broker.list_objects_iter(25, None, None, 'b:', ':')
        self.assertEquals(len(listing), 2)
        self.assertEquals([row[0] for row in listing], ['b:a', 'b:b'])

    def test_list_objects_iter_includes_policy(self):
        broker = ContainerBroker(':memory:', account='a', container='c')
        broker.initialize(normalize_timestamp('1'), 0)
        broker.put_object('a', normalize_timestamp(100), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e', 0)
        broker.put_object('b', normalize_timestamp(101), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e', 1)
        broker.put_object('c', normalize_timestamp(101), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e', 2)
        listing = broker.list_objects_iter(25, None, None, None, None)
        self.assertEquals(len(listing), 3)
        self.assertEquals([(row[0], row[5]) for row in listing],
                          [('a', 0), ('b', 1), ('c', 2)])

    def test_list_misplaced_objects(self):
        broker = ContainerBroker(':memory:', account='a', container='c')
        broker.initialize(normalize_timestamp('1'), 0)
        broker.put_object('aaa', normalize_timestamp(100), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e', 0)
        broker.put_object('aac', normalize_timestamp(101), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e', 1)
        broker.put_object('aae', normalize_timestamp(101), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e', 2)
        listing = broker.list_misplaced_objects(25)
        self.assertEquals(len(listing), 2)
        self.assertEquals([(row[0], row[5]) for row in listing],
                          [('aac', 1), ('aae', 2)])

        listing = broker.list_misplaced_objects(25, marker='aab')
        self.assertEquals(len(listing), 2)
        self.assertEquals([(row[0], row[5]) for row in listing],
                          [('aac', 1), ('aae', 2)])

        listing = broker.list_misplaced_objects(25, marker='aac')
        self.assertEquals(len(listing), 1)
        self.assertEquals([(row[0], row[5]) for row in listing],
                          [('aae', 2)])

    def test_chexor(self):
        broker = ContainerBroker(':memory:', account='a', container='c')
        broker.initialize(normalize_timestamp('1'), 0)
        broker.put_object('a', normalize_timestamp(1), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e', 0)
        broker.put_object('b', normalize_timestamp(2), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e', 0)
        hasha = hashlib.md5('%s-%s' % ('a', '0000000001.00000')).digest()
        hashb = hashlib.md5('%s-%s' % ('b', '0000000002.00000')).digest()
        hashc = ''.join(
            ('%2x' % (ord(a) ^ ord(b)) for a, b in zip(hasha, hashb)))
        self.assertEquals(broker.get_info()['hash'], hashc)
        broker.put_object('b', normalize_timestamp(3), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e', 0)
        hashb = hashlib.md5('%s-%s' % ('b', '0000000003.00000')).digest()
        hashc = ''.join(
            ('%02x' % (ord(a) ^ ord(b)) for a, b in zip(hasha, hashb)))
        self.assertEquals(broker.get_info()['hash'], hashc)

    def test_newid(self):
        # test DatabaseBroker.newid
        broker = ContainerBroker(':memory:', account='a', container='c')
        broker.initialize(normalize_timestamp('1'), 0)
        id = broker.get_info()['id']
        broker.newid('someid')
        self.assertNotEquals(id, broker.get_info()['id'])

    def test_get_items_since(self):
        # test DatabaseBroker.get_items_since
        broker = ContainerBroker(':memory:', account='a', container='c')
        broker.initialize(normalize_timestamp('1'), 0)
        broker.put_object('a', normalize_timestamp(1), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e', 0)
        max_row = broker.get_replication_info()['max_row']
        broker.put_object('b', normalize_timestamp(2), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e', 0)
        items = broker.get_items_since(max_row, 1000)
        self.assertEquals(len(items), 1)
        self.assertEquals(items[0]['name'], 'b')

    def test_sync_merging(self):
        # exercise the DatabaseBroker sync functions a bit
        broker1 = ContainerBroker(':memory:', account='a', container='c')
        broker1.initialize(normalize_timestamp('1'), 0)
        broker2 = ContainerBroker(':memory:', account='a', container='c')
        broker2.initialize(normalize_timestamp('1'), 0)
        self.assertEquals(broker2.get_sync('12345'), -1)
        broker1.merge_syncs([{'sync_point': 3, 'remote_id': '12345'}])
        broker2.merge_syncs(broker1.get_syncs())
        self.assertEquals(broker2.get_sync('12345'), 3)

    def test_merge_items(self):
        broker1 = ContainerBroker(':memory:', account='a', container='c')
        broker1.initialize(normalize_timestamp('1'), 0)
        broker2 = ContainerBroker(':memory:', account='a', container='c')
        broker2.initialize(normalize_timestamp('1'), 0)
        broker1.put_object('a', normalize_timestamp(1), 0,
                           'text/plain', 'd41d8cd98f00b204e9800998ecf8427e', 0)
        broker1.put_object('b', normalize_timestamp(2), 0,
                           'text/plain', 'd41d8cd98f00b204e9800998ecf8427e', 0)
        id = broker1.get_info()['id']
        broker2.merge_items(broker1.get_items_since(
            broker2.get_sync(id), 1000), id)
        items = broker2.get_items_since(-1, 1000)
        self.assertEquals(len(items), 2)
        self.assertEquals(['a', 'b'], sorted([rec['name'] for rec in items]))
        broker1.put_object('c', normalize_timestamp(3), 0,
                           'text/plain', 'd41d8cd98f00b204e9800998ecf8427e', 0)
        broker2.merge_items(broker1.get_items_since(
            broker2.get_sync(id), 1000), id)
        items = broker2.get_items_since(-1, 1000)
        self.assertEquals(len(items), 3)
        self.assertEquals(['a', 'b', 'c'],
                          sorted([rec['name'] for rec in items]))

    def test_merge_items_overwrite(self):
        # test DatabaseBroker.merge_items
        broker1 = ContainerBroker(':memory:', account='a', container='c')
        broker1.initialize(normalize_timestamp('1'), 0)
        id = broker1.get_info()['id']
        broker2 = ContainerBroker(':memory:', account='a', container='c')
        broker2.initialize(normalize_timestamp('1'), 0)
        broker1.put_object('a', normalize_timestamp(2), 0,
                           'text/plain', 'd41d8cd98f00b204e9800998ecf8427e', 0)
        broker1.put_object('b', normalize_timestamp(3), 0,
                           'text/plain', 'd41d8cd98f00b204e9800998ecf8427e', 0)
        broker2.merge_items(broker1.get_items_since(
            broker2.get_sync(id), 1000), id)
        broker1.put_object('a', normalize_timestamp(4), 0,
                           'text/plain', 'd41d8cd98f00b204e9800998ecf8427e', 0)
        broker2.merge_items(broker1.get_items_since(
            broker2.get_sync(id), 1000), id)
        items = broker2.get_items_since(-1, 1000)
        self.assertEquals(['a', 'b'], sorted([rec['name'] for rec in items]))
        for rec in items:
            if rec['name'] == 'a':
                self.assertEquals(rec['created_at'], normalize_timestamp(4))
            if rec['name'] == 'b':
                self.assertEquals(rec['created_at'], normalize_timestamp(3))

    def test_merge_items_post_overwrite_out_of_order(self):
        # test DatabaseBroker.merge_items
        broker1 = ContainerBroker(':memory:', account='a', container='c')
        broker1.initialize(normalize_timestamp('1'), 0)
        id = broker1.get_info()['id']
        broker2 = ContainerBroker(':memory:', account='a', container='c')
        broker2.initialize(normalize_timestamp('1'), 0)
        broker1.put_object('a', normalize_timestamp(2), 0,
                           'text/plain', 'd41d8cd98f00b204e9800998ecf8427e', 0)
        broker1.put_object('b', normalize_timestamp(3), 0,
                           'text/plain', 'd41d8cd98f00b204e9800998ecf8427e', 0)
        broker2.merge_items(broker1.get_items_since(
            broker2.get_sync(id), 1000), id)
        broker1.put_object('a', normalize_timestamp(4), 0,
                           'text/plain', 'd41d8cd98f00b204e9800998ecf8427e', 0)
        broker2.merge_items(broker1.get_items_since(
            broker2.get_sync(id), 1000), id)
        items = broker2.get_items_since(-1, 1000)
        self.assertEquals(['a', 'b'], sorted([rec['name'] for rec in items]))
        for rec in items:
            if rec['name'] == 'a':
                self.assertEquals(rec['created_at'], normalize_timestamp(4))
            if rec['name'] == 'b':
                self.assertEquals(rec['created_at'], normalize_timestamp(3))
                self.assertEquals(rec['content_type'], 'text/plain')
        items = broker2.get_items_since(-1, 1000)
        self.assertEquals(['a', 'b'], sorted([rec['name'] for rec in items]))
        for rec in items:
            if rec['name'] == 'a':
                self.assertEquals(rec['created_at'], normalize_timestamp(4))
            if rec['name'] == 'b':
                self.assertEquals(rec['created_at'], normalize_timestamp(3))
        broker1.put_object('b', normalize_timestamp(5), 0,
                           'text/plain', 'd41d8cd98f00b204e9800998ecf8427e', 0)
        broker2.merge_items(broker1.get_items_since(
            broker2.get_sync(id), 1000), id)
        items = broker2.get_items_since(-1, 1000)
        self.assertEquals(['a', 'b'], sorted([rec['name'] for rec in items]))
        for rec in items:
            if rec['name'] == 'a':
                self.assertEquals(rec['created_at'], normalize_timestamp(4))
            if rec['name'] == 'b':
                self.assertEquals(rec['created_at'], normalize_timestamp(5))
                self.assertEquals(rec['content_type'], 'text/plain')

    def test_set_storage_policy_index(self):
        broker = ContainerBroker(':memory:', account='test_account',
                                 container='test_container')
        broker.initialize(normalize_timestamp('1'), 0)
        broker.put_object('a1', normalize_timestamp(time()), 0, 'text/plain',
                          'd41d8cd98f00b204e9800998ecf8427e', 111)
        broker.put_object('a2', normalize_timestamp(time()), 0, 'text/plain',
                          'd41d8cd98f00b204e9800998ecf8427e', 111)

        broker.put_object('b1', normalize_timestamp(time()), 0, 'text/plain',
                          'd41d8cd98f00b204e9800998ecf8427e', 222)
        broker.put_object('b2', normalize_timestamp(time()), 0, 'text/plain',
                          'd41d8cd98f00b204e9800998ecf8427e', 222)
        broker.put_object('b3', normalize_timestamp(time()), 0, 'text/plain',
                          'd41d8cd98f00b204e9800998ecf8427e', 222)

        info = broker.get_info()
        self.assertEqual(0, info['storage_policy_index'])  # sanity checks
        self.assertEqual(5, info['misplaced_object_count'])

        broker.set_storage_policy_index(111)
        self.assertEqual(broker.storage_policy_index, 111)
        info = broker.get_info()
        print repr(info)
        self.assertEqual(111, info['storage_policy_index'])
        self.assertEqual(3, info['misplaced_object_count'])

        broker.set_storage_policy_index(222)
        self.assertEqual(broker.storage_policy_index, 222)
        info = broker.get_info()
        self.assertEqual(222, info['storage_policy_index'])
        self.assertEqual(2, info['misplaced_object_count'])

        # it's idempotent
        broker.set_storage_policy_index(222)
        info = broker.get_info()
        self.assertEqual(222, info['storage_policy_index'])
        self.assertEqual(2, info['misplaced_object_count'])

        # deleted objects don't count
        broker.delete_object('b1', normalize_timestamp(time()), 0)
        broker.delete_object('b2', normalize_timestamp(time()), 0)
        broker.set_storage_policy_index(111)
        info = broker.get_info()
        self.assertEqual(1, info['misplaced_object_count'])

    def test_set_storage_policy_index_empty(self):
        # Putting an object may trigger migrations, so test with a
        # never-had-an-object container to make sure we handle it
        broker = ContainerBroker(':memory:', account='test_account',
                                 container='test_container')
        broker.initialize(normalize_timestamp('1'), 0)
        info = broker.get_info()
        self.assertEqual(0, info['storage_policy_index'])

        broker.set_storage_policy_index(2)
        info = broker.get_info()
        self.assertEqual(2, info['storage_policy_index'])

    def test_merge_items_misplaced(self):
        broker = ContainerBroker(':memory:', account='test_account',
                                 container='test_container')
        broker.initialize(normalize_timestamp('1'), 0)
        self.assertEqual(0, broker.get_info()['misplaced_object_count'])

        broker.put_object('abc', normalize_timestamp(time()), 0, 'text/plain',
                          'd41d8cd98f00b204e9800998ecf8427e', 0)
        broker.put_object('def', normalize_timestamp(time()), 0, 'text/plain',
                          'd41d8cd98f00b204e9800998ecf8427e', 0)
        self.assertEqual(0, broker.get_info()['misplaced_object_count'])

        # this object is in the wrong place (doesn't match container)
        broker.put_object('ghi', normalize_timestamp(time()), 0, 'text/plain',
                          'd41d8cd98f00b204e9800998ecf8427e', 1234)
        self.assertEqual(1, broker.get_info()['misplaced_object_count'])

        # now it's in the right place (matches container)
        broker.put_object('ghi', normalize_timestamp(time()), 0, 'text/plain',
                          'd41d8cd98f00b204e9800998ecf8427e', 0)
        self.assertEqual(0, broker.get_info()['misplaced_object_count'])

        # it's not just a boolean
        broker.put_object('jkl1', normalize_timestamp(time()), 0, 'text/plain',
                          'd41d8cd98f00b204e9800998ecf8427e', 1234)
        broker.put_object('jkl2', normalize_timestamp(time()), 0, 'text/plain',
                          'd41d8cd98f00b204e9800998ecf8427e', 1234)
        self.assertEqual(2, broker.get_info()['misplaced_object_count'])

        # deleting a misplaced object drops the count back down
        broker.delete_object('jkl2', normalize_timestamp(time()), 1234)
        self.assertEqual(1, broker.get_info()['misplaced_object_count'])
        broker.delete_object('jkl1', normalize_timestamp(time()), 1234)
        self.assertEqual(0, broker.get_info()['misplaced_object_count'])

        # deleting an object we didn't know about doesn't affect the count
        broker.delete_object('nonesuch', normalize_timestamp(time()), 0)
        self.assertEqual(0, broker.get_info()['misplaced_object_count'])

    def test_storage_policy_index_in_replication_info(self):
        broker = ContainerBroker(':memory:', account='a', container='c')
        broker.initialize(normalize_timestamp('1'), 0)
        self.assertEqual(
            0, broker.get_replication_info().get('storage_policy_index'))

        broker.set_storage_policy_index(4931)
        self.assertEqual(
            4931, broker.get_replication_info().get('storage_policy_index'))


def prespi_create_object_table(self, conn):
    """
    Copied from ContainerBroker before the storage_policy_index column
    was added to the objects table.

    Create the object table which is specific to the container DB.
    Not a part of Pluggable Back-ends, internal to the baseline code.

    :param conn: DB connection object
    """
    conn.executescript("""
        CREATE TABLE object (
            ROWID INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            created_at TEXT,
            size INTEGER,
            content_type TEXT,
            etag TEXT,
            deleted INTEGER DEFAULT 0
        );

        CREATE INDEX ix_object_deleted_name ON object (deleted, name);

        CREATE TRIGGER object_insert AFTER INSERT ON object
        BEGIN
            UPDATE container_stat
            SET object_count = object_count + (1 - new.deleted),
                bytes_used = bytes_used + new.size,
                hash = chexor(hash, new.name, new.created_at);
        END;

        CREATE TRIGGER object_update BEFORE UPDATE ON object
        BEGIN
            SELECT RAISE(FAIL, 'UPDATE not allowed; DELETE and INSERT');
        END;

        CREATE TRIGGER object_delete AFTER DELETE ON object
        BEGIN
            UPDATE container_stat
            SET object_count = object_count - (1 - old.deleted),
                bytes_used = bytes_used - old.size,
                hash = chexor(hash, old.name, old.created_at);
        END;
    """)


def premetadata_create_container_stat_table(self, conn, put_timestamp,
                                            _spi=None):
    """
    Copied from ContainerBroker before the metadata column was
    added; used for testing with TestContainerBrokerBeforeMetadata.

    Create the container_stat table which is specific to the container DB.

    :param conn: DB connection object
    :param put_timestamp: put timestamp
    """
    if put_timestamp is None:
        put_timestamp = normalize_timestamp(0)
    conn.executescript('''
        CREATE TABLE container_stat (
            account TEXT,
            container TEXT,
            created_at TEXT,
            put_timestamp TEXT DEFAULT '0',
            delete_timestamp TEXT DEFAULT '0',
            object_count INTEGER,
            bytes_used INTEGER,
            reported_put_timestamp TEXT DEFAULT '0',
            reported_delete_timestamp TEXT DEFAULT '0',
            reported_object_count INTEGER DEFAULT 0,
            reported_bytes_used INTEGER DEFAULT 0,
            hash TEXT default '00000000000000000000000000000000',
            id TEXT,
            status TEXT DEFAULT '',
            status_changed_at TEXT DEFAULT '0'
        );

        INSERT INTO container_stat (object_count, bytes_used)
            VALUES (0, 0);
    ''')
    conn.execute('''
        UPDATE container_stat
        SET account = ?, container = ?, created_at = ?, id = ?,
            put_timestamp = ?
    ''', (self.account, self.container, normalize_timestamp(time()),
          str(uuid4()), put_timestamp))


class TestContainerBrokerBeforeMetadata(TestContainerBroker):
    """
    Tests for ContainerBroker against databases created before
    the metadata column was added.
    """

    def setUp(self):
        self._imported_create_container_stat_table = \
            ContainerBroker.create_container_stat_table
        self._imported_create_object_table = \
            ContainerBroker.create_object_table
        ContainerBroker.create_container_stat_table = \
            premetadata_create_container_stat_table
        ContainerBroker.create_object_table = prespi_create_object_table
        broker = ContainerBroker(':memory:', account='a', container='c')
        broker.initialize(normalize_timestamp('1'), 0)
        exc = None
        with broker.get() as conn:
            try:
                conn.execute('SELECT metadata FROM container_stat')
            except BaseException as err:
                exc = err
        self.assert_('no such column: metadata' in str(exc))

    def tearDown(self):
        ContainerBroker.create_container_stat_table = \
            self._imported_create_container_stat_table
        ContainerBroker.create_object_table = \
            self._imported_create_object_table
        broker = ContainerBroker(':memory:', account='a', container='c')
        broker.initialize(normalize_timestamp('1'), 0)
        with broker.get() as conn:
            conn.execute('SELECT metadata FROM container_stat')


def prexsync_create_container_stat_table(self, conn, put_timestamp,
                                         _spi=None):
    """
    Copied from ContainerBroker before the
    x_container_sync_point[12] columns were added; used for testing with
    TestContainerBrokerBeforeXSync.

    Create the container_stat table which is specific to the container DB.

    :param conn: DB connection object
    :param put_timestamp: put timestamp
    """
    if put_timestamp is None:
        put_timestamp = normalize_timestamp(0)
    conn.executescript("""
        CREATE TABLE container_stat (
            account TEXT,
            container TEXT,
            created_at TEXT,
            put_timestamp TEXT DEFAULT '0',
            delete_timestamp TEXT DEFAULT '0',
            object_count INTEGER,
            bytes_used INTEGER,
            reported_put_timestamp TEXT DEFAULT '0',
            reported_delete_timestamp TEXT DEFAULT '0',
            reported_object_count INTEGER DEFAULT 0,
            reported_bytes_used INTEGER DEFAULT 0,
            hash TEXT default '00000000000000000000000000000000',
            id TEXT,
            status TEXT DEFAULT '',
            status_changed_at TEXT DEFAULT '0',
            metadata TEXT DEFAULT ''
        );

        INSERT INTO container_stat (object_count, bytes_used)
            VALUES (0, 0);
    """)
    conn.execute('''
        UPDATE container_stat
        SET account = ?, container = ?, created_at = ?, id = ?,
            put_timestamp = ?
    ''', (self.account, self.container, normalize_timestamp(time()),
          str(uuid4()), put_timestamp))


class TestContainerBrokerBeforeXSync(TestContainerBroker):
    """
    Tests for ContainerBroker against databases created
    before the x_container_sync_point[12] columns were added.
    """

    def setUp(self):
        self._imported_create_container_stat_table = \
            ContainerBroker.create_container_stat_table
        self._imported_create_object_table = \
            ContainerBroker.create_object_table
        ContainerBroker.create_container_stat_table = \
            prexsync_create_container_stat_table
        ContainerBroker.create_object_table = prespi_create_object_table
        broker = ContainerBroker(':memory:', account='a', container='c')
        broker.initialize(normalize_timestamp('1'), 0)
        exc = None
        with broker.get() as conn:
            try:
                conn.execute('''SELECT x_container_sync_point1
                                FROM container_stat''')
            except BaseException as err:
                exc = err
        self.assert_('no such column: x_container_sync_point1' in str(exc))

    def tearDown(self):
        ContainerBroker.create_container_stat_table = \
            self._imported_create_container_stat_table
        ContainerBroker.create_object_table = \
            self._imported_create_object_table
        broker = ContainerBroker(':memory:', account='a', container='c')
        broker.initialize(normalize_timestamp('1'), 0)
        with broker.get() as conn:
            conn.execute('SELECT x_container_sync_point1 FROM container_stat')


def prespi_create_container_stat_table(self, conn, put_timestamp,
                                       _spi=None):
    """
    Copied from ContainerBroker before the
    storage_policy_index column was added; used for testing with
    TestContainerBrokerBeforeSPI.

    Create the container_stat table which is specific to the container DB.

    :param conn: DB connection object
    :param put_timestamp: put timestamp
    """
    if put_timestamp is None:
        put_timestamp = normalize_timestamp(0)
    conn.executescript("""
        CREATE TABLE container_stat (
            account TEXT,
            container TEXT,
            created_at TEXT,
            put_timestamp TEXT DEFAULT '0',
            delete_timestamp TEXT DEFAULT '0',
            object_count INTEGER,
            bytes_used INTEGER,
            reported_put_timestamp TEXT DEFAULT '0',
            reported_delete_timestamp TEXT DEFAULT '0',
            reported_object_count INTEGER DEFAULT 0,
            reported_bytes_used INTEGER DEFAULT 0,
            hash TEXT default '00000000000000000000000000000000',
            id TEXT,
            status TEXT DEFAULT '',
            status_changed_at TEXT DEFAULT '0',
            metadata TEXT DEFAULT '',
            x_container_sync_point1 INTEGER DEFAULT -1,
            x_container_sync_point2 INTEGER DEFAULT -1
        );

        INSERT INTO container_stat (object_count, bytes_used)
            VALUES (0, 0);
    """)
    conn.execute('''
        UPDATE container_stat
        SET account = ?, container = ?, created_at = ?, id = ?,
            put_timestamp = ?
    ''', (self.account, self.container, normalize_timestamp(time()),
          str(uuid4()), put_timestamp))


# grf, we need to put the old objects table in here too
class TestContainerBrokerBeforeSPI(TestContainerBroker):
    """
    Tests for ContainerBroker against databases created
    before the storage_policy_index column was added.
    """

    def setUp(self):
        self._imported_create_container_stat_table = \
            ContainerBroker.create_container_stat_table
        self._imported_create_object_table = \
            ContainerBroker.create_object_table
        ContainerBroker.create_container_stat_table = \
            prespi_create_container_stat_table
        ContainerBroker.create_object_table = prespi_create_object_table
        broker = ContainerBroker(':memory:', account='a', container='c')
        broker.initialize(normalize_timestamp('1'), 0)
        exc = None
        with broker.get() as conn:
            try:
                conn.execute('''SELECT storage_policy_index
                                FROM container_stat''')
            except BaseException as err:
                exc = err
        self.assert_('no such column: storage_policy_index' in str(exc))

    def tearDown(self):
        ContainerBroker.create_container_stat_table = \
            self._imported_create_container_stat_table
        ContainerBroker.create_object_table = \
            self._imported_create_object_table
        broker = ContainerBroker(':memory:', account='a', container='c')
        broker.initialize(normalize_timestamp('1'), 0)
        with broker.get() as conn:
            conn.execute('SELECT storage_policy_index FROM container_stat')
