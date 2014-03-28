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

""" Tests for swift.account.backend """

import hashlib
import unittest
import pickle
import os
from time import sleep, time
from uuid import uuid4
import functools
from tempfile import mkdtemp
from shutil import rmtree
import sqlite3
import itertools
from contextlib import contextmanager

from swift.account.backend import AccountBroker
from swift.common.utils import normalize_timestamp
from swift.common.db import DatabaseConnectionError
from test.unit import patch_policies
from swift.common.storage_policy import StoragePolicy, POLICIES


@patch_policies
class TestAccountBroker(unittest.TestCase):
    """Tests for AccountBroker"""

    def test_creation(self):
        # Test AccountBroker.__init__
        broker = AccountBroker(':memory:', account='a')
        self.assertEqual(broker.db_file, ':memory:')
        try:
            with broker.get() as conn:
                pass
        except DatabaseConnectionError as e:
            self.assertTrue(hasattr(e, 'path'))
            self.assertEquals(e.path, ':memory:')
            self.assertTrue(hasattr(e, 'msg'))
            self.assertEquals(e.msg, "DB doesn't exist")
        except Exception as e:
            self.fail("Unexpected exception raised: %r" % e)
        else:
            self.fail("Expected a DatabaseConnectionError exception")
        broker.initialize(normalize_timestamp('1'))
        with broker.get() as conn:
            curs = conn.cursor()
            curs.execute('SELECT 1')
            self.assertEqual(curs.fetchall()[0][0], 1)

    def test_exception(self):
        # Test AccountBroker throwing a conn away after exception
        first_conn = None
        broker = AccountBroker(':memory:', account='a')
        broker.initialize(normalize_timestamp('1'))
        with broker.get() as conn:
            first_conn = conn
        try:
            with broker.get() as conn:
                self.assertEqual(first_conn, conn)
                raise Exception('OMG')
        except Exception:
            pass
        self.assert_(broker.conn is None)

    def test_empty(self):
        # Test AccountBroker.empty
        broker = AccountBroker(':memory:', account='a')
        broker.initialize(normalize_timestamp('1'))
        self.assert_(broker.empty())
        broker.put_container('o', normalize_timestamp(time()), 0, 0, 0,
                             POLICIES.default.idx)
        self.assert_(not broker.empty())
        sleep(.00001)
        broker.put_container('o', 0, normalize_timestamp(time()), 0, 0,
                             POLICIES.default.idx)
        self.assert_(broker.empty())

    def test_reclaim(self):
        broker = AccountBroker(':memory:', account='test_account')
        broker.initialize(normalize_timestamp('1'))
        broker.put_container('c', normalize_timestamp(time()), 0, 0, 0,
                             POLICIES.default.idx)
        with broker.get() as conn:
            self.assertEqual(conn.execute(
                "SELECT count(*) FROM container "
                "WHERE deleted = 0").fetchone()[0], 1)
            self.assertEqual(conn.execute(
                "SELECT count(*) FROM container "
                "WHERE deleted = 1").fetchone()[0], 0)
        broker.reclaim(normalize_timestamp(time() - 999), time())
        with broker.get() as conn:
            self.assertEqual(conn.execute(
                "SELECT count(*) FROM container "
                "WHERE deleted = 0").fetchone()[0], 1)
            self.assertEqual(conn.execute(
                "SELECT count(*) FROM container "
                "WHERE deleted = 1").fetchone()[0], 0)
        sleep(.00001)
        broker.put_container('c', 0, normalize_timestamp(time()), 0, 0,
                             POLICIES.default.idx)
        with broker.get() as conn:
            self.assertEqual(conn.execute(
                "SELECT count(*) FROM container "
                "WHERE deleted = 0").fetchone()[0], 0)
            self.assertEqual(conn.execute(
                "SELECT count(*) FROM container "
                "WHERE deleted = 1").fetchone()[0], 1)
        broker.reclaim(normalize_timestamp(time() - 999), time())
        with broker.get() as conn:
            self.assertEqual(conn.execute(
                "SELECT count(*) FROM container "
                "WHERE deleted = 0").fetchone()[0], 0)
            self.assertEqual(conn.execute(
                "SELECT count(*) FROM container "
                "WHERE deleted = 1").fetchone()[0], 1)
        sleep(.00001)
        broker.reclaim(normalize_timestamp(time()), time())
        with broker.get() as conn:
            self.assertEqual(conn.execute(
                "SELECT count(*) FROM container "
                "WHERE deleted = 0").fetchone()[0], 0)
            self.assertEqual(conn.execute(
                "SELECT count(*) FROM container "
                "WHERE deleted = 1").fetchone()[0], 0)
        # Test reclaim after deletion. Create 3 test containers
        broker.put_container('x', 0, 0, 0, 0, POLICIES.default.idx)
        broker.put_container('y', 0, 0, 0, 0, POLICIES.default.idx)
        broker.put_container('z', 0, 0, 0, 0, POLICIES.default.idx)
        broker.reclaim(normalize_timestamp(time()), time())
        # self.assertEqual(len(res), 2)
        # self.assert_(isinstance(res, tuple))
        # containers, account_name = res
        # self.assert_(containers is None)
        # self.assert_(account_name is None)
        # Now delete the account
        broker.delete_db(normalize_timestamp(time()))
        broker.reclaim(normalize_timestamp(time()), time())
        # self.assertEqual(len(res), 2)
        # self.assert_(isinstance(res, tuple))
        # containers, account_name = res
        # self.assertEqual(account_name, 'test_account')
        # self.assertEqual(len(containers), 3)
        # self.assert_('x' in containers)
        # self.assert_('y' in containers)
        # self.assert_('z' in containers)
        # self.assert_('a' not in containers)

    def test_delete_container(self):
        # Test AccountBroker.delete_container
        broker = AccountBroker(':memory:', account='a')
        broker.initialize(normalize_timestamp('1'))
        broker.put_container('o', normalize_timestamp(time()), 0, 0, 0,
                             POLICIES.default.idx)
        with broker.get() as conn:
            self.assertEqual(conn.execute(
                "SELECT count(*) FROM container "
                "WHERE deleted = 0").fetchone()[0], 1)
            self.assertEqual(conn.execute(
                "SELECT count(*) FROM container "
                "WHERE deleted = 1").fetchone()[0], 0)
        sleep(.00001)
        broker.put_container('o', 0, normalize_timestamp(time()), 0, 0,
                             POLICIES.default.idx)
        with broker.get() as conn:
            self.assertEqual(conn.execute(
                "SELECT count(*) FROM container "
                "WHERE deleted = 0").fetchone()[0], 0)
            self.assertEqual(conn.execute(
                "SELECT count(*) FROM container "
                "WHERE deleted = 1").fetchone()[0], 1)

    def test_put_container(self):
        # Test AccountBroker.put_container
        broker = AccountBroker(':memory:', account='a')
        broker.initialize(normalize_timestamp('1'))

        # Create initial container
        timestamp = normalize_timestamp(time())
        broker.put_container('"{<container \'&\' name>}"', timestamp, 0, 0, 0,
                             POLICIES.default.idx)
        with broker.get() as conn:
            self.assertEqual(conn.execute(
                "SELECT name FROM container").fetchone()[0],
                '"{<container \'&\' name>}"')
            self.assertEqual(conn.execute(
                "SELECT put_timestamp FROM container").fetchone()[0],
                timestamp)
            self.assertEqual(conn.execute(
                "SELECT deleted FROM container").fetchone()[0], 0)

        # Reput same event
        broker.put_container('"{<container \'&\' name>}"', timestamp, 0, 0, 0,
                             POLICIES.default.idx)
        with broker.get() as conn:
            self.assertEqual(conn.execute(
                "SELECT name FROM container").fetchone()[0],
                '"{<container \'&\' name>}"')
            self.assertEqual(conn.execute(
                "SELECT put_timestamp FROM container").fetchone()[0],
                timestamp)
            self.assertEqual(conn.execute(
                "SELECT deleted FROM container").fetchone()[0], 0)

        # Put new event
        sleep(.00001)
        timestamp = normalize_timestamp(time())
        broker.put_container('"{<container \'&\' name>}"', timestamp, 0, 0, 0,
                             POLICIES.default.idx)
        with broker.get() as conn:
            self.assertEqual(conn.execute(
                "SELECT name FROM container").fetchone()[0],
                '"{<container \'&\' name>}"')
            self.assertEqual(conn.execute(
                "SELECT put_timestamp FROM container").fetchone()[0],
                timestamp)
            self.assertEqual(conn.execute(
                "SELECT deleted FROM container").fetchone()[0], 0)

        # Put old event
        otimestamp = normalize_timestamp(float(timestamp) - 1)
        broker.put_container('"{<container \'&\' name>}"', otimestamp, 0, 0, 0,
                             POLICIES.default.idx)
        with broker.get() as conn:
            self.assertEqual(conn.execute(
                "SELECT name FROM container").fetchone()[0],
                '"{<container \'&\' name>}"')
            self.assertEqual(conn.execute(
                "SELECT put_timestamp FROM container").fetchone()[0],
                timestamp)
            self.assertEqual(conn.execute(
                "SELECT deleted FROM container").fetchone()[0], 0)

        # Put old delete event
        dtimestamp = normalize_timestamp(float(timestamp) - 1)
        broker.put_container('"{<container \'&\' name>}"', 0, dtimestamp, 0, 0,
                             POLICIES.default.idx)
        with broker.get() as conn:
            self.assertEqual(conn.execute(
                "SELECT name FROM container").fetchone()[0],
                '"{<container \'&\' name>}"')
            self.assertEqual(conn.execute(
                "SELECT put_timestamp FROM container").fetchone()[0],
                timestamp)
            self.assertEqual(conn.execute(
                "SELECT delete_timestamp FROM container").fetchone()[0],
                dtimestamp)
            self.assertEqual(conn.execute(
                "SELECT deleted FROM container").fetchone()[0], 0)

        # Put new delete event
        sleep(.00001)
        timestamp = normalize_timestamp(time())
        broker.put_container('"{<container \'&\' name>}"', 0, timestamp, 0, 0,
                             POLICIES.default.idx)
        with broker.get() as conn:
            self.assertEqual(conn.execute(
                "SELECT name FROM container").fetchone()[0],
                '"{<container \'&\' name>}"')
            self.assertEqual(conn.execute(
                "SELECT delete_timestamp FROM container").fetchone()[0],
                timestamp)
            self.assertEqual(conn.execute(
                "SELECT deleted FROM container").fetchone()[0], 1)

        # Put new event
        sleep(.00001)
        timestamp = normalize_timestamp(time())
        broker.put_container('"{<container \'&\' name>}"', timestamp, 0, 0, 0,
                             POLICIES.default.idx)
        with broker.get() as conn:
            self.assertEqual(conn.execute(
                "SELECT name FROM container").fetchone()[0],
                '"{<container \'&\' name>}"')
            self.assertEqual(conn.execute(
                "SELECT put_timestamp FROM container").fetchone()[0],
                timestamp)
            self.assertEqual(conn.execute(
                "SELECT deleted FROM container").fetchone()[0], 0)

    def test_get_info(self):
        # Test AccountBroker.get_info
        broker = AccountBroker(':memory:', account='test1')
        broker.initialize(normalize_timestamp('1'))

        info = broker.get_info()
        self.assertEqual(info['account'], 'test1')
        self.assertEqual(info['hash'], '00000000000000000000000000000000')

        info = broker.get_info()
        self.assertEqual(info['container_count'], 0)

        broker.put_container('c1', normalize_timestamp(time()), 0, 0, 0,
                             POLICIES.default.idx)
        info = broker.get_info()
        self.assertEqual(info['container_count'], 1)

        sleep(.00001)
        broker.put_container('c2', normalize_timestamp(time()), 0, 0, 0,
                             POLICIES.default.idx)
        info = broker.get_info()
        self.assertEqual(info['container_count'], 2)

        sleep(.00001)
        broker.put_container('c2', normalize_timestamp(time()), 0, 0, 0,
                             POLICIES.default.idx)
        info = broker.get_info()
        self.assertEqual(info['container_count'], 2)

        sleep(.00001)
        broker.put_container('c1', 0, normalize_timestamp(time()), 0, 0,
                             POLICIES.default.idx)
        info = broker.get_info()
        self.assertEqual(info['container_count'], 1)

        sleep(.00001)
        broker.put_container('c2', 0, normalize_timestamp(time()), 0, 0,
                             POLICIES.default.idx)
        info = broker.get_info()
        self.assertEqual(info['container_count'], 0)

    def test_list_containers_iter(self):
        # Test AccountBroker.list_containers_iter
        broker = AccountBroker(':memory:', account='a')
        broker.initialize(normalize_timestamp('1'))
        for cont1 in xrange(4):
            for cont2 in xrange(125):
                broker.put_container('%d-%04d' % (cont1, cont2),
                                     normalize_timestamp(time()), 0, 0, 0,
                                     POLICIES.default.idx)
        for cont in xrange(125):
            broker.put_container('2-0051-%04d' % cont,
                                 normalize_timestamp(time()), 0, 0, 0,
                                 POLICIES.default.idx)

        for cont in xrange(125):
            broker.put_container('3-%04d-0049' % cont,
                                 normalize_timestamp(time()), 0, 0, 0,
                                 POLICIES.default.idx)

        listing = broker.list_containers_iter(100, '', None, None, '')
        self.assertEqual(len(listing), 100)
        self.assertEqual(listing[0][0], '0-0000')
        self.assertEqual(listing[-1][0], '0-0099')

        listing = broker.list_containers_iter(100, '', '0-0050', None, '')
        self.assertEqual(len(listing), 50)
        self.assertEqual(listing[0][0], '0-0000')
        self.assertEqual(listing[-1][0], '0-0049')

        listing = broker.list_containers_iter(100, '0-0099', None, None, '')
        self.assertEqual(len(listing), 100)
        self.assertEqual(listing[0][0], '0-0100')
        self.assertEqual(listing[-1][0], '1-0074')

        listing = broker.list_containers_iter(55, '1-0074', None, None, '')
        self.assertEqual(len(listing), 55)
        self.assertEqual(listing[0][0], '1-0075')
        self.assertEqual(listing[-1][0], '2-0004')

        listing = broker.list_containers_iter(10, '', None, '0-01', '')
        self.assertEqual(len(listing), 10)
        self.assertEqual(listing[0][0], '0-0100')
        self.assertEqual(listing[-1][0], '0-0109')

        listing = broker.list_containers_iter(10, '', None, '0-01', '-')
        self.assertEqual(len(listing), 10)
        self.assertEqual(listing[0][0], '0-0100')
        self.assertEqual(listing[-1][0], '0-0109')

        listing = broker.list_containers_iter(10, '', None, '0-', '-')
        self.assertEqual(len(listing), 10)
        self.assertEqual(listing[0][0], '0-0000')
        self.assertEqual(listing[-1][0], '0-0009')

        listing = broker.list_containers_iter(10, '', None, '', '-')
        self.assertEqual(len(listing), 4)
        self.assertEqual([row[0] for row in listing],
                         ['0-', '1-', '2-', '3-'])

        listing = broker.list_containers_iter(10, '2-', None, None, '-')
        self.assertEqual(len(listing), 1)
        self.assertEqual([row[0] for row in listing], ['3-'])

        listing = broker.list_containers_iter(10, '', None, '2', '-')
        self.assertEqual(len(listing), 1)
        self.assertEqual([row[0] for row in listing], ['2-'])

        listing = broker.list_containers_iter(10, '2-0050', None, '2-', '-')
        self.assertEqual(len(listing), 10)
        self.assertEqual(listing[0][0], '2-0051')
        self.assertEqual(listing[1][0], '2-0051-')
        self.assertEqual(listing[2][0], '2-0052')
        self.assertEqual(listing[-1][0], '2-0059')

        listing = broker.list_containers_iter(10, '3-0045', None, '3-', '-')
        self.assertEqual(len(listing), 10)
        self.assertEqual([row[0] for row in listing],
                         ['3-0045-', '3-0046', '3-0046-', '3-0047',
                          '3-0047-', '3-0048', '3-0048-', '3-0049',
                          '3-0049-', '3-0050'])

        broker.put_container('3-0049-', normalize_timestamp(time()), 0, 0, 0,
                             POLICIES.default.idx)
        listing = broker.list_containers_iter(10, '3-0048', None, None, None)
        self.assertEqual(len(listing), 10)
        self.assertEqual([row[0] for row in listing],
                         ['3-0048-0049', '3-0049', '3-0049-', '3-0049-0049',
                          '3-0050', '3-0050-0049', '3-0051', '3-0051-0049',
                          '3-0052', '3-0052-0049'])

        listing = broker.list_containers_iter(10, '3-0048', None, '3-', '-')
        self.assertEqual(len(listing), 10)
        self.assertEqual([row[0] for row in listing],
                         ['3-0048-', '3-0049', '3-0049-', '3-0050',
                          '3-0050-', '3-0051', '3-0051-', '3-0052',
                          '3-0052-', '3-0053'])

        listing = broker.list_containers_iter(10, None, None, '3-0049-', '-')
        self.assertEqual(len(listing), 2)
        self.assertEqual([row[0] for row in listing],
                         ['3-0049-', '3-0049-0049'])

    def test_double_check_trailing_delimiter(self):
        # Test AccountBroker.list_containers_iter for an
        # account that has an odd container with a trailing delimiter
        broker = AccountBroker(':memory:', account='a')
        broker.initialize(normalize_timestamp('1'))
        broker.put_container('a', normalize_timestamp(time()), 0, 0, 0,
                             POLICIES.default.idx)
        broker.put_container('a-', normalize_timestamp(time()), 0, 0, 0,
                             POLICIES.default.idx)
        broker.put_container('a-a', normalize_timestamp(time()), 0, 0, 0,
                             POLICIES.default.idx)
        broker.put_container('a-a-a', normalize_timestamp(time()), 0, 0, 0,
                             POLICIES.default.idx)
        broker.put_container('a-a-b', normalize_timestamp(time()), 0, 0, 0,
                             POLICIES.default.idx)
        broker.put_container('a-b', normalize_timestamp(time()), 0, 0, 0,
                             POLICIES.default.idx)
        broker.put_container('b', normalize_timestamp(time()), 0, 0, 0,
                             POLICIES.default.idx)
        broker.put_container('b-a', normalize_timestamp(time()), 0, 0, 0,
                             POLICIES.default.idx)
        broker.put_container('b-b', normalize_timestamp(time()), 0, 0, 0,
                             POLICIES.default.idx)
        broker.put_container('c', normalize_timestamp(time()), 0, 0, 0,
                             POLICIES.default.idx)
        listing = broker.list_containers_iter(15, None, None, None, None)
        self.assertEqual(len(listing), 10)
        self.assertEqual([row[0] for row in listing],
                         ['a', 'a-', 'a-a', 'a-a-a', 'a-a-b', 'a-b', 'b',
                          'b-a', 'b-b', 'c'])
        listing = broker.list_containers_iter(15, None, None, '', '-')
        self.assertEqual(len(listing), 5)
        self.assertEqual([row[0] for row in listing],
                         ['a', 'a-', 'b', 'b-', 'c'])
        listing = broker.list_containers_iter(15, None, None, 'a-', '-')
        self.assertEqual(len(listing), 4)
        self.assertEqual([row[0] for row in listing],
                         ['a-', 'a-a', 'a-a-', 'a-b'])
        listing = broker.list_containers_iter(15, None, None, 'b-', '-')
        self.assertEqual(len(listing), 2)
        self.assertEqual([row[0] for row in listing], ['b-a', 'b-b'])

    def test_chexor(self):
        broker = AccountBroker(':memory:', account='a')
        broker.initialize(normalize_timestamp('1'))
        broker.put_container('a', normalize_timestamp(1),
                             normalize_timestamp(0), 0, 0,
                             POLICIES.default.idx)
        broker.put_container('b', normalize_timestamp(2),
                             normalize_timestamp(0), 0, 0,
                             POLICIES.default.idx)
        hasha = hashlib.md5(
            '%s-%s' % ('a', '0000000001.00000-0000000000.00000-0-0')
        ).digest()
        hashb = hashlib.md5(
            '%s-%s' % ('b', '0000000002.00000-0000000000.00000-0-0')
        ).digest()
        hashc = \
            ''.join(('%02x' % (ord(a) ^ ord(b)) for a, b in zip(hasha, hashb)))
        self.assertEqual(broker.get_info()['hash'], hashc)
        broker.put_container('b', normalize_timestamp(3),
                             normalize_timestamp(0), 0, 0,
                             POLICIES.default.idx)
        hashb = hashlib.md5(
            '%s-%s' % ('b', '0000000003.00000-0000000000.00000-0-0')
        ).digest()
        hashc = \
            ''.join(('%02x' % (ord(a) ^ ord(b)) for a, b in zip(hasha, hashb)))
        self.assertEqual(broker.get_info()['hash'], hashc)

    def test_merge_items(self):
        broker1 = AccountBroker(':memory:', account='a')
        broker1.initialize(normalize_timestamp('1'))
        broker2 = AccountBroker(':memory:', account='a')
        broker2.initialize(normalize_timestamp('1'))
        broker1.put_container('a', normalize_timestamp(1), 0, 0, 0,
                              POLICIES.default.idx)
        broker1.put_container('b', normalize_timestamp(2), 0, 0, 0,
                              POLICIES.default.idx)
        id = broker1.get_info()['id']
        broker2.merge_items(broker1.get_items_since(
            broker2.get_sync(id), 1000), id)
        items = broker2.get_items_since(-1, 1000)
        self.assertEqual(len(items), 2)
        self.assertEqual(['a', 'b'], sorted([rec['name'] for rec in items]))
        broker1.put_container('c', normalize_timestamp(3), 0, 0, 0,
                              POLICIES.default.idx)
        broker2.merge_items(broker1.get_items_since(
            broker2.get_sync(id), 1000), id)
        items = broker2.get_items_since(-1, 1000)
        self.assertEqual(len(items), 3)
        self.assertEqual(['a', 'b', 'c'],
                         sorted([rec['name'] for rec in items]))

    def test_load_old_pending_puts(self):
        # pending puts from pre-storage-policy account brokers won't contain
        # the storage policy index
        tempdir = mkdtemp()
        broker_path = os.path.join(tempdir, 'test-load-old.db')
        try:
            broker = AccountBroker(broker_path, account='real')
            broker.initialize(normalize_timestamp(1))
            with open(broker_path + '.pending', 'a+b') as pending:
                pending.write(':')
                pending.write(pickle.dumps(
                    # name, put_timestamp, delete_timestamp, object_count,
                    # bytes_used, deleted
                    ('oldcon', normalize_timestamp(200),
                     normalize_timestamp(0),
                     896, 9216695, 0)).encode('base64'))

            broker._commit_puts()
            with broker.get() as conn:
                results = list(conn.execute('''
                    SELECT name, storage_policy_index FROM container
                '''))
            self.assertEqual(len(results), 1)
            self.assertEqual(dict(results[0]),
                             {'name': 'oldcon', 'storage_policy_index': 0})
        finally:
            rmtree(tempdir)

    @patch_policies([StoragePolicy(0, 'zero', False),
                     StoragePolicy(1, 'one', True),
                     StoragePolicy(2, 'two', False),
                     StoragePolicy(3, 'three', False)])
    def test_get_policy_stats(self):
        ts = itertools.count()
        broker = AccountBroker(':memory:', account='a')
        broker.initialize(normalize_timestamp(ts.next()))
        # check empty policy_stats
        self.assertTrue(broker.empty())
        policy_stats = broker.get_policy_stats()
        self.assertEqual(policy_stats, {})

        # add some empty containers
        for policy in POLICIES:
            container_name = 'c-%s' % policy.name
            put_timestamp = normalize_timestamp(ts.next())
            broker.put_container(container_name,
                                 put_timestamp, 0,
                                 0, 0,
                                 policy.idx)

            policy_stats = broker.get_policy_stats()
            stats = policy_stats[policy.idx]
            self.assertEqual(stats['object_count'], 0)
            self.assertEqual(stats['bytes_used'], 0)

        # update the containers object & byte count
        for policy in POLICIES:
            container_name = 'c-%s' % policy.name
            put_timestamp = normalize_timestamp(ts.next())
            count = policy.idx * 100  # good as any integer
            broker.put_container(container_name,
                                 put_timestamp, 0,
                                 count, count,
                                 policy.idx)

            policy_stats = broker.get_policy_stats()
            stats = policy_stats[policy.idx]
            self.assertEqual(stats['object_count'], count)
            self.assertEqual(stats['bytes_used'], count)

        # check all the policy_stats at once
        for policy_index, stats in policy_stats.items():
            policy = POLICIES[policy_index]
            count = policy.idx * 100  # coupled with policy for test
            self.assertEqual(stats['object_count'], count)
            self.assertEqual(stats['bytes_used'], count)

        # now delete the containers one by one
        for policy in POLICIES:
            container_name = 'c-%s' % policy.name
            delete_timestamp = normalize_timestamp(ts.next())
            broker.put_container(container_name,
                                 0, delete_timestamp,
                                 0, 0,
                                 policy.idx)

            policy_stats = broker.get_policy_stats()
            stats = policy_stats[policy.idx]
            self.assertEqual(stats['object_count'], 0)
            self.assertEqual(stats['bytes_used'], 0)

    @patch_policies([StoragePolicy(0, 'zero', False),
                     StoragePolicy(1, 'one', True)])
    def test_policy_stats_tracking(self):
        ts = itertools.count()
        broker = AccountBroker(':memory:', account='a')
        broker.initialize(normalize_timestamp(ts.next()))

        # policy 0
        broker.put_container('con1', ts.next(), 0, 12, 2798641, 0)
        broker.put_container('con1', ts.next(), 0, 13, 8156441, 0)
        # policy 1
        broker.put_container('con2', ts.next(), 0, 7, 5751991, 1)
        broker.put_container('con2', ts.next(), 0, 8, 6085379, 1)

        stats = broker.get_policy_stats()
        self.assertEqual(len(stats), 2)
        self.assertEqual(stats[0]['object_count'], 13)
        self.assertEqual(stats[0]['bytes_used'], 8156441)
        self.assertEqual(stats[1]['object_count'], 8)
        self.assertEqual(stats[1]['bytes_used'], 6085379)

        # Break encapsulation here to make sure that there's only 2 rows in
        # the stats table. It's possible that there could be 4 rows (one per
        # put_container) but that they came out in the right order so that
        # get_policy_stats() collapsed them down to the right number. To prove
        # that's not so, we have to go peek at the broker's internals.
        with broker.get() as conn:
            nrows = conn.execute(
                "SELECT COUNT(*) FROM policy_stat").fetchall()[0][0]
        self.assertEqual(nrows, 2)


def prespi_AccountBroker_initialize(self, conn, put_timestamp):
    """
    The AccountBroker initialze() function before we added the
    policy stat table.  Used by test_policy_table_creation() to
    make sure that the AccountBroker will correctly add the table
    for cases where the DB existed before the policy suport was added.

    :param conn: DB connection object
    :param put_timestamp: put timestamp
    """
    if not self.account:
        raise ValueError(
            'Attempting to create a new database with no account set')
    self.create_container_table(conn)
    self.create_account_stat_table(conn, put_timestamp)


def premetadata_create_account_stat_table(self, conn, put_timestamp):
    """
    Copied from AccountBroker before the metadata column was
    added; used for testing with TestAccountBrokerBeforeMetadata.

    Create account_stat table which is specific to the account DB.

    :param conn: DB connection object
    :param put_timestamp: put timestamp
    """
    conn.executescript('''
        CREATE TABLE account_stat (
            account TEXT,
            created_at TEXT,
            put_timestamp TEXT DEFAULT '0',
            delete_timestamp TEXT DEFAULT '0',
            container_count INTEGER,
            object_count INTEGER DEFAULT 0,
            bytes_used INTEGER DEFAULT 0,
            hash TEXT default '00000000000000000000000000000000',
            id TEXT,
            status TEXT DEFAULT '',
            status_changed_at TEXT DEFAULT '0'
        );

        INSERT INTO account_stat (container_count) VALUES (0);
    ''')

    conn.execute('''
        UPDATE account_stat SET account = ?, created_at = ?, id = ?,
               put_timestamp = ?
        ''', (self.account, normalize_timestamp(time()), str(uuid4()),
              put_timestamp))


class TestAccountBrokerBeforeMetadata(TestAccountBroker):
    """
    Tests for AccountBroker against databases created before
    the metadata column was added.
    """

    def setUp(self):
        self._imported_create_account_stat_table = \
            AccountBroker.create_account_stat_table
        AccountBroker.create_account_stat_table = \
            premetadata_create_account_stat_table
        broker = AccountBroker(':memory:', account='a')
        broker.initialize(normalize_timestamp('1'))
        exc = None
        with broker.get() as conn:
            try:
                conn.execute('SELECT metadata FROM account_stat')
            except BaseException as err:
                exc = err
        self.assert_('no such column: metadata' in str(exc))

    def tearDown(self):
        AccountBroker.create_account_stat_table = \
            self._imported_create_account_stat_table
        broker = AccountBroker(':memory:', account='a')
        broker.initialize(normalize_timestamp('1'))
        with broker.get() as conn:
            conn.execute('SELECT metadata FROM account_stat')


def prespi_create_container_table(self, conn):
    """
    Copied from AccountBroker before the sstoage_policy_index column was
    added; used for testing with TestAccountBrokerBeforeSPI.

    Create container table which is specific to the account DB.

    :param conn: DB connection object
    """
    conn.executescript("""
        CREATE TABLE container (
            ROWID INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            put_timestamp TEXT,
            delete_timestamp TEXT,
            object_count INTEGER,
            bytes_used INTEGER,
            deleted INTEGER DEFAULT 0
        );

        CREATE INDEX ix_container_deleted_name ON
            container (deleted, name);

        CREATE TRIGGER container_insert AFTER INSERT ON container
        BEGIN
            UPDATE account_stat
            SET container_count = container_count + (1 - new.deleted),
                object_count = object_count + new.object_count,
                bytes_used = bytes_used + new.bytes_used,
                hash = chexor(hash, new.name,
                              new.put_timestamp || '-' ||
                                new.delete_timestamp || '-' ||
                                new.object_count || '-' || new.bytes_used);
        END;

        CREATE TRIGGER container_update BEFORE UPDATE ON container
        BEGIN
            SELECT RAISE(FAIL, 'UPDATE not allowed; DELETE and INSERT');
        END;


        CREATE TRIGGER container_delete AFTER DELETE ON container
        BEGIN
            UPDATE account_stat
            SET container_count = container_count - (1 - old.deleted),
                object_count = object_count - old.object_count,
                bytes_used = bytes_used - old.bytes_used,
                hash = chexor(hash, old.name,
                              old.put_timestamp || '-' ||
                                old.delete_timestamp || '-' ||
                                old.object_count || '-' || old.bytes_used);
        END;
    """)


def with_tempdir(f):
    @functools.wraps(f)
    def wrapped(*args, **kwargs):
        tempdir = mkdtemp()
        args = list(args)
        args.append(tempdir)
        try:
            return f(*args, **kwargs)
        finally:
            rmtree(tempdir)
    return wrapped


class TestAccountBrokerBeforeSPI(TestAccountBroker):
    """
    Tests for AccountBroker against databases created before
    the storage_policy_index column was added.
    """

    def setUp(self):
        self._imported_create_container_table = \
            AccountBroker.create_container_table
        AccountBroker.create_container_table = \
            prespi_create_container_table
        self._imported_initialize = AccountBroker._initialize
        AccountBroker._initialize = prespi_AccountBroker_initialize
        broker = AccountBroker(':memory:', account='a')
        broker.initialize(normalize_timestamp('1'))
        exc = None
        with broker.get() as conn:
            try:
                conn.execute('SELECT storage_policy_index FROM container')
            except BaseException as err:
                exc = err
        self.assert_('no such column: storage_policy_index' in str(exc))
        with broker.get() as conn:
            try:
                conn.execute('SELECT * FROM policy_stat')
            except sqlite3.OperationalError as err:
                self.assert_('no such table: policy_stat' in str(err))
            else:
                self.fail('database created with policy_stat table')

    def tearDown(self):
        AccountBroker.create_container_table = \
            self._imported_create_container_table
        AccountBroker._initialize = self._imported_initialize
        broker = AccountBroker(':memory:', account='a')
        broker.initialize(normalize_timestamp('1'))
        with broker.get() as conn:
            conn.execute('SELECT storage_policy_index FROM container')

    @with_tempdir
    def test_policy_table_migration(self, tempdir):
        db_path = os.path.join(tempdir, 'account.db')

        # first init an acct DB without the policy_stat table present
        broker = AccountBroker(db_path, account='a')
        broker.initialize(normalize_timestamp('1'))
        with broker.get() as conn:
            try:
                conn.execute('''
                    SELECT * FROM policy_stat
                    ''').fetchone()[0]
            except sqlite3.OperationalError as err:
                # confirm that the table really isn't there
                self.assert_('no such table: policy_stat' in str(err))
            else:
                self.fail('broker did not raise sqlite3.OperationalError '
                          'trying to select from policy_stat table!')

        # make sure we can HEAD this thing w/o the table
        broker.get_policy_stats()

        # now do a PUT to create the table
        broker.put_container('o', normalize_timestamp(time()), 0, 0, 0,
                             POLICIES.default.idx)
        broker._commit_puts_stale_ok()

        # now confirm that the table was created
        with broker.get() as conn:
            conn.execute('SELECT * FROM policy_stat')

    @with_tempdir
    def test_half_upgraded_database(self, tempdir):
        db_path = os.path.join(tempdir, 'account.db')
        ts = itertools.count()

        broker = AccountBroker(db_path, account='a')
        broker.initialize(normalize_timestamp(ts.next()))

        self.assertTrue(broker.empty())

        # add a container (to pending file)
        broker.put_container('c', normalize_timestamp(ts.next()), 0, 0, 0,
                             POLICIES.default.idx)

        real_get = broker.get
        called = []

        @contextmanager
        def mock_get():
            with real_get() as conn:

                def mock_executescript(script):
                    if called:
                        raise Exception('kaboom!')
                    called.append(script)

                conn.executescript = mock_executescript
                yield conn

        broker.get = mock_get

        try:
            broker._commit_puts()
        except Exception:
            pass
        else:
            self.fail('mock exception was not raised')

        self.assertEqual(len(called), 1)
        self.assert_('CREATE TABLE policy_stat' in called[0])

        # nothing was commited
        broker = AccountBroker(db_path, account='a')
        with broker.get() as conn:
            try:
                conn.execute('SELECT * FROM policy_stat')
            except sqlite3.OperationalError as err:
                self.assert_('no such table: policy_stat' in str(err))
            else:
                self.fail('half upgraded database!')
            container_count = conn.execute(
                'SELECT count(*) FROM container').fetchone()[0]
            self.assertEqual(container_count, 0)

        # try again to commit puts
        self.assertFalse(broker.empty())

        # full migration successful
        with broker.get() as conn:
            conn.execute('SELECT * FROM policy_stat')
            conn.execute('SELECT storage_policy_index FROM container')
