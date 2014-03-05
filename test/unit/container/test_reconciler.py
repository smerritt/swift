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

import json
import mock
import operator
import time
import unittest
import urllib

from collections import defaultdict
from datetime import datetime
from swift.container import reconciler
from swift.common.direct_client import ClientException
from swift.common import swob

from test.unit import FakeLogger, FakeRing
from test.unit.common.middleware.helpers import FakeSwift


def timestamp_to_last_modified(timestamp):
    return datetime.fromtimestamp(timestamp).strftime('%Y-%m-%dT%H:%M:%S.%f')


class FakeInternalClient(reconciler.InternalClient):
    def __init__(self, listings):
        self.app = FakeSwift()
        self.user_agent = 'fake-internal-client'
        self.request_tries = 1
        self.parse(listings)

    def parse(self, listings):
        self.accounts = defaultdict(lambda: defaultdict(list))
        for item, timestamp in listings.items():
            account, container_name, obj_name = item.lstrip('/').split('/', 2)
            self.accounts[account][container_name].append(
                (obj_name, timestamp))
        for account_name, containers in self.accounts.items():
            for con in containers:
                self.accounts[account_name][con].sort(key=lambda t: t[0])
        for account, containers in self.accounts.items():
            account_listing_data = []
            account_path = '/v1/%s' % account
            for container, objects in containers.items():
                container_path = account_path + '/' + container
                container_listing_data = []
                for obj_name, timestamp in objects:
                    obj_path = container_path + '/' + obj_name
                    headers = {'X-Timestamp': timestamp}
                    # register object response
                    self.app.register('GET', obj_path, swob.HTTPOk, headers)
                    self.app.register('DELETE', obj_path,
                                      swob.HTTPNoContent, {})
                    # container listing entry
                    obj_data = {
                        'hash': '%s-etag' % obj_path,
                        'bytes': 0,
                        'name': obj_name,
                        'last_modified': timestamp_to_last_modified(timestamp),
                    }
                    container_listing_data.append(obj_data)
                container_listing_data.sort(key=operator.itemgetter('name'))
                # register container listing response
                container_headers = {}
                container_qry_string = '?format=json&marker=&end_marker='
                self.app.register('GET', container_path + container_qry_string,
                                  swob.HTTPOk, container_headers,
                                  json.dumps(container_listing_data))
                end_qry_string = '?format=json&marker=%s&end_marker=' % (
                    urllib.quote(container_listing_data[-1]['name']))
                self.app.register('GET', container_path + end_qry_string,
                                  swob.HTTPOk, container_headers,
                                  json.dumps([]))
                self.app.register('DELETE', container_path,
                                  swob.HTTPConflict, {}, '')
                # simple account listing entry
                container_data = {'name': container}
                account_listing_data.append(container_data)
            # register account response
            account_headers = {}
            account_qry_string = '?format=json&marker=&end_marker='
            self.app.register('GET', account_path + account_qry_string,
                              swob.HTTPOk, account_headers,
                              json.dumps(account_listing_data))
            end_qry_string = '?format=json&marker=%s&end_marker=' % (
                urllib.quote(account_listing_data[-1]['name']))
            self.app.register('GET', account_path + end_qry_string,
                              swob.HTTPOk, account_headers,
                              json.dumps([]))


class TestReconcilerUtils(unittest.TestCase):

    def setUp(self):
        self.fake_ring = FakeRing()

    def test_parse_raw_obj(self):
        got = reconciler.parse_raw_obj(
            {'name': "2:/AUTH_bob/con/obj",
             'last_modified': timestamp_to_last_modified(2017551.493500)})
        self.assertEqual(got['real_storage_policy_index'], 2)
        self.assertEqual(got['account'], 'AUTH_bob')
        self.assertEqual(got['container'], 'con')
        self.assertEqual(got['obj'], 'obj')
        self.assertEqual(got['q_timestamp'], 2017551.493500)

        got = reconciler.parse_raw_obj(
            {'name': "2:/AUTH_bob/con/obj",
             'last_modified': timestamp_to_last_modified(1234.20190)})
        self.assertEqual(got['real_storage_policy_index'], 2)
        self.assertEqual(got['account'], 'AUTH_bob')
        self.assertEqual(got['container'], 'con')
        self.assertEqual(got['obj'], 'obj')
        self.assertEqual(got['q_timestamp'], 1234.20190)

    def test_get_oldest_storage_policy_index(self):
        mock_path = 'swift.container.reconciler.direct_head_container'
        stub_resp_headers = [
            {
                'x-timestamp': '1393542492.31822',
                'x-storage-policy-index': '0',
            },
            {
                'x-timestamp': '1393542493.75106',
                'x-storage-policy-index': '1',
            },
            {
                'x-timestamp': '1393542492.31822',
                'x-storage-policy-index': '0',
            },
        ]
        with mock.patch(mock_path) as direct_head:
            direct_head.side_effect = stub_resp_headers
            oldest_spi = reconciler.direct_get_oldest_storage_policy_index(
                self.fake_ring, 'a', 'con')
        self.assertEqual(oldest_spi, 0)

    def test_get_oldest_storage_policy_index_with_error(self):
        mock_path = 'swift.container.reconciler.direct_head_container'
        stub_resp_headers = [
            {
                'x-timestamp': '1393542492.31822',
                'x-storage-policy-index': '1',
            },
            {
                'x-timestamp': '1393542499.31822',
                'x-storage-policy-index': '0',
            },
            ClientException(
                'Container Server blew up',
                '10.0.0.1', 6001, 'sdb', 404, 'Not Found'
            ),
        ]
        with mock.patch(mock_path) as direct_head:
            direct_head.side_effect = stub_resp_headers
            oldest_spi = reconciler.direct_get_oldest_storage_policy_index(
                self.fake_ring, 'a', 'con')
        self.assertEqual(oldest_spi, 1)

    def test_get_oldest_storage_policy_index_with_too_many_errors(self):
        mock_path = 'swift.container.reconciler.direct_head_container'
        stub_resp_headers = [
            {
                'x-timestamp': '1393542492.31822',
                'x-storage-policy-index': '0',
            },
            ClientException(
                'Container Server blew up',
                '10.0.0.1', 6001, 'sdb', 404, 'Not Found'
            ),
            ClientException(
                'Container Server blew up',
                '10.0.0.12', 6001, 'sdj', 404, 'Not Found'
            ),
        ]
        with mock.patch(mock_path) as direct_head:
            direct_head.side_effect = stub_resp_headers
            oldest_spi = reconciler.direct_get_oldest_storage_policy_index(
                self.fake_ring, 'a', 'con')
        self.assertEqual(oldest_spi, None)


def listing_qs(marker):
    return "?format=json&marker=%s&end_marker=" % urllib.quote(marker)


class TestReconciler(unittest.TestCase):

    def setUp(self):
        self.logger = FakeLogger()
        conf = {}
        with mock.patch('swift.container.reconciler.InternalClient'):
            self.reconciler = reconciler.ContainerReconciler(conf)
        self.reconciler.logger = self.logger
        self._mock_oldest_spi_map = {}

    def _mock_listing(self, objects):
        self.reconciler.swift = FakeInternalClient(objects)
        self.fake_swift = self.reconciler.swift.app

    def _mock_oldest_spi(self, container_oldest_spi_map):
        self._mock_oldest_spi_map = container_oldest_spi_map

    def _run_once(self):
        """
        Helper method to run the reconciler once with appropriate direct-client
        mocks in place.

        Returns the list of direct-deleted container entries in the format
        [(acc1, con1, obj1), ...]
        """

        def mock_oldest_spi(ring, account, container_name):
            return self._mock_oldest_spi_map.get(container_name, 0)

        delete_listings_mock = mock.patch.object(
            reconciler, 'direct_delete_container_entry')

        with mock.patch.object(reconciler,
                               'direct_get_oldest_storage_policy_index',
                               new=mock_oldest_spi):
            with delete_listings_mock as dlm:
                self.reconciler.run_once()

        return [c[1][1:4] for c in dlm.mock_calls]

    def test_object_move(self):
        self._mock_listing({
            "/.misplaced_objects/3600/1:/AUTH_bob/c/o1": 3618.841878,
        })
        dest_response = (swob.HTTPNotFound, {}, '')
        headers = {'X-Timestamp': '3618.841878'}
        real_response = (swob.HTTPOk, headers, '')
        responses = [dest_response, real_response]
        self.fake_swift.register_responses(
            'GET', '/v1/AUTH_bob/c/o1', responses)
        self.fake_swift.register(
            'PUT', '/v1/AUTH_bob/c/o1', swob.HTTPCreated, {}, '')
        self.fake_swift.register(
            'DELETE', '/v1/AUTH_bob/c/o1', swob.HTTPNoContent, {}, '')
        self._mock_oldest_spi({'c': 0})
        deleted_container_entries = self._run_once()
        self.assertEqual(self.reconciler.stats['misplaced_objects'], 1)
        self.assertEqual(self.reconciler.stats['unhandled_errors'], 0)

        self.maxDiff = None
        self.assertEqual(
            self.fake_swift.calls,
            [('GET', '/v1/.misplaced_objects' + listing_qs('')),
             ('GET', '/v1/.misplaced_objects/3600' + listing_qs('')),
             ('HEAD', '/v1/AUTH_bob/c/o1'),
             ('GET', '/v1/AUTH_bob/c/o1'),
             ('PUT', '/v1/AUTH_bob/c/o1'),
             ('DELETE', '/v1/AUTH_bob/c/o1'),
             ('GET', '/v1/.misplaced_objects/3600' +
              listing_qs('1:/AUTH_bob/c/o1')),
             ('DELETE', '/v1/.misplaced_objects/3600'),
             ('GET', '/v1/.misplaced_objects' + listing_qs('3600'))])

        sent_headers = self.fake_swift.headers
        self.assertEqual(
            # the object *should be* in policy 0
            sent_headers[2].get('X-Override-Storage-Policy-Index'), '0')
        self.assertEqual(
            # but the object *is* in policy 1
            sent_headers[3].get('X-Override-Storage-Policy-Index'), '1')
        self.assertEqual(
            # so we PUT it into policy 0
            sent_headers[4].get('X-Override-Storage-Policy-Index'), '0')
        self.assertEqual(
            # and DELETE it from policy 1
            sent_headers[5].get('X-Override-Storage-Policy-Index'), '1')

        # we PUT the object in the right place with its original timestamp
        self.assertEqual(
            sent_headers[4].get('X-Timestamp'), '3618.841878')
        # we DELETE the object from the wrong place with a slightly newer
        # timestamp to make sure the change takes effect
        self.assertEqual(
            sent_headers[5].get('X-Timestamp'), '3618.841879')

        # and when we're done, we clean up the container listings
        self.assertEqual(deleted_container_entries,
                         [('.misplaced_objects', '3600', '1:/AUTH_bob/c/o1')])

    def test_object_move_already_in_right_place(self):
        self._mock_listing({
            "/.misplaced_objects/3600/1:/AUTH_bob/c/o1": 3618.841878,
        })
        self._mock_oldest_spi({'c': 1})
        deleted_container_entries = self._run_once()
        self.assertEqual(self.reconciler.stats['correct_objects'], 1)

        self.maxDiff = None
        self.assertEqual(
            self.fake_swift.calls,
            [('GET', '/v1/.misplaced_objects' + listing_qs('')),
             ('GET', '/v1/.misplaced_objects/3600' + listing_qs('')),
             ('GET', '/v1/.misplaced_objects/3600' +
              listing_qs('1:/AUTH_bob/c/o1')),
             ('DELETE', '/v1/.misplaced_objects/3600'),
             ('GET', '/v1/.misplaced_objects' + listing_qs('3600'))])

        self.assertEqual(deleted_container_entries,
                         [('.misplaced_objects', '3600', '1:/AUTH_bob/c/o1')])

    def test_object_move_dest_object_newer_than_queue_entry(self):
        self._mock_listing({
            "/.misplaced_objects/3600/1:/AUTH_bob/c/o1": 3679.2019,
        })
        self.fake_swift.register(
            'GET', '/v1/AUTH_bob/c/o1', swob.HTTPOk,
            {'X-Timestamp': '4000.365353'})  # newer than queue entry
        self.fake_swift.register(
            'DELETE', '/v1/AUTH_bob/c/o1', swob.HTTPNoContent, {})
        self._mock_oldest_spi({'c': 0})
        deleted_container_entries = self._run_once()
        self.assertEqual(self.reconciler.stats['newer_objects'], 1)

        self.maxDiff = None
        self.assertEqual(
            self.fake_swift.calls,
            [('GET', '/v1/.misplaced_objects' + listing_qs('')),
             ('GET', '/v1/.misplaced_objects/3600' + listing_qs('')),
             ('HEAD', '/v1/AUTH_bob/c/o1'),
             ('DELETE', '/v1/AUTH_bob/c/o1'),
             ('GET', '/v1/.misplaced_objects/3600' +
              listing_qs('1:/AUTH_bob/c/o1')),
             ('DELETE', '/v1/.misplaced_objects/3600'),
             ('GET', '/v1/.misplaced_objects' + listing_qs('3600'))])

        sent_headers = self.fake_swift.headers
        self.assertEqual(
            # the object is in policy 0 with a newer timestamp
            sent_headers[2].get('X-Override-Storage-Policy-Index'), '0')
        self.assertEqual(
            # so we just make sure it's deleted from policy 1
            sent_headers[3].get('X-Override-Storage-Policy-Index'), '1')

        # the tombstones have the oldest possible timestamp that will actually
        # cause object cleanup, just in case there's a newer object hanging
        # out on handoffs somewhere
        self.assertEqual(
            sent_headers[3].get('X-Timestamp'), '3679.201901')

        # we cleaned up the old object, so this counts as done
        self.assertEqual(deleted_container_entries,
                         [('.misplaced_objects', '3600', '1:/AUTH_bob/c/o1')])

    def test_object_move_src_object_newer_than_queue_entry(self):
        self._mock_listing({
            "/.misplaced_objects/3600/1:/AUTH_bob/c/o1": 3600.123456,
        })
        # the destination object doesn't exist
        self.fake_swift.register(
            'HEAD', '/v1/AUTH_bob/c/o1', swob.HTTPNotFound, {})
        # the source object exists and is newer than the queue entry
        self.fake_swift.register(
            'GET', '/v1/AUTH_bob/c/o1', swob.HTTPNotFound,
            {'X-Timestamp': '3600.234567'})
        self._mock_oldest_spi({'c': 0})
        deleted_container_entries = self._run_once()
        self.assertEqual(self.reconciler.stats['source_newer'], 1)

        self.maxDiff = None
        self.assertEqual(
            self.fake_swift.calls,
            [('GET', '/v1/.misplaced_objects' + listing_qs('')),
             ('GET', '/v1/.misplaced_objects/3600' + listing_qs('')),
             ('HEAD', '/v1/AUTH_bob/c/o1'),
             ('GET', '/v1/AUTH_bob/c/o1'),
             ('GET', '/v1/.misplaced_objects/3600' +
              listing_qs('1:/AUTH_bob/c/o1')),
             ('DELETE', '/v1/.misplaced_objects/3600'),
             ('GET', '/v1/.misplaced_objects' + listing_qs('3600'))])

        sent_headers = self.fake_swift.headers
        self.assertEqual(
            sent_headers[2].get('X-Override-Storage-Policy-Index'), '0')
        self.assertEqual(
            sent_headers[3].get('X-Override-Storage-Policy-Index'), '1')

        self.assertEqual(deleted_container_entries,
                         [('.misplaced_objects', '3600', '1:/AUTH_bob/c/o1')])

    def test_object_move_dest_object_older_than_queue_entry(self):
        self._mock_listing({
            "/.misplaced_objects/36000/1:/AUTH_bob/c/o1": 36123.383925,
        })
        dest_response = (swob.HTTPNotFound, {}, '')
        headers = {'X-Timestamp': '36121.5'}  # slightly older
        real_response = (swob.HTTPOk, headers, '')
        responses = [dest_response, real_response]
        self.fake_swift.register_responses(
            'GET', '/v1/AUTH_bob/c/o1', responses)
        self.fake_swift.register(
            'PUT', '/v1/AUTH_bob/c/o1', swob.HTTPCreated, {}, '')
        self.fake_swift.register(
            'DELETE', '/v1/AUTH_bob/c/o1', swob.HTTPNoContent, {}, '')
        self._mock_oldest_spi({'c': 0})
        deleted_container_entries = self._run_once()

        self.maxDiff = None
        self.assertEqual(
            self.fake_swift.calls,
            [('GET', '/v1/.misplaced_objects' + listing_qs('')),
             ('GET', '/v1/.misplaced_objects/36000' + listing_qs('')),
             ('HEAD', '/v1/AUTH_bob/c/o1'),
             ('GET', '/v1/AUTH_bob/c/o1'),
             ('PUT', '/v1/AUTH_bob/c/o1'),
             ('DELETE', '/v1/AUTH_bob/c/o1'),
             ('GET', '/v1/.misplaced_objects/36000' +
              listing_qs('1:/AUTH_bob/c/o1')),
             ('DELETE', '/v1/.misplaced_objects/36000'),
             ('GET', '/v1/.misplaced_objects' + listing_qs('36000'))])

        sent_headers = self.fake_swift.headers
        self.assertEqual(
            # the object *should be* in policy 0
            sent_headers[2].get('X-Override-Storage-Policy-Index'), '0')
        self.assertEqual(
            # but the object *is* in policy 1
            sent_headers[3].get('X-Override-Storage-Policy-Index'), '1')
        self.assertEqual(
            # so we PUT it into policy 0
            sent_headers[4].get('X-Override-Storage-Policy-Index'), '0')
        self.assertEqual(
            # and DELETE it from policy 1
            sent_headers[5].get('X-Override-Storage-Policy-Index'), '1')

        # the PUT to the right policy keeps the original object's timestamp
        self.assertEqual(
            sent_headers[4].get('X-Timestamp'), '36121.5')
        # the DELETE to the wrong policy has to be slightly newer or else
        # it'll end up as a noop
        self.assertEqual(
            sent_headers[5].get('X-Timestamp'), '36121.500001')

        self.assertEqual(deleted_container_entries,
                         [('.misplaced_objects', '36000', '1:/AUTH_bob/c/o1')])

    def test_object_move_put_fails(self):
        # XXX why is this test so slow? it's like 2 seconds in here
        self._mock_listing({
            "/.misplaced_objects/36000/1:/AUTH_bob/c/o1": 36123.383925,
        })
        dest_response = (swob.HTTPNotFound, {}, '')
        headers = {'X-Timestamp': '36121.5'}  # slightly older
        real_response = (swob.HTTPOk, headers, '')
        responses = [dest_response, real_response]
        self.fake_swift.register_responses(
            'GET', '/v1/AUTH_bob/c/o1', responses)
        self.fake_swift.register(
            'PUT', '/v1/AUTH_bob/c/o1', swob.HTTPNotFound, {}, '')
        self.fake_swift.register(
            'DELETE', '/v1/AUTH_bob/c/o1', swob.HTTPNoContent, {}, '')
        self._mock_oldest_spi({'c': 0})
        deleted_container_entries = self._run_once()

        self.maxDiff = None
        self.assertEqual(
            self.fake_swift.calls,
            [('GET', '/v1/.misplaced_objects' + listing_qs('')),
             ('GET', '/v1/.misplaced_objects/36000' + listing_qs('')),
             ('HEAD', '/v1/AUTH_bob/c/o1'),
             ('GET', '/v1/AUTH_bob/c/o1'),
             ('PUT', '/v1/AUTH_bob/c/o1'),
             ('GET', '/v1/.misplaced_objects/36000' +
              listing_qs('1:/AUTH_bob/c/o1')),
             ('DELETE', '/v1/.misplaced_objects/36000'),
             ('GET', '/v1/.misplaced_objects' + listing_qs('36000'))])

        self.assertEqual(deleted_container_entries, [])
        self.assertEqual(self.reconciler.stats['unhandled_errors'], 0)

    def test_object_move_no_such_object_no_tombstone_recent(self):
        queue_ts = time.time()
        container = str(queue_ts // 3600 * 3600)

        self._mock_listing({
            "/.misplaced_objects/%s/1:/AUTH_jeb/c/o1" % container: queue_ts
        })

        # the destination object doesn't exist at all (no tombstones or
        # anything)
        self.fake_swift.register(
            'HEAD', '/v1/AUTH_jeb/c/o1', swob.HTTPNotFound, {})
        # the source object doesn't either
        self.fake_swift.register(
            'GET', '/v1/AUTH_jeb/c/o1', swob.HTTPNotFound, {})

        deleted_container_entries = self._run_once()

        self.maxDiff = None
        self.assertEqual(
            self.fake_swift.calls,
            [('GET', '/v1/.misplaced_objects' + listing_qs('')),
             ('GET', '/v1/.misplaced_objects/%s' % container + listing_qs('')),
             ('HEAD', '/v1/AUTH_jeb/c/o1'),
             ('GET', '/v1/AUTH_jeb/c/o1'),
             ('GET', '/v1/.misplaced_objects/%s' % container +
              listing_qs('1:/AUTH_jeb/c/o1')),
             ('DELETE', '/v1/.misplaced_objects/%s' % container),
             ('GET', '/v1/.misplaced_objects' + listing_qs(container))])

        # the queue entry is recent enough that there could easily be
        # tombstones on offline nodes or something, so we'll just leave it
        # here and try again later
        self.assertEqual(deleted_container_entries, [])

    def test_object_move_no_such_object_no_tombstone_ancient(self):
        queue_ts = time.time() - self.reconciler.reclaim_age * 1.1
        container = str(queue_ts // 3600 * 3600)

        self._mock_listing({
            "/.misplaced_objects/%s/1:/AUTH_jeb/c/o1" % container: queue_ts
        })

        # the destination object doesn't exist at all (no tombstones or
        # anything)
        self.fake_swift.register(
            'HEAD', '/v1/AUTH_jeb/c/o1', swob.HTTPNotFound, {})
        # the source object doesn't either
        self.fake_swift.register(
            'GET', '/v1/AUTH_jeb/c/o1', swob.HTTPNotFound, {})

        deleted_container_entries = self._run_once()

        self.maxDiff = None
        self.assertEqual(
            self.fake_swift.calls,
            [('GET', '/v1/.misplaced_objects' + listing_qs('')),
             ('GET', '/v1/.misplaced_objects/%s' % container + listing_qs('')),
             ('HEAD', '/v1/AUTH_jeb/c/o1'),
             ('GET', '/v1/AUTH_jeb/c/o1'),
             ('GET', '/v1/.misplaced_objects/%s' % container +
              listing_qs('1:/AUTH_jeb/c/o1')),
             ('DELETE', '/v1/.misplaced_objects/%s' % container),
             ('GET', '/v1/.misplaced_objects' + listing_qs(container))])

        # the queue entry is old enough that the tombstones, if any, have
        # probably been reaped, so we'll just give up
        self.assertEqual(
            deleted_container_entries,
            [('.misplaced_objects', container, '1:/AUTH_jeb/c/o1')])


if __name__ == '__main__':
    unittest.main()
