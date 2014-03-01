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
import unittest
import mock
import operator
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

    """
    def iter_containers(self, account_name):
        return ({'name': c} for c in sorted(self.accounts[account_name]))

    def iter_objects(self, account_name, container_name):
        for obj, timestamp in self.accounts[account_name][container_name]:
            yield {
                'name': obj,
                'timestamp': timestamp,
            }

    def get_object_metadata(self, account, container, obj, headers=None,
                            acceptable_statuses=(2,)):
        for item in self.iter_objects(account, container):
            if item['name'] == obj:
                return item
        raise Exception("couldn't find /%s/%s/%s" % (account, container, obj))

    def get_object(self, account, container, obj, headers=None,
                   acceptable_statuses=(2,)):
        pass

    def upload_object(self, fobj, account, container, obj, headers=None,
                      acceptable_statuses=(2,)):
        pass
    """


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
            oldest_spi = reconciler.get_oldest_storage_policy_index(
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
            oldest_spi = reconciler.get_oldest_storage_policy_index(
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
            oldest_spi = reconciler.get_oldest_storage_policy_index(
                self.fake_ring, 'a', 'con')
        self.assertEqual(oldest_spi, None)


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

    def _mock_oldest_spi(self, container_oldest_spi_map):
        self._mock_oldest_spi_map = container_oldest_spi_map

    def _run_once(self):
        def mock_oldest_spi(ring, account, container_name):
            return self._mock_oldest_spi_map.get(container_name, 0)
        with mock.patch.object(reconciler, 'get_oldest_storage_policy_index',
                               new=mock_oldest_spi):
            self.reconciler.run_once()

    def test_stats(self):
        self._mock_listing({
            "/.misplaced_objects/3600/1:/AUTH_bob/c/o1": 3618.841878,
            "/.misplaced_objects/3600/1:/AUTH_bob/c/o2": 3693.892568,
            "/.misplaced_objects/7200/2:/AUTH_jeb/con-a/obj-a": 7269.737582,
            "/AUTH_bob/c/o1": 3899.728469,
            "/AUTH_bob/c/o2": 3899.728469,
        })

        mock_spi = {
            'c': 0,
            'con-a': 2,
        }
        self._mock_oldest_spi(mock_spi)
        self._run_once()
        self.assertEqual(self.reconciler.stats['misplaced_objects'], 2)
        self.assertEqual(self.reconciler.stats['correct_objects'], 1)


if __name__ == '__main__':
    unittest.main()
