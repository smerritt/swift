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

import mock
import unittest

from swift.common.swob import Request
from swift.proxy import server as proxy_server
from swift.proxy.controllers.base import headers_to_container_info
from test.unit import fake_http_connect, FakeRing, FakeMemcache
from swift.common.utils import StoragePolicy, StoragePolicyCollection

class TestContainerController(unittest.TestCase):
    def setUp(self):
        policy = [StoragePolicy('0', '', '', 'yes', FakeRing())]
        policy_coll = StoragePolicyCollection(policy)
        self.app = proxy_server.Application(None, FakeMemcache(),
                                            account_ring=FakeRing(),
                                            container_ring=FakeRing(),
                                            stor_policies=policy_coll)

    def test_container_info_in_response_env(self):
        controller = proxy_server.ContainerController(self.app, 'a', 'c')
        with mock.patch('swift.proxy.controllers.base.http_connect',
                        fake_http_connect(200, 200, body='')):
            req = Request.blank('/a/c', {'PATH_INFO': '/a/c'})
            resp = controller.HEAD(req)
        self.assertEqual(2, resp.status_int // 100)
        self.assertTrue("swift.container/a/c" in resp.environ)
        self.assertEqual(headers_to_container_info(resp.headers),
                         resp.environ['swift.container/a/c'])


if __name__ == '__main__':
    unittest.main()
