#-*- coding:utf-8 -*-
# Copyright (c) 2013 OpenStack Foundation
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

import contextlib
import hashlib
import json
import mock
import time
import unittest
from swift.common import exceptions, swob
from swift.common.middleware import dlo
from test.unit.common.middleware.helpers import FakeSwift

LIMIT = 'swift.common.middleware.dlo.CONTAINER_LISTING_LIMIT'


class DloTestCase(unittest.TestCase):
    def call_dlo(self, req, app=None, expect_exception=False):
        if app is None:
            app = self.dlo

        req.headers.setdefault("User-Agent", "Soap Opera")

        status = [None]
        headers = [None]

        def start_response(s, h, ei=None):
            status[0] = s
            headers[0] = h

        body_iter = app(req.environ, start_response)
        body = ''
        caught_exc = None
        try:
            for chunk in body_iter:
                body += chunk
        except Exception as exc:
            if expect_exception:
                caught_exc = exc
            else:
                raise

        if expect_exception:
            return status[0], headers[0], body, caught_exc
        else:
            return status[0], headers[0], body

    def setUp(self):
        self.app = FakeSwift()
        self.dlo = dlo.filter_factory({
            # don't slow down tests with rate limiting
            'rate_limit_after_segment': '1000000',
        })(self.app)

        self.app.register(
            'GET', '/v1/AUTH_test/c/seg_01',
            swob.HTTPOk, {'Content-Length': '5', 'Etag': 'seg01-etag'},
            'aaaaa')
        self.app.register(
            'GET', '/v1/AUTH_test/c/seg_02',
            swob.HTTPOk, {'Content-Length': '5', 'Etag': 'seg02-etag'},
            'bbbbb')
        self.app.register(
            'GET', '/v1/AUTH_test/c/seg_03',
            swob.HTTPOk, {'Content-Length': '5', 'Etag': 'seg03-etag'},
            'ccccc')
        self.app.register(
            'GET', '/v1/AUTH_test/c/seg_04',
            swob.HTTPOk, {'Content-Length': '5', 'Etag': 'seg04-etag'},
            'ddddd')
        self.app.register(
            'GET', '/v1/AUTH_test/c/seg_05',
            swob.HTTPOk, {'Content-Length': '5', 'Etag': 'seg05-etag'},
            'eeeee')

        # an unrelated object (not seg*) to test the prefix matching
        self.app.register(
            'GET', '/v1/AUTH_test/c/catpicture.jpg',
            swob.HTTPOk, {'Content-Length': '9', 'Etag': 'cats-etag'},
            'meow meow meow meow')

        self.app.register(
            'GET', '/v1/AUTH_test/mancon/manifest',
            swob.HTTPOk, {'Content-Length': '17', 'Etag': 'manifest-etag',
                          'X-Object-Manifest': 'c/seg'},
            'manifest-contents')

        lm = '2013-11-22T02:42:13.781760'
        ct = 'application/octet-stream'
        segs = [{"hash": "seg01-etag", "bytes": 5, "name": "seg_01",
                 "last_modified": lm, "content_type": ct},
                {"hash": "seg02-etag", "bytes": 5, "name": "seg_02",
                 "last_modified": lm, "content_type": ct},
                {"hash": "seg03-etag", "bytes": 5, "name": "seg_03",
                 "last_modified": lm, "content_type": ct},
                {"hash": "seg04-etag", "bytes": 5, "name": "seg_04",
                 "last_modified": lm, "content_type": ct},
                {"hash": "seg05-etag", "bytes": 5, "name": "seg_05",
                 "last_modified": lm, "content_type": ct}]

        full_container_listing = segs + [{"hash": "cats-etag", "bytes": 9,
                                          "name": "catpicture.jpg",
                                          "last_modified": lm,
                                          "content_type": "application/png"}]
        self.app.register(
            'GET', '/v1/AUTH_test/c?format=json',
            swob.HTTPOk, {'Content-Type': 'application/json; charset=utf-8'},
            json.dumps(full_container_listing))
        self.app.register(
            'GET', '/v1/AUTH_test/c?format=json&prefix=seg',
            swob.HTTPOk, {'Content-Type': 'application/json; charset=utf-8'},
            json.dumps(segs))

        # This is to let us test multi-page container listings; we use the
        # trailing underscore to send small (pagesize=3) listings.
        #
        # If you're testing against this, be sure to mock out
        # CONTAINER_LISTING_LIMIT to 3 in your test.
        self.app.register(
            'GET', '/v1/AUTH_test/mancon/manifest-many-segments',
            swob.HTTPOk, {'Content-Length': '7', 'Etag': 'etag-manyseg',
                          'X-Object-Manifest': 'c/seg_'},
            'manyseg')
        self.app.register(
            'GET', '/v1/AUTH_test/c?format=json&prefix=seg_',
            swob.HTTPOk, {'Content-Type': 'application/json; charset=utf-8'},
            json.dumps(segs[:3]))
        self.app.register(
            'GET', '/v1/AUTH_test/c?format=json&prefix=seg_&marker=seg_03',
            swob.HTTPOk, {'Content-Type': 'application/json; charset=utf-8'},
            json.dumps(segs[3:]))

        # Here's a manifest with 0 segments
        self.app.register(
            'GET', '/v1/AUTH_test/mancon/manifest-no-segments',
            swob.HTTPOk, {'Content-Length': '7', 'Etag': 'noseg',
                          'X-Object-Manifest': 'c/noseg_'},
            'noseg')
        self.app.register(
            'GET', '/v1/AUTH_test/c?format=json&prefix=noseg_',
            swob.HTTPOk, {'Content-Type': 'application/json; charset=utf-8'},
            json.dumps([]))


class TestDloPutManifest(DloTestCase):
    def setUp(self):
        super(TestDloPutManifest, self).setUp()
        self.app.register(
            'PUT', '/v1/AUTH_test/c/m',
            swob.HTTPCreated, {}, None)

    def test_validating_x_object_manifest(self):
        exp_okay = ["c/o",
                    "c/obj/with/slashes",
                    "c/obj/with/trailing/slash/",
                    "c/obj/with//multiple///slashes////adjacent"]
        exp_bad = ["",
                   "/leading/slash",
                   "double//slash",
                   "container-only",
                   "whole-container/",
                   "c/o?short=querystring",
                   "c/o?has=a&long-query=string"]

        got_okay = []
        got_bad = []
        for val in (exp_okay + exp_bad):
            req = swob.Request.blank("/v1/AUTH_test/c/m",
                                     environ={'REQUEST_METHOD': 'PUT'},
                                     headers={"X-Object-Manifest": val})
            status, _, _ = self.call_dlo(req)
            if status.startswith("201"):
                got_okay.append(val)
            else:
                got_bad.append(val)

        self.assertEqual(exp_okay, got_okay)
        self.assertEqual(exp_bad, got_bad)

    def test_validation_watches_manifests_with_slashes(self):
        self.app.register(
            'PUT', '/v1/AUTH_test/con/w/x/y/z',
            swob.HTTPCreated, {}, None)

        req = swob.Request.blank(
            "/v1/AUTH_test/con/w/x/y/z", environ={'REQUEST_METHOD': 'PUT'},
            headers={"X-Object-Manifest": 'good/value'})
        status, _, _ = self.call_dlo(req)
        self.assertEqual(status, "201 Created")

        req = swob.Request.blank(
            "/v1/AUTH_test/con/w/x/y/z", environ={'REQUEST_METHOD': 'PUT'},
            headers={"X-Object-Manifest": '/badvalue'})
        status, _, _ = self.call_dlo(req)
        self.assertEqual(status, "400 Bad Request")

    def test_validation_ignores_containers(self):
        self.app.register(
            'PUT', '/v1/a/c',
            swob.HTTPAccepted, {}, None)
        req = swob.Request.blank(
            "/v1/a/c", environ={'REQUEST_METHOD': 'PUT'},
            headers={"X-Object-Manifest": "/superbogus/?wrong=in&every=way"})
        status, _, _ = self.call_dlo(req)
        self.assertEqual(status, "202 Accepted")

    def test_validation_ignores_accounts(self):
        self.app.register(
            'PUT', '/v1/a',
            swob.HTTPAccepted, {}, None)
        req = swob.Request.blank(
            "/v1/a", environ={'REQUEST_METHOD': 'PUT'},
            headers={"X-Object-Manifest": "/superbogus/?wrong=in&every=way"})
        status, _, _ = self.call_dlo(req)
        self.assertEqual(status, "202 Accepted")


class TestDloHeadManifest(DloTestCase):
    def test_head_large_object(self):
        expected_etag = '"%s"' % hashlib.md5(
            "seg01-etag" + "seg02-etag" + "seg03-etag" +
            "seg04-etag" + "seg05-etag").hexdigest()
        req = swob.Request.blank('/v1/AUTH_test/mancon/manifest',
                                 environ={'REQUEST_METHOD': 'HEAD'})
        status, headers, body = self.call_dlo(req)
        headers = swob.HeaderKeyDict(headers)
        self.assertEqual(headers["Etag"], expected_etag)
        self.assertEqual(headers["Content-Length"], "25")

    def test_head_large_object_too_many_segments(self):
        req = swob.Request.blank('/v1/AUTH_test/mancon/manifest-many-segments',
                                 environ={'REQUEST_METHOD': 'HEAD'})
        with mock.patch(LIMIT, 3):
            status, headers, body = self.call_dlo(req)
        headers = swob.HeaderKeyDict(headers)

        # etag is manifest's etag
        self.assertEqual(headers["Etag"], "etag-manyseg")
        self.assertEqual(headers.get("Content-Length"), None)

    def test_head_large_object_no_segments(self):
        req = swob.Request.blank('/v1/AUTH_test/mancon/manifest-no-segments',
                                 environ={'REQUEST_METHOD': 'HEAD'})
        status, headers, body = self.call_dlo(req)
        headers = swob.HeaderKeyDict(headers)
        self.assertEqual(headers["Etag"],
                         '"' + hashlib.md5("").hexdigest() + '"')
        self.assertEqual(headers["Content-Length"], "0")

        # one request to HEAD the manifest
        # one request for the first page of listings
        # *zero* requests for the second page of listings
        self.assertEqual(
            self.app.calls,
            [('HEAD', '/v1/AUTH_test/mancon/manifest-no-segments'),
             ('GET', '/v1/AUTH_test/c?format=json&prefix=noseg_')])


class TestDloGetManifest(DloTestCase):
    def test_get_manifest(self):
        expected_etag = '"%s"' % hashlib.md5(
            "seg01-etag" + "seg02-etag" + "seg03-etag" +
            "seg04-etag" + "seg05-etag").hexdigest()
        req = swob.Request.blank('/v1/AUTH_test/mancon/manifest',
                                 environ={'REQUEST_METHOD': 'GET'})
        status, headers, body = self.call_dlo(req)
        headers = swob.HeaderKeyDict(headers)
        self.assertEqual(headers["Etag"], expected_etag)
        self.assertEqual(headers["Content-Length"], "25")
        self.assertEqual(body, 'aaaaabbbbbcccccdddddeeeee')

        for _, _, hdrs in self.app.calls_with_headers[1:]:
            ua = hdrs.get("User-Agent", "")
            self.assertTrue("DLO MultipartGET" in ua)
            self.assertFalse("DLO MultipartGET DLO MultipartGET" in ua)
        # the first request goes through unaltered
        self.assertFalse(
            "DLO MultipartGET" in self.app.calls_with_headers[0][2])

    def test_get_non_manifest_passthrough(self):
        req = swob.Request.blank('/v1/AUTH_test/c/catpicture.jpg',
                                 environ={'REQUEST_METHOD': 'GET'})
        status, headers, body = self.call_dlo(req)
        self.assertEqual(body, "meow meow meow meow")

    def test_get_non_object_passthrough(self):
        self.app.register('GET', '/info', swob.HTTPOk,
                          {}, 'useful stuff here')
        req = swob.Request.blank('/info',
                                 environ={'REQUEST_METHOD': 'GET'})
        status, headers, body = self.call_dlo(req)
        self.assertEqual(status, '200 OK')
        self.assertEqual(body, 'useful stuff here')
        self.assertEqual(self.app.call_count, 1)

    def test_get_manifest_passthrough(self):
        # reregister it with the query param
        self.app.register(
            'GET', '/v1/AUTH_test/mancon/manifest?multipart-manifest=get',
            swob.HTTPOk, {'Content-Length': '17', 'Etag': 'manifest-etag',
                          'X-Object-Manifest': 'c/seg'},
            'manifest-contents')
        req = swob.Request.blank(
            '/v1/AUTH_test/mancon/manifest',
            environ={'REQUEST_METHOD': 'GET',
                     'QUERY_STRING': 'multipart-manifest=get'})
        status, headers, body = self.call_dlo(req)
        headers = swob.HeaderKeyDict(headers)
        self.assertEqual(headers["Etag"], "manifest-etag")
        self.assertEqual(body, "manifest-contents")

    def test_error_passthrough(self):
        self.app.register(
            'GET', '/v1/AUTH_test/gone/404ed',
            swob.HTTPNotFound, {}, None)
        req = swob.Request.blank('/v1/AUTH_test/gone/404ed',
                                 environ={'REQUEST_METHOD': 'GET'})
        status, headers, body = self.call_dlo(req)
        self.assertEqual(status, '404 Not Found')

    def test_get_range(self):
        req = swob.Request.blank('/v1/AUTH_test/mancon/manifest',
                                 environ={'REQUEST_METHOD': 'GET'},
                                 headers={'Range': 'bytes=8-17'})
        status, headers, body = self.call_dlo(req)
        headers = swob.HeaderKeyDict(headers)
        self.assertEqual(status, "206 Partial Content")
        self.assertEqual(headers["Content-Length"], "10")
        self.assertEqual(body, "bbcccccddd")
        expected_etag = '"%s"' % hashlib.md5(
            "seg01-etag" + "seg02-etag" + "seg03-etag" +
            "seg04-etag" + "seg05-etag").hexdigest()
        self.assertEqual(headers.get("Etag"), expected_etag)

    def test_get_range_on_segment_boundaries(self):
        req = swob.Request.blank('/v1/AUTH_test/mancon/manifest',
                                 environ={'REQUEST_METHOD': 'GET'},
                                 headers={'Range': 'bytes=10-19'})
        status, headers, body = self.call_dlo(req)
        headers = swob.HeaderKeyDict(headers)
        self.assertEqual(status, "206 Partial Content")
        self.assertEqual(headers["Content-Length"], "10")
        self.assertEqual(body, "cccccddddd")

    def test_get_range_first_byte(self):
        req = swob.Request.blank('/v1/AUTH_test/mancon/manifest',
                                 environ={'REQUEST_METHOD': 'GET'},
                                 headers={'Range': 'bytes=0-0'})
        status, headers, body = self.call_dlo(req)
        headers = swob.HeaderKeyDict(headers)
        self.assertEqual(status, "206 Partial Content")
        self.assertEqual(headers["Content-Length"], "1")
        self.assertEqual(body, "a")

    def test_get_range_last_byte(self):
        req = swob.Request.blank('/v1/AUTH_test/mancon/manifest',
                                 environ={'REQUEST_METHOD': 'GET'},
                                 headers={'Range': 'bytes=24-24'})
        status, headers, body = self.call_dlo(req)
        headers = swob.HeaderKeyDict(headers)
        self.assertEqual(status, "206 Partial Content")
        self.assertEqual(headers["Content-Length"], "1")
        self.assertEqual(body, "e")

    def test_get_range_overlapping_end(self):
        req = swob.Request.blank('/v1/AUTH_test/mancon/manifest',
                                 environ={'REQUEST_METHOD': 'GET'},
                                 headers={'Range': 'bytes=18-30'})
        status, headers, body = self.call_dlo(req)
        headers = swob.HeaderKeyDict(headers)
        self.assertEqual(status, "206 Partial Content")
        self.assertEqual(headers["Content-Length"], "7")
        self.assertEqual(headers["Content-Range"], "bytes 18-24/25")
        self.assertEqual(body, "ddeeeee")

    def test_get_range_unsatisfiable(self):
        req = swob.Request.blank('/v1/AUTH_test/mancon/manifest',
                                 environ={'REQUEST_METHOD': 'GET'},
                                 headers={'Range': 'bytes=25-30'})
        status, headers, body = self.call_dlo(req)
        self.assertEqual(status, "416 Requested Range Not Satisfiable")

    def test_get_range_many_segments_satisfiable(self):
        req = swob.Request.blank('/v1/AUTH_test/mancon/manifest-many-segments',
                                 environ={'REQUEST_METHOD': 'GET'},
                                 headers={'Range': 'bytes=3-12'})
        with mock.patch(LIMIT, 3):
            status, headers, body = self.call_dlo(req)
        headers = swob.HeaderKeyDict(headers)
        self.assertEqual(status, "206 Partial Content")
        self.assertEqual(headers["Content-Length"], "10")
        # The /15 here indicates that this is a 15-byte object. DLO can't tell
        # if there are more segments or not without fetching more container
        # listings, though, so we just go with the sum of the lengths of the
        # segments we can see. In an ideal world, this would be "bytes 3-12/*"
        # to indicate that we don't know the full object length. However, RFC
        # 2616 section 14.16 explicitly forbids us from doing that:
        #
        #   A response with status code 206 (Partial Content) MUST NOT include
        #   a Content-Range field with a byte-range-resp-spec of "*".
        #
        # Since the truth is forbidden, we lie.
        self.assertEqual(headers["Content-Range"], "bytes 3-12/15")
        self.assertEqual(body, "aabbbbbccc")

        self.assertEqual(
            self.app.calls,
            [('GET', '/v1/AUTH_test/mancon/manifest-many-segments'),
             ('GET', '/v1/AUTH_test/c?format=json&prefix=seg_'),
             ('GET', '/v1/AUTH_test/c/seg_01'),
             ('GET', '/v1/AUTH_test/c/seg_02'),
             ('GET', '/v1/AUTH_test/c/seg_03')])

    def test_get_range_many_segments_satisfiability_unknown(self):
        req = swob.Request.blank('/v1/AUTH_test/mancon/manifest-many-segments',
                                 environ={'REQUEST_METHOD': 'GET'},
                                 headers={'Range': 'bytes=10-22'})
        with mock.patch(LIMIT, 3):
            status, headers, body = self.call_dlo(req)
        headers = swob.HeaderKeyDict(headers)
        self.assertEqual(status, "200 OK")
        # this requires multiple pages of container listing, so we can't send
        # a Content-Length header
        self.assertEqual(headers.get("Content-Length"), None)
        self.assertEqual(body, "aaaaabbbbbcccccdddddeeeee")

    def test_get_suffix_range(self):
        req = swob.Request.blank('/v1/AUTH_test/mancon/manifest',
                                 environ={'REQUEST_METHOD': 'GET'},
                                 headers={'Range': 'bytes=-40'})
        status, headers, body = self.call_dlo(req)
        headers = swob.HeaderKeyDict(headers)
        self.assertEqual(status, "206 Partial Content")
        self.assertEqual(headers["Content-Length"], "25")
        self.assertEqual(body, "aaaaabbbbbcccccdddddeeeee")

    def test_get_suffix_range_many_segments(self):
        req = swob.Request.blank('/v1/AUTH_test/mancon/manifest-many-segments',
                                 environ={'REQUEST_METHOD': 'GET'},
                                 headers={'Range': 'bytes=-5'})
        with mock.patch(LIMIT, 3):
            status, headers, body = self.call_dlo(req)
        headers = swob.HeaderKeyDict(headers)
        self.assertEqual(status, "200 OK")
        self.assertEqual(headers.get("Content-Length"), None)
        self.assertEqual(headers.get("Content-Range"), None)
        self.assertEqual(body, "aaaaabbbbbcccccdddddeeeee")

    def test_get_multi_range(self):
        # DLO doesn't support multi-range GETs. The way that you express that
        # in HTTP is to return a 200 response containing the whole entity.
        req = swob.Request.blank('/v1/AUTH_test/mancon/manifest-many-segments',
                                 environ={'REQUEST_METHOD': 'GET'},
                                 headers={'Range': 'bytes=5-9,15-19'})
        with mock.patch(LIMIT, 3):
            status, headers, body = self.call_dlo(req)
        headers = swob.HeaderKeyDict(headers)
        self.assertEqual(status, "200 OK")
        self.assertEqual(headers.get("Content-Length"), None)
        self.assertEqual(headers.get("Content-Range"), None)
        self.assertEqual(body, "aaaaabbbbbcccccdddddeeeee")

    def test_error_fetching_first_segment(self):
        self.app.register(
            'GET', '/v1/AUTH_test/c/seg_01',
            swob.HTTPForbidden, {}, None)

        req = swob.Request.blank('/v1/AUTH_test/mancon/manifest',
                                 environ={'REQUEST_METHOD': 'GET'})
        status, headers, body, exc = self.call_dlo(req, expect_exception=True)
        headers = swob.HeaderKeyDict(headers)
        self.assertTrue(isinstance(exc, exceptions.SegmentError))

        self.assertEqual(status, "200 OK")
        self.assertEqual(body, '')  # error right away -> no body bytes sent

    def test_error_fetching_second_segment(self):
        self.app.register(
            'GET', '/v1/AUTH_test/c/seg_02',
            swob.HTTPForbidden, {}, None)

        req = swob.Request.blank('/v1/AUTH_test/mancon/manifest',
                                 environ={'REQUEST_METHOD': 'GET'})
        status, headers, body, exc = self.call_dlo(req, expect_exception=True)
        headers = swob.HeaderKeyDict(headers)

        self.assertTrue(isinstance(exc, exceptions.SegmentError))
        self.assertEqual(status, "200 OK")
        self.assertEqual(''.join(body), "aaaaa")  # first segment made it out

    def test_error_listing_container_first_listing_request(self):
        self.app.register(
            'GET', '/v1/AUTH_test/c?format=json&prefix=seg_',
            swob.HTTPNotFound, {}, None)

        req = swob.Request.blank('/v1/AUTH_test/mancon/manifest-many-segments',
                                 environ={'REQUEST_METHOD': 'GET'},
                                 headers={'Range': 'bytes=-5'})
        with mock.patch(LIMIT, 3):
            status, headers, body = self.call_dlo(req)
        self.assertEqual(status, "404 Not Found")

    def test_error_listing_container_second_listing_request(self):
        self.app.register(
            'GET', '/v1/AUTH_test/c?format=json&prefix=seg_&marker=seg_03',
            swob.HTTPNotFound, {}, None)

        req = swob.Request.blank('/v1/AUTH_test/mancon/manifest-many-segments',
                                 environ={'REQUEST_METHOD': 'GET'},
                                 headers={'Range': 'bytes=-5'})
        with mock.patch(LIMIT, 3):
            status, headers, body, exc = self.call_dlo(
                req, expect_exception=True)
        self.assertTrue(isinstance(exc, exceptions.ListingIterError))
        self.assertEqual(status, "200 OK")
        self.assertEqual(body, "aaaaabbbbbccccc")

    def test_etag_comparison_ignores_quotes(self):
        # a little future-proofing here in case we ever fix this
        self.app.register(
            'HEAD', '/v1/AUTH_test/mani/festo',
            swob.HTTPOk, {'Content-Length': '0', 'Etag': 'blah',
                          'X-Object-Manifest': 'c/quotetags'}, None)
        self.app.register(
            'GET', '/v1/AUTH_test/c?format=json&prefix=quotetags',
            swob.HTTPOk, {'Content-Type': 'application/json; charset=utf-8'},
            json.dumps([{"hash": "\"abc\"", "bytes": 5, "name": "quotetags1",
                         "last_modified": "2013-11-22T02:42:14.261620",
                         "content-type": "application/octet-stream"},
                        {"hash": "def", "bytes": 5, "name": "quotetags2",
                         "last_modified": "2013-11-22T02:42:14.261620",
                         "content-type": "application/octet-stream"}]))

        req = swob.Request.blank('/v1/AUTH_test/mani/festo',
                                 environ={'REQUEST_METHOD': 'HEAD'})
        status, headers, body = self.call_dlo(req)
        headers = swob.HeaderKeyDict(headers)
        self.assertEqual(headers["Etag"],
                         '"' + hashlib.md5("abcdef").hexdigest() + '"')

    def test_object_prefix_quoting(self):
        self.app.register(
            'GET', '/v1/AUTH_test/man/accent',
            swob.HTTPOk, {'Content-Length': '0', 'Etag': 'blah',
                          'X-Object-Manifest': u'c/é'.encode('utf-8')}, None)

        segs = [{"hash": "etag1", "bytes": 5, "name": u"é1"},
                {"hash": "etag2", "bytes": 5, "name": u"é2"}]
        self.app.register(
            'GET', '/v1/AUTH_test/c?format=json&prefix=%C3%A9',
            swob.HTTPOk, {'Content-Type': 'application/json'},
            json.dumps(segs))

        self.app.register(
            'GET', '/v1/AUTH_test/c/\xC3\xa91',
            swob.HTTPOk, {'Content-Length': '5', 'Etag': 'etag1'},
            "AAAAA")
        self.app.register(
            'GET', '/v1/AUTH_test/c/\xC3\xA92',
            swob.HTTPOk, {'Content-Length': '5', 'Etag': 'etag2'},
            "BBBBB")

        req = swob.Request.blank('/v1/AUTH_test/man/accent',
                                 environ={'REQUEST_METHOD': 'GET'})
        status, headers, body = self.call_dlo(req)
        self.assertEqual(status, "200 OK")
        self.assertEqual(body, "AAAAABBBBB")

    def test_get_taking_too_long(self):
        the_time = [time.time()]

        def mock_time():
            return the_time[0]

        # this is just a convenient place to hang a time jump
        def mock_is_success(status_int):
            the_time[0] += 9 * 3600
            return status_int // 100 == 2

        req = swob.Request.blank(
            '/v1/AUTH_test/mancon/manifest',
            environ={'REQUEST_METHOD': 'GET'})

        with contextlib.nested(
                mock.patch('swift.common.utils.time.time', mock_time),
                mock.patch('swift.common.utils.is_success', mock_is_success),
                mock.patch.object(dlo, 'is_success', mock_is_success)):
            status, headers, body, exc = self.call_dlo(
                req, expect_exception=True)

        self.assertEqual(status, '200 OK')
        self.assertEqual(body, 'aaaaabbbbbccccc')
        self.assertTrue(isinstance(exc, exceptions.SegmentError))


def fake_start_response(*args, **kwargs):
    pass


class TestDloCopyHook(DloTestCase):
    def setUp(self):
        super(TestDloCopyHook, self).setUp()

        self.app.register(
            'GET', '/v1/AUTH_test/c/o1', swob.HTTPOk,
            {'Content-Length': '10', 'Etag': 'o1-etag'},
            "aaaaaaaaaa")
        self.app.register(
            'GET', '/v1/AUTH_test/c/o2', swob.HTTPOk,
            {'Content-Length': '10', 'Etag': 'o2-etag'},
            "bbbbbbbbbb")
        self.app.register(
            'GET', '/v1/AUTH_test/c/man',
            swob.HTTPOk, {'X-Object-Manifest': 'c/o'},
            "manifest-contents")

        lm = '2013-11-22T02:42:13.781760'
        ct = 'application/octet-stream'
        segs = [{"hash": "o1-etag", "bytes": 10, "name": "o1",
                 "last_modified": lm, "content_type": ct},
                {"hash": "o2-etag", "bytes": 5, "name": "o2",
                 "last_modified": lm, "content_type": ct}]

        self.app.register(
            'GET', '/v1/AUTH_test/c?format=json&prefix=o',
            swob.HTTPOk, {'Content-Type': 'application/json; charset=utf-8'},
            json.dumps(segs))

        copy_hook = [None]

        # slip this guy in there to pull out the hook
        def extract_copy_hook(env, sr):
            copy_hook[0] = env.get('swift.copy_response_hook')
            return self.app(env, sr)

        self.dlo = dlo.filter_factory({})(extract_copy_hook)

        req = swob.Request.blank('/v1/AUTH_test/c/o1',
                                 environ={'REQUEST_METHOD': 'GET'})
        self.dlo(req.environ, fake_start_response)
        self.copy_hook = copy_hook[0]

        self.assertTrue(self.copy_hook is not None)  # sanity check

    def test_copy_hook_passthrough(self):
        req = swob.Request.blank('/v1/AUTH_test/c/man')
        # no X-Object-Manifest header, so do nothing
        resp = swob.Response(request=req, status=200)

        modified_resp = self.copy_hook(req, resp)
        self.assertTrue(modified_resp is resp)

    def test_copy_hook_manifest(self):
        req = swob.Request.blank('/v1/AUTH_test/c/man')
        resp = swob.Response(request=req, status=200,
                             headers={"X-Object-Manifest": "c/o"},
                             app_iter=["manifest"])

        modified_resp = self.copy_hook(req, resp)
        self.assertTrue(modified_resp is not resp)
        self.assertEqual(modified_resp.etag,
                         hashlib.md5("o1-etago2-etag").hexdigest())


if __name__ == '__main__':
    unittest.main()
