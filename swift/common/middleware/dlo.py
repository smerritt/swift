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

from hashlib import md5
from swift.common.constraints import CONTAINER_LISTING_LIMIT
from swift.common.exceptions import ListingIterError
from swift.common.http import is_success
from swift.common.swob import Request, Response, \
    HTTPRequestedRangeNotSatisfiable, HTTPBadRequest
from swift.common.utils import get_logger, json, SegmentedIterable, \
    RateLimitedIterator
from swift.common.wsgi import WSGIContext
from urllib import quote, unquote


class GetContext(WSGIContext):
    def __init__(self, dlo, logger):
        super(GetContext, self).__init__(dlo.app)
        self.dlo = dlo
        self.logger = logger

    def _get_container_listing(self, req, version, account, container,
                               prefix, marker=''):
        con_req = req.copy_get()
        con_req.script_name = ''
        con_req.range = None
        con_req.path_info = '/'.join(['', version, account, container])
        con_req.query_string = 'format=json&prefix=%s' % quote(prefix)
        con_req.user_agent = '%s DLO MultipartGET' % con_req.user_agent
        if marker:
            con_req.query_string += '&marker=%s' % quote(marker)

        con_resp = con_req.get_response(self.dlo.app)
        if not is_success(con_resp.status_int):
            return con_resp, None
        return None, json.loads(''.join(con_resp.app_iter))

    def _segment_listing_iterator(self, req, version, account, container,
                                  prefix, segments, first_byte=None,
                                  last_byte=None):
        # It's sort of hokey that this thing takes in the first page of
        # segments as an argument, but we need to compute the etag and content
        # length from the first page, and it's better to have a hokey
        # interface than to make redundant requests.
        if first_byte is None:
            first_byte = 0
        if last_byte is None:
            last_byte = float("inf")

        marker = ''
        while True:
            for segment in segments:
                seg_length = int(segment['bytes'])

                if first_byte >= seg_length:
                    # don't need any bytes from this segment
                    first_byte = max(first_byte - seg_length, -1)
                    last_byte = max(last_byte - seg_length, -1)
                    continue
                elif last_byte < 0:
                    # no bytes are needed from this or any future segment
                    break

                seg_name = segment['name']
                if isinstance(seg_name, unicode):
                    seg_name = seg_name.encode("utf-8")

                # (obj path, etag, size, first byte, last byte)
                yield ("/" + "/".join((version, account, container,
                                       seg_name)),
                       # We deliberately omit the etag and size here;
                       # SegmentedIterable will check size and etag if
                       # specified, but we don't want it to. DLOs only care
                       # that the objects' names match the specified prefix.
                       None, None,
                       (None if first_byte <= 0 else first_byte),
                       (None if last_byte >= seg_length - 1 else last_byte))

                first_byte = max(first_byte - seg_length, -1)
                last_byte = max(last_byte - seg_length, -1)

            if len(segments) < CONTAINER_LISTING_LIMIT:
                # a short page means that we're done with the listing
                break
            elif last_byte < 0:
                break

            marker = segments[-1]['name']
            error_response, segments = self._get_container_listing(
                req, version, account, container, prefix, marker)
            if error_response:
                # we've already started sending the response body to the
                # client, so all we can do is raise an exception to make the
                # WSGI server close the connection early
                raise ListingIterError(
                    "Got status %d listing container /%s/%s" %
                    (error_response.status_int, account, container))

    def get_or_head_response(self, req, x_object_manifest,
                             response_headers=None):
        if response_headers is None:
            response_headers = self._response_headers

        container, obj_prefix = x_object_manifest.split('/', 1)
        container = unquote(container)
        obj_prefix = unquote(obj_prefix)

        # manifest might point to a different container
        req.acl = None
        version, account, _junk = req.split_path(2, 3, True)
        error_response, segments = self._get_container_listing(
            req, version, account, container, obj_prefix)
        if error_response:
            return error_response
        have_complete_listing = len(segments) < CONTAINER_LISTING_LIMIT

        first_byte = last_byte = None
        content_length = None
        if req.range and len(req.range.ranges) == 1:
            content_length = sum(o['bytes'] for o in segments)

            # This is a hack to handle suffix byte ranges (e.g. "bytes=-5"),
            # which we can't honor unless we have a complete listing.
            _junk, range_end = req.range.ranges_for_length(float("inf"))[0]

            # If this is all the segments, we know whether or not this
            # range request is satisfiable.
            #
            # Alternately, we may not have all the segments, but this range
            # falls entirely within the first page's segments, so we know
            # whether or not it's satisfiable.
            if have_complete_listing or range_end < content_length:
                byteranges = req.range.ranges_for_length(content_length)
                if not byteranges:
                    return HTTPRequestedRangeNotSatisfiable(request=req)
                first_byte, last_byte = byteranges[0]
                # For some reason, swob.Range.ranges_for_length adds 1 to the
                # last byte's position.
                last_byte -= 1
            else:
                # The range may or may not be satisfiable, but we can't tell
                # based on just one page of listing, and we're not going to go
                # get more pages because that would use up too many resources,
                # so we ignore the Range header and return the whole object.
                content_length = None
                req.range = None

        response_headers = [
            (h, v) for h, v in response_headers
            if h.lower() not in ("content-length", "content-range")]

        if content_length is not None:
            # Here, we have to give swob a big-enough content length so that
            # it can compute the actual content length based on the Range
            # header. This value will not be visible to the client; swob will
            # substitute its own Content-Length.
            #
            # Note: if the manifest points to at least CONTAINER_LISTING_LIMIT
            # segments, this may be less than the sum of all the segments'
            # sizes. However, it'll still be greater than the last byte in the
            # Range header, so it's good enough for swob.
            response_headers.append(('Content-Length', str(content_length)))
        elif have_complete_listing:
            response_headers.append(('Content-Length',
                                     str(sum(o['bytes'] for o in segments))))

        if have_complete_listing:
            response_headers = [(h, v) for h, v in response_headers
                                if h.lower() != "etag"]
            etag = md5()
            for seg_dict in segments:
                etag.update(seg_dict['hash'].strip('"'))
            response_headers.append(('Etag', '"%s"' % etag.hexdigest()))

        listing_iter = RateLimitedIterator(
            self._segment_listing_iterator(
                req, version, account, container, obj_prefix, segments,
                first_byte=first_byte, last_byte=last_byte),
            self.dlo.rate_limit_segments_per_sec,
            limit_after=self.dlo.rate_limit_after_segment)
        resp = Response(request=req, headers=response_headers,
                        conditional_response=True,
                        app_iter=SegmentedIterable(
                            req, self.dlo.app, listing_iter,
                            ua_suffix="DLO MultipartGET",
                            name=req.path, logger=self.logger,
                            max_get_time=86400))
        resp.app_iter.response = resp
        return resp

    def handle_request(self, req, start_response):
        """
        Take a GET or HEAD request, and if it is for a dynamic large object
        manifest, return an appropriate response.

        Otherwise, simply pass it through.
        """
        resp_iter = self._app_call(req.environ)

        # make sure this response is for a dynamic large object manifest
        for header, value in self._response_headers:
            if (header.lower() == 'x-object-manifest'):
                response = self.get_or_head_response(req, value)
                return response(req.environ, start_response)
        else:
            # Not a dynamic large object manifest; just pass it through.
            start_response(self._response_status,
                           self._response_headers,
                           self._response_exc_info)
            return resp_iter


class DynamicLargeObject(object):
    def __init__(self, app, conf):
        self.app = app
        self.conf = conf
        self.logger = get_logger(conf, log_route='dlo')

        self.max_get_time = int(self.conf.get('max_get_time', '86400'))
        self.rate_limit_after_segment = int(self.conf.get(
            'rate_limit_after_segment', '10'))
        self.rate_limit_segments_per_sec = int(self.conf.get(
            'rate_limit_segments_per_sec', '1'))

    def __call__(self, env, start_response):
        """
        WSGI entry point
        """
        req = Request(env)
        try:
            vrs, account, container, obj = req.split_path(4, 4, True)
        except ValueError:
            return self.app(env, start_response)

        # install our COPY-callback hook
        env['swift.copy_response_hook'] = self.copy_response_hook(
            env.get('swift.copy_response_hook', lambda req, resp: resp))

        if ((req.method == 'GET' or req.method == 'HEAD') and
                req.params.get('multipart-manifest') != 'get'):
            return GetContext(self, self.logger).\
                handle_request(req, start_response)
        elif req.method == 'PUT':
            error_response = self.validate_x_object_manifest_header(
                req, start_response)
            if error_response:
                return error_response(env, start_response)
        return self.app(env, start_response)

    def validate_x_object_manifest_header(self, req, start_response):
        """
        Make sure that X-Object-Manifest is valid if present.
        """
        if 'X-Object-Manifest' in req.headers:
            value = req.headers['X-Object-Manifest']
            container = prefix = None
            try:
                container, prefix = value.split('/', 1)
            except ValueError:
                pass
            if not container or not prefix or '?' in value or '&' in value or \
                    prefix[0] == '/':
                return HTTPBadRequest(
                    request=req,
                    body=('X-Object-Manifest must be in the '
                          'format container/prefix'))

    def copy_response_hook(self, inner_hook):

        def dlo_copy_hook(req, resp):
            x_o_m = resp.headers.get('X-Object-Manifest')
            if (x_o_m and req.params.get('multipart-manifest') != 'get'):
                resp = GetContext(self, self.logger).get_or_head_response(
                    req, x_o_m, resp.headers.items())
            return inner_hook(req, resp)

        return dlo_copy_hook


def filter_factory(global_conf, **local_conf):
    conf = global_conf.copy()
    conf.update(local_conf)

    def dlo_filter(app):
        return DynamicLargeObject(app, conf)
    return dlo_filter
