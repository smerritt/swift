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

""" Object Server for Swift """

import cPickle as pickle
import os
import multiprocessing
import time
import traceback
import socket
import math
from datetime import datetime
from swift import gettext_ as _
from hashlib import md5

from eventlet import sleep, Timeout

from swift.common.utils import public, get_logger, \
    config_true_value, timing_stats, replication, normalize_delete_at_timestamp
from swift.common.bufferedhttp import http_connect
from swift.common.constraints import check_object_creation, \
    check_float, check_utf8
from swift.common.exceptions import ConnectionTimeout, DiskFileQuarantined, \
    DiskFileNotExist, DiskFileCollision, DiskFileNoSpace, DiskFileDeleted, \
    DiskFileDeviceUnavailable, DiskFileExpired, ChunkReadTimeout
from swift.obj import ssync_receiver
from swift.common.http import is_success
from swift.common.request_helpers import get_name_and_placement, is_user_meta
from swift.common.swob import HTTPAccepted, HTTPBadRequest, HTTPCreated, \
    HTTPInternalServerError, HTTPNoContent, HTTPNotFound, HTTPNotModified, \
    HTTPPreconditionFailed, HTTPRequestTimeout, HTTPUnprocessableEntity, \
    HTTPClientDisconnect, HTTPMethodNotAllowed, Request, Response, UTC, \
    HTTPInsufficientStorage, HTTPForbidden, HTTPException, HeaderKeyDict, \
    HTTPConflict
from swift.obj.diskfile import DATAFILE_SYSTEM_META, DiskFileManager
from swift.common.storage_policy import POLICY_INDEX


class ObjectController(object):
    """Implements the WSGI application for the Swift Object Server."""

    def __init__(self, conf, logger=None):
        """
        Creates a new WSGI application for the Swift Object Server. An
        example configuration is given at
        <source-dir>/etc/object-server.conf-sample or
        /etc/swift/object-server.conf-sample.
        """
        self.logger = logger or get_logger(conf, log_route='object-server')
        self.node_timeout = int(conf.get('node_timeout', 3))
        self.conn_timeout = float(conf.get('conn_timeout', 0.5))
        self.client_timeout = int(conf.get('client_timeout', 60))
        self.disk_chunk_size = int(conf.get('disk_chunk_size', 65536))
        self.network_chunk_size = int(conf.get('network_chunk_size', 65536))
        self.log_requests = config_true_value(conf.get('log_requests', 'true'))
        self.max_upload_time = int(conf.get('max_upload_time', 86400))
        self.slow = int(conf.get('slow', 0))
        self.keep_cache_private = \
            config_true_value(conf.get('keep_cache_private', 'false'))
        replication_server = conf.get('replication_server', None)
        if replication_server is not None:
            replication_server = config_true_value(replication_server)
        self.replication_server = replication_server

        default_allowed_headers = '''
            content-disposition,
            content-encoding,
            x-delete-at,
            x-object-manifest,
            x-static-large-object,
        '''
        extra_allowed_headers = [
            header.strip().lower() for header in conf.get(
                'allowed_headers', default_allowed_headers).split(',')
            if header.strip()
        ]
        self.allowed_headers = set()
        for header in extra_allowed_headers:
            if header not in DATAFILE_SYSTEM_META:
                self.allowed_headers.add(header)
        self.expiring_objects_account = \
            (conf.get('auto_create_account_prefix') or '.') + \
            (conf.get('expiring_objects_account_name') or 'expiring_objects')
        self.expiring_objects_container_divisor = \
            int(conf.get('expiring_objects_container_divisor') or 86400)
        # Initialization was successful, so now apply the network chunk size
        # parameter as the default read / write buffer size for the network
        # sockets.
        #
        # NOTE WELL: This is a class setting, so until we get set this on a
        # per-connection basis, this affects reading and writing on ALL
        # sockets, those between the proxy servers and external clients, and
        # those between the proxy servers and the other internal servers.
        #
        # ** Because the primary motivation for this is to optimize how data
        # is written back to the proxy server, we could use the value from the
        # disk_chunk_size parameter. However, it affects all created sockets
        # using this class so we have chosen to tie it to the
        # network_chunk_size parameter value instead.
        socket._fileobject.default_bufsize = self.network_chunk_size

        # Provide further setup sepecific to an object server implemenation.
        self.setup(conf)

    def setup(self, conf):
        """
        Implementation specific setup. This method is called at the very end
        by the constructor to allow a specific implementation to modify
        existing attributes or add its own attributes.

        :param conf: WSGI configuration parameter
        """

        # Common on-disk hierarchy shared across account, container and object
        # servers.
        self._diskfile_mgr = DiskFileManager(conf, self.logger)
        # This is populated by global_conf_callback way below as the semaphore
        # is shared by all workers.
        if 'replication_semaphore' in conf:
            # The value was put in a list so it could get past paste
            self.replication_semaphore = conf['replication_semaphore'][0]
        else:
            self.replication_semaphore = None
        self.replication_failure_threshold = int(
            conf.get('replication_failure_threshold') or 100)
        self.replication_failure_ratio = float(
            conf.get('replication_failure_ratio') or 1.0)

    def get_diskfile(self, device, partition, account, container, obj,
                     policy_idx, **kwargs):
        """
        Utility method for instantiating a DiskFile object supporting a given
        REST API.

        An implementation of the object server that wants to use a different
        DiskFile class would simply over-ride this method to provide that
        behavior.
        """
        return self._diskfile_mgr.get_diskfile(
            device, partition, account, container, obj, policy_idx, **kwargs)

    def async_update(self, op, account, container, obj, host, partition,
                     contdevice, headers_out, objdevice, policy_idx):
        """
        Sends or saves an async update.

        :param op: operation performed (ex: 'PUT', or 'DELETE')
        :param account: account name for the object
        :param container: container name for the object
        :param obj: object name
        :param host: host that the container is on
        :param partition: partition that the container is on
        :param contdevice: device name that the container is on
        :param headers_out: dictionary of headers to send in the container
                            request
        :param objdevice: device name that the object is in
        :param policy_idx: the associated storage policy index
        """
        headers_out['user-agent'] = 'obj-server %s' % os.getpid()
        full_path = '/%s/%s/%s' % (account, container, obj)
        if all([host, partition, contdevice]):
            try:
                with ConnectionTimeout(self.conn_timeout):
                    ip, port = host.rsplit(':', 1)
                    conn = http_connect(ip, port, contdevice, partition, op,
                                        full_path, headers_out)
                with Timeout(self.node_timeout):
                    response = conn.getresponse()
                    response.read()
                    if is_success(response.status):
                        return
                    else:
                        self.logger.error(_(
                            'ERROR Container update failed '
                            '(saving for async update later): %(status)d '
                            'response from %(ip)s:%(port)s/%(dev)s'),
                            {'status': response.status, 'ip': ip, 'port': port,
                             'dev': contdevice})
            except (Exception, Timeout):
                self.logger.exception(_(
                    'ERROR container update failed with '
                    '%(ip)s:%(port)s/%(dev)s (saving for async update later)'),
                    {'ip': ip, 'port': port, 'dev': contdevice})
        data = {'op': op, 'account': account, 'container': container,
                'obj': obj, 'headers': headers_out}
        timestamp = headers_out['x-timestamp']
        self._diskfile_mgr.pickle_async_update(objdevice, account, container,
                                               obj, data, timestamp,
                                               policy_idx)

    def container_update(self, op, account, container, obj, request,
                         headers_out, objdevice, policy_idx):
        """
        Update the container when objects are updated.

        :param op: operation performed (ex: 'PUT', or 'DELETE')
        :param account: account name for the object
        :param container: container name for the object
        :param obj: object name
        :param request: the original request object driving the update
        :param headers_out: dictionary of headers to send in the container
                            request(s)
        :param objdevice: device name that the object is in
        """
        headers_in = request.headers
        conthosts = [h.strip() for h in
                     headers_in.get('X-Container-Host', '').split(',')]
        contdevices = [d.strip() for d in
                       headers_in.get('X-Container-Device', '').split(',')]
        contpartition = headers_in.get('X-Container-Partition', '')

        if len(conthosts) != len(contdevices):
            # This shouldn't happen unless there's a bug in the proxy,
            # but if there is, we want to know about it.
            self.logger.error(_('ERROR Container update failed: different  '
                                'numbers of hosts and devices in request: '
                                '"%s" vs "%s"') %
                               (headers_in.get('X-Container-Host', ''),
                                headers_in.get('X-Container-Device', '')))
            return

        if contpartition:
            updates = zip(conthosts, contdevices)
        else:
            updates = []

        headers_out['x-trans-id'] = headers_in.get('x-trans-id', '-')
        headers_out['referer'] = request.as_referer()
        headers_out[POLICY_INDEX.lower()] = str(policy_idx)
        for conthost, contdevice in updates:
            self.async_update(op, account, container, obj, conthost,
                              contpartition, contdevice, headers_out,
                              objdevice, policy_idx)

    def delete_at_update(self, op, delete_at, account, container, obj,
                         request, objdevice):
        """
        Update the expiring objects container when objects are updated.

        :param op: operation performed (ex: 'PUT', or 'DELETE')
        :param delete_at: scheduled delete in UNIX seconds, int
        :param account: account name for the object
        :param container: container name for the object
        :param obj: object name
        :param request: the original request driving the update
        :param objdevice: device name that the object is in
        """
        if config_true_value(
                request.headers.get('x-backend-replication', 'f')):
            return
        delete_at = normalize_delete_at_timestamp(delete_at)
        updates = [(None, None)]

        partition = None
        hosts = contdevices = [None]
        headers_in = request.headers
        headers_out = HeaderKeyDict({
            'x-timestamp': headers_in['x-timestamp'],
            'x-trans-id': headers_in.get('x-trans-id', '-'),
            'referer': request.as_referer()})
        if op != 'DELETE':
            delete_at_container = headers_in.get('X-Delete-At-Container', None)
            if not delete_at_container:
                self.logger.warning(
                    'X-Delete-At-Container header must be specified for '
                    'expiring objects background %s to work properly. Making '
                    'best guess as to the container name for now.' % op)
                # TODO(gholt): In a future release, change the above warning to
                # a raised exception and remove the guess code below.
                delete_at_container = (
                    int(delete_at) / self.expiring_objects_container_divisor *
                    self.expiring_objects_container_divisor)
            partition = headers_in.get('X-Delete-At-Partition', None)
            hosts = headers_in.get('X-Delete-At-Host', '')
            contdevices = headers_in.get('X-Delete-At-Device', '')
            updates = [upd for upd in
                       zip((h.strip() for h in hosts.split(',')),
                           (c.strip() for c in contdevices.split(',')))
                       if all(upd) and partition]
            if not updates:
                updates = [(None, None)]
            headers_out['x-size'] = '0'
            headers_out['x-content-type'] = 'text/plain'
            headers_out['x-etag'] = 'd41d8cd98f00b204e9800998ecf8427e'
        else:
            # DELETEs of old expiration data have no way of knowing what the
            # old X-Delete-At-Container was at the time of the initial setting
            # of the data, so a best guess is made here.
            # Worst case is a DELETE is issued now for something that doesn't
            # exist there and the original data is left where it is, where
            # it will be ignored when the expirer eventually tries to issue the
            # object DELETE later since the X-Delete-At value won't match up.
            delete_at_container = str(
                int(delete_at) / self.expiring_objects_container_divisor *
                self.expiring_objects_container_divisor)
        delete_at_container = normalize_delete_at_timestamp(
            delete_at_container)

        for host, contdevice in updates:
            self.async_update(
                op, self.expiring_objects_account, delete_at_container,
                '%s-%s/%s/%s' % (delete_at, account, container, obj),
                host, partition, contdevice, headers_out, objdevice, 0)

    @public
    @timing_stats()
    def POST(self, request):
        """Handle HTTP POST requests for the Swift Object Server."""
        device, partition, account, container, obj, policy_idx = \
            get_name_and_placement(request, 5, 5, True)
        if 'x-timestamp' not in request.headers or \
                not check_float(request.headers['x-timestamp']):
            return HTTPBadRequest(body='Missing timestamp', request=request,
                                  content_type='text/plain')
        new_delete_at = int(request.headers.get('X-Delete-At') or 0)
        if new_delete_at and new_delete_at < time.time():
            return HTTPBadRequest(body='X-Delete-At in past', request=request,
                                  content_type='text/plain')
        try:
            disk_file = self.get_diskfile(
                device, partition, account, container, obj,
                policy_idx=policy_idx)
        except DiskFileDeviceUnavailable:
            return HTTPInsufficientStorage(drive=device, request=request)
        try:
            orig_metadata = disk_file.read_metadata()
        except (DiskFileNotExist, DiskFileQuarantined):
            return HTTPNotFound(request=request)
        orig_timestamp = orig_metadata.get('X-Timestamp', '0')
        if orig_timestamp >= request.headers['x-timestamp']:
            return HTTPConflict(request=request)
        metadata = {'X-Timestamp': request.headers['x-timestamp']}
        metadata.update(val for val in request.headers.iteritems()
                        if is_user_meta('object', val[0]))
        for header_key in self.allowed_headers:
            if header_key in request.headers:
                header_caps = header_key.title()
                metadata[header_caps] = request.headers[header_key]
        orig_delete_at = int(orig_metadata.get('X-Delete-At') or 0)
        if orig_delete_at != new_delete_at:
            if new_delete_at:
                self.delete_at_update('PUT', new_delete_at, account, container,
                                      obj, request, device)
            if orig_delete_at:
                self.delete_at_update('DELETE', orig_delete_at, account,
                                      container, obj, request, device)
        disk_file.write_metadata(metadata)
        return HTTPAccepted(request=request)

    @public
    @timing_stats()
    def PUT(self, request):
        """Handle HTTP PUT requests for the Swift Object Server."""
        device, partition, account, container, obj, policy_idx = \
            get_name_and_placement(request, 5, 5, True)
        if 'x-timestamp' not in request.headers or \
                not check_float(request.headers['x-timestamp']):
            return HTTPBadRequest(body='Missing timestamp', request=request,
                                  content_type='text/plain')
        error_response = check_object_creation(request, obj)
        if error_response:
            return error_response
        new_delete_at = int(request.headers.get('X-Delete-At') or 0)
        if new_delete_at and new_delete_at < time.time():
            return HTTPBadRequest(body='X-Delete-At in past', request=request,
                                  content_type='text/plain')
        try:
            fsize = request.message_length()
        except ValueError as e:
            return HTTPBadRequest(body=str(e), request=request,
                                  content_type='text/plain')
        try:
            disk_file = self.get_diskfile(
                device, partition, account, container, obj,
                policy_idx=policy_idx)
        except DiskFileDeviceUnavailable:
            return HTTPInsufficientStorage(drive=device, request=request)
        try:
            orig_metadata = disk_file.read_metadata()
        except (DiskFileNotExist, DiskFileQuarantined):
            orig_metadata = {}
        orig_timestamp = orig_metadata.get('X-Timestamp')
        if orig_timestamp and orig_timestamp >= request.headers['x-timestamp']:
            return HTTPConflict(request=request)
        orig_delete_at = int(orig_metadata.get('X-Delete-At') or 0)
        upload_expiration = time.time() + self.max_upload_time
        etag = md5()
        elapsed_time = 0
        try:
            with disk_file.create(size=fsize) as writer:
                upload_size = 0

                def timeout_reader():
                    with ChunkReadTimeout(self.client_timeout):
                        return request.environ['wsgi.input'].read(
                            self.network_chunk_size)

                try:
                    for chunk in iter(lambda: timeout_reader(), ''):
                        start_time = time.time()
                        if start_time > upload_expiration:
                            self.logger.increment('PUT.timeouts')
                            return HTTPRequestTimeout(request=request)
                        etag.update(chunk)
                        upload_size = writer.write(chunk)
                        elapsed_time += time.time() - start_time
                except ChunkReadTimeout:
                    return HTTPRequestTimeout(request=request)
                if upload_size:
                    self.logger.transfer_rate(
                        'PUT.' + device + '.timing', elapsed_time,
                        upload_size)
                if fsize is not None and fsize != upload_size:
                    return HTTPClientDisconnect(request=request)
                etag = etag.hexdigest()
                if 'etag' in request.headers and \
                        request.headers['etag'].lower() != etag:
                    return HTTPUnprocessableEntity(request=request)
                metadata = {
                    'X-Timestamp': request.headers['x-timestamp'],
                    'Content-Type': request.headers['content-type'],
                    'ETag': etag,
                    'Content-Length': str(upload_size),
                }
                metadata.update(val for val in request.headers.iteritems()
                                if is_user_meta('object', val[0]))
                for header_key in (
                        request.headers.get('X-Backend-Replication-Headers') or
                        self.allowed_headers):
                    if header_key in request.headers:
                        header_caps = header_key.title()
                        metadata[header_caps] = request.headers[header_key]
                writer.put(metadata)
        except DiskFileNoSpace:
            return HTTPInsufficientStorage(drive=device, request=request)
        if orig_delete_at != new_delete_at:
            if new_delete_at:
                self.delete_at_update(
                    'PUT', new_delete_at, account, container, obj, request,
                    device)
            if orig_delete_at:
                self.delete_at_update(
                    'DELETE', orig_delete_at, account, container, obj,
                    request, device)
        self.container_update(
            'PUT', account, container, obj, request,
            HeaderKeyDict({
                'x-size': metadata['Content-Length'],
                'x-content-type': metadata['Content-Type'],
                'x-timestamp': metadata['X-Timestamp'],
                'x-etag': metadata['ETag']}),
            device, policy_idx)
        return HTTPCreated(request=request, etag=etag)

    @public
    @timing_stats()
    def GET(self, request):
        """Handle HTTP GET requests for the Swift Object Server."""
        device, partition, account, container, obj, policy_idx = \
            get_name_and_placement(request, 5, 5, True)
        keep_cache = self.keep_cache_private or (
            'X-Auth-Token' not in request.headers and
            'X-Storage-Token' not in request.headers)
        try:
            disk_file = self.get_diskfile(
                device, partition, account, container, obj,
                policy_idx=policy_idx)
        except DiskFileDeviceUnavailable:
            return HTTPInsufficientStorage(drive=device, request=request)
        try:
            with disk_file.open():
                metadata = disk_file.get_metadata()
                obj_size = int(metadata['Content-Length'])
                file_x_ts = metadata['X-Timestamp']
                file_x_ts_flt = float(file_x_ts)
                file_x_ts_utc = datetime.fromtimestamp(file_x_ts_flt, UTC)

                if_unmodified_since = request.if_unmodified_since
                if if_unmodified_since and file_x_ts_utc > if_unmodified_since:
                    return HTTPPreconditionFailed(request=request)

                if_modified_since = request.if_modified_since
                if if_modified_since and file_x_ts_utc <= if_modified_since:
                    return HTTPNotModified(request=request)

                keep_cache = (self.keep_cache_private or
                              ('X-Auth-Token' not in request.headers and
                               'X-Storage-Token' not in request.headers))
                response = Response(
                    app_iter=disk_file.reader(keep_cache=keep_cache),
                    request=request, conditional_response=True)
                response.headers['Content-Type'] = metadata.get(
                    'Content-Type', 'application/octet-stream')
                for key, value in metadata.iteritems():
                    if is_user_meta('object', key) or \
                            key.lower() in self.allowed_headers:
                        response.headers[key] = value
                response.etag = metadata['ETag']
                response.last_modified = math.ceil(file_x_ts_flt)
                response.content_length = obj_size
                try:
                    response.content_encoding = metadata[
                        'Content-Encoding']
                except KeyError:
                    pass
                response.headers['X-Timestamp'] = file_x_ts
                resp = request.get_response(response)
        except (DiskFileNotExist, DiskFileQuarantined):
            resp = HTTPNotFound(request=request, conditional_response=True)
        return resp

    @public
    @timing_stats(sample_rate=0.8)
    def HEAD(self, request):
        """Handle HTTP HEAD requests for the Swift Object Server."""
        device, partition, account, container, obj, policy_idx = \
            get_name_and_placement(request, 5, 5, True)
        try:
            disk_file = self.get_diskfile(
                device, partition, account, container, obj,
                policy_idx=policy_idx)
        except DiskFileDeviceUnavailable:
            return HTTPInsufficientStorage(drive=device, request=request)
        try:
            metadata = disk_file.read_metadata()
        except (DiskFileNotExist, DiskFileQuarantined):
            return HTTPNotFound(request=request, conditional_response=True)
        response = Response(request=request, conditional_response=True)
        response.headers['Content-Type'] = metadata.get(
            'Content-Type', 'application/octet-stream')
        for key, value in metadata.iteritems():
            if is_user_meta('object', key) or \
                    key.lower() in self.allowed_headers:
                response.headers[key] = value
        response.etag = metadata['ETag']
        ts = metadata['X-Timestamp']
        response.last_modified = math.ceil(float(ts))
        # Needed for container sync feature
        response.headers['X-Timestamp'] = ts
        response.content_length = int(metadata['Content-Length'])
        try:
            response.content_encoding = metadata['Content-Encoding']
        except KeyError:
            pass
        return response

    @public
    @timing_stats()
    def DELETE(self, request):
        """Handle HTTP DELETE requests for the Swift Object Server."""
        device, partition, account, container, obj, policy_idx = \
            get_name_and_placement(request, 5, 5, True)
        if 'x-timestamp' not in request.headers or \
                not check_float(request.headers['x-timestamp']):
            return HTTPBadRequest(body='Missing timestamp', request=request,
                                  content_type='text/plain')
        try:
            disk_file = self.get_diskfile(
                device, partition, account, container, obj,
                policy_idx=policy_idx)
        except DiskFileDeviceUnavailable:
            return HTTPInsufficientStorage(drive=device, request=request)
        try:
            orig_metadata = disk_file.read_metadata()
        except DiskFileExpired as e:
            orig_timestamp = e.timestamp
            orig_metadata = e.metadata
            response_class = HTTPNotFound
        except DiskFileDeleted as e:
            orig_timestamp = e.timestamp
            orig_metadata = {}
            response_class = HTTPNotFound
        except (DiskFileNotExist, DiskFileQuarantined):
            orig_timestamp = 0
            orig_metadata = {}
            response_class = HTTPNotFound
        else:
            orig_timestamp = orig_metadata.get('X-Timestamp', 0)
            if orig_timestamp < request.headers['x-timestamp']:
                response_class = HTTPNoContent
            else:
                response_class = HTTPConflict
        orig_delete_at = int(orig_metadata.get('X-Delete-At') or 0)
        try:
            req_if_delete_at_val = request.headers['x-if-delete-at']
            req_if_delete_at = int(req_if_delete_at_val)
        except KeyError:
            pass
        except ValueError:
            return HTTPBadRequest(
                request=request,
                body='Bad X-If-Delete-At header value')
        else:
            if orig_delete_at != req_if_delete_at:
                return HTTPPreconditionFailed(
                    request=request,
                    body='X-If-Delete-At and X-Delete-At do not match')
        if orig_delete_at:
            self.delete_at_update('DELETE', orig_delete_at, account,
                                  container, obj, request, device)
        req_timestamp = request.headers['X-Timestamp']
        if orig_timestamp < req_timestamp:
            disk_file.delete(req_timestamp)
            self.container_update(
                'DELETE', account, container, obj, request,
                HeaderKeyDict({'x-timestamp': req_timestamp}),
                device, policy_idx)
        return response_class(request=request)

    @public
    @replication
    @timing_stats(sample_rate=0.1)
    def REPLICATE(self, request):
        """
        Handle REPLICATE requests for the Swift Object Server.  This is used
        by the object replicator to get hashes for directories.
        """
        device, partition, suffix, policy_idx = \
            get_name_and_placement(request, 2, 3, True)
        try:
            hashes = self._diskfile_mgr.get_hashes(device, partition, suffix,
                                                   policy_idx)
        except DiskFileDeviceUnavailable:
            resp = HTTPInsufficientStorage(drive=device, request=request)
        else:
            resp = Response(body=pickle.dumps(hashes))
        return resp

    @public
    @replication
    @timing_stats(sample_rate=0.1)
    def REPLICATION(self, request):
        return Response(app_iter=ssync_receiver.Receiver(self, request)())

    def __call__(self, env, start_response):
        """WSGI Application entry point for the Swift Object Server."""
        start_time = time.time()
        req = Request(env)
        self.logger.txn_id = req.headers.get('x-trans-id', None)

        if not check_utf8(req.path_info):
            res = HTTPPreconditionFailed(body='Invalid UTF8 or contains NULL')
        else:
            try:
                # disallow methods which have not been marked 'public'
                try:
                    method = getattr(self, req.method)
                    getattr(method, 'publicly_accessible')
                    replication_method = getattr(method, 'replication', False)
                    if (self.replication_server is not None and
                            self.replication_server != replication_method):
                        raise AttributeError('Not allowed method.')
                except AttributeError:
                    res = HTTPMethodNotAllowed()
                else:
                    res = method(req)
            except DiskFileCollision:
                res = HTTPForbidden(request=req)
            except HTTPException as error_response:
                res = error_response
            except (Exception, Timeout):
                self.logger.exception(_(
                    'ERROR __call__ error with %(method)s'
                    ' %(path)s '), {'method': req.method, 'path': req.path})
                res = HTTPInternalServerError(body=traceback.format_exc())
        trans_time = time.time() - start_time
        if self.log_requests:
            log_line = '%s - - [%s] "%s %s" %s %s "%s" "%s" "%s" %.4f' % (
                req.remote_addr,
                time.strftime('%d/%b/%Y:%H:%M:%S +0000',
                              time.gmtime()),
                req.method, req.path, res.status.split()[0],
                res.content_length or '-', req.referer or '-',
                req.headers.get('x-trans-id', '-'),
                req.user_agent or '-',
                trans_time)
            if req.method in ('REPLICATE', 'REPLICATION') or \
                    'X-Backend-Replication' in req.headers:
                self.logger.debug(log_line)
            else:
                self.logger.info(log_line)
        if req.method in ('PUT', 'DELETE'):
            slow = self.slow - trans_time
            if slow > 0:
                sleep(slow)
        return res(env, start_response)


def global_conf_callback(preloaded_app_conf, global_conf):
    """
    Callback for swift.common.wsgi.run_wsgi during the global_conf
    creation so that we can add our replication_semaphore, used to
    limit the number of concurrent REPLICATION_REQUESTS across all
    workers.

    :param preloaded_app_conf: The preloaded conf for the WSGI app.
                               This conf instance will go away, so
                               just read from it, don't write.
    :param global_conf: The global conf that will eventually be
                        passed to the app_factory function later.
                        This conf is created before the worker
                        subprocesses are forked, so can be useful to
                        set up semaphores, shared memory, etc.
    """
    replication_concurrency = int(
        preloaded_app_conf.get('replication_concurrency') or 4)
    if replication_concurrency:
        # Have to put the value in a list so it can get past paste
        global_conf['replication_semaphore'] = [
            multiprocessing.BoundedSemaphore(replication_concurrency)]


def app_factory(global_conf, **local_conf):
    """paste.deploy app factory for creating WSGI object server apps"""
    conf = global_conf.copy()
    conf.update(local_conf)
    return ObjectController(conf)
