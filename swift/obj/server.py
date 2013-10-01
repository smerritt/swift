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

""" Object Server for Swift """

from __future__ import with_statement
import cPickle as pickle
import os
import time
import traceback
from collections import defaultdict
from datetime import datetime
from swift import gettext_ as _
from hashlib import md5

from eventlet import sleep, Timeout

from swift.common.utils import mkdirs, public, get_logger, write_pickle, \
    config_true_value, timing_stats, ThreadPool, replication
from swift.common.ondisk import normalize_timestamp, hash_path
from swift.common.bufferedhttp import http_connect
from swift.common.constraints import check_object_creation, check_mount, \
    check_float, check_utf8
from swift.common.exceptions import ConnectionTimeout, DiskFileError, \
    DiskFileNotExist, DiskFileCollision, DiskFileNoSpace, \
    DiskFileDeviceUnavailable
from swift.common.http import is_success
from swift.common.request_helpers import split_and_validate_path, \
    get_hash_algorithm
from swift.common.swob import HTTPAccepted, HTTPBadRequest, HTTPCreated, \
    HTTPInternalServerError, HTTPNoContent, HTTPNotFound, HTTPNotModified, \
    HTTPPreconditionFailed, HTTPRequestTimeout, HTTPUnprocessableEntity, \
    HTTPClientDisconnect, HTTPMethodNotAllowed, Request, Response, UTC, \
    HTTPInsufficientStorage, HTTPForbidden, HTTPException, HeaderKeyDict, \
    HTTPConflict
from swift.obj.diskfile import DATAFILE_SYSTEM_META, DiskFile, \
    get_hashes


DATADIR = 'objects'
ASYNCDIR = 'async_pending'
MAX_OBJECT_NAME_LENGTH = 1024


class ObjectController(object):
    """Implements the WSGI application for the Swift Object Server."""

    def __init__(self, conf):
        """
        Creates a new WSGI application for the Swift Object Server. An
        example configuration is given at
        <source-dir>/etc/object-server.conf-sample or
        /etc/swift/object-server.conf-sample.
        """
        self.logger = get_logger(conf, log_route='object-server')
        self.devices = conf.get('devices', '/srv/node/')
        self.mount_check = config_true_value(conf.get('mount_check', 'true'))
        self.node_timeout = int(conf.get('node_timeout', 3))
        self.conn_timeout = float(conf.get('conn_timeout', 0.5))
        self.disk_chunk_size = int(conf.get('disk_chunk_size', 65536))
        self.network_chunk_size = int(conf.get('network_chunk_size', 65536))
        self.keep_cache_size = int(conf.get('keep_cache_size', 5242880))
        self.keep_cache_private = \
            config_true_value(conf.get('keep_cache_private', 'false'))
        self.log_requests = config_true_value(conf.get('log_requests', 'true'))
        self.max_upload_time = int(conf.get('max_upload_time', 86400))
        self.slow = int(conf.get('slow', 0))
        self.bytes_per_sync = int(conf.get('mb_per_sync', 512)) * 1024 * 1024
        replication_server = conf.get('replication_server', None)
        if replication_server is not None:
            replication_server = config_true_value(replication_server)
        self.replication_server = replication_server
        self.threads_per_disk = int(conf.get('threads_per_disk', '0'))
        self.threadpools = defaultdict(
            lambda: ThreadPool(nthreads=self.threads_per_disk))
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
            'expiring_objects'
        self.expiring_objects_container_divisor = \
            int(conf.get('expiring_objects_container_divisor') or 86400)

    def _diskfile(self, device, partition, account, container, obj, **kwargs):
        """Utility method for instantiating a DiskFile."""
        kwargs.setdefault('mount_check', self.mount_check)
        kwargs.setdefault('bytes_per_sync', self.bytes_per_sync)
        kwargs.setdefault('disk_chunk_size', self.disk_chunk_size)
        kwargs.setdefault('threadpool', self.threadpools[device])
        kwargs.setdefault('obj_dir', DATADIR)
        return DiskFile(self.devices, device, partition, account,
                        container, obj, self.logger, **kwargs)

    def async_update(self, op, account, container, obj, host, partition,
                     contdevice, headers_out, objdevice):
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
        async_dir = os.path.join(self.devices, objdevice, ASYNCDIR)
        ohash = hash_path(account, container, obj)
        self.logger.increment('async_pendings')
        self.threadpools[objdevice].run_in_thread(
            write_pickle,
            {'op': op, 'account': account, 'container': container,
             'obj': obj, 'headers': headers_out},
            os.path.join(async_dir, ohash[-3:], ohash + '-' +
                         normalize_timestamp(headers_out['x-timestamp'])),
            os.path.join(self.devices, objdevice, 'tmp'))

    def container_update(self, op, account, container, obj, request,
                         headers_out, objdevice):
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
        for conthost, contdevice in updates:
            self.async_update(op, account, container, obj, conthost,
                              contpartition, contdevice, headers_out,
                              objdevice)

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
        # Quick cap that will work from now until Sat Nov 20 17:46:39 2286
        # At that time, Swift will be so popular and pervasive I will have
        # created income for thousands of future programmers.
        delete_at = max(min(delete_at, 9999999999), 0)
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
                delete_at_container = str(
                    delete_at / self.expiring_objects_container_divisor *
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
                delete_at / self.expiring_objects_container_divisor *
                self.expiring_objects_container_divisor)

        for host, contdevice in updates:
            self.async_update(
                op, self.expiring_objects_account, delete_at_container,
                '%s-%s/%s/%s' % (delete_at, account, container, obj),
                host, partition, contdevice, headers_out, objdevice)

    @public
    @timing_stats()
    def POST(self, request):
        """Handle HTTP POST requests for the Swift Object Server."""
        device, partition, account, container, obj = \
            split_and_validate_path(request, 5, 5, True)

        if 'x-timestamp' not in request.headers or \
                not check_float(request.headers['x-timestamp']):
            return HTTPBadRequest(body='Missing timestamp', request=request,
                                  content_type='text/plain')
        new_delete_at = int(request.headers.get('X-Delete-At') or 0)
        if new_delete_at and new_delete_at < time.time():
            return HTTPBadRequest(body='X-Delete-At in past', request=request,
                                  content_type='text/plain')
        try:
            disk_file = self._diskfile(
                device, partition, account, container, obj,
                hash_algorithm=get_hash_algorithm(request))
        except DiskFileDeviceUnavailable:
            return HTTPInsufficientStorage(drive=device, request=request)
        with disk_file.open():
            if disk_file.is_deleted() or disk_file.is_expired():
                return HTTPNotFound(request=request)
            try:
                disk_file.get_data_file_size()
            except (DiskFileError, DiskFileNotExist):
                disk_file.quarantine()
                return HTTPNotFound(request=request)
            orig_metadata = disk_file.get_metadata()
        orig_timestamp = orig_metadata.get('X-Timestamp', '0')
        if orig_timestamp >= request.headers['x-timestamp']:
            return HTTPConflict(request=request)
        metadata = {'X-Timestamp': request.headers['x-timestamp']}
        metadata.update(val for val in request.headers.iteritems()
                        if val[0].startswith('X-Object-Meta-'))
        for header_key in self.allowed_headers:
            if header_key in request.headers:
                header_caps = header_key.title()
                metadata[header_caps] = request.headers[header_key]
        old_delete_at = int(orig_metadata.get('X-Delete-At') or 0)
        if old_delete_at != new_delete_at:
            if new_delete_at:
                self.delete_at_update('PUT', new_delete_at, account, container,
                                      obj, request, device)
            if old_delete_at:
                self.delete_at_update('DELETE', old_delete_at, account,
                                      container, obj, request, device)
        disk_file.put_metadata(metadata)
        return HTTPAccepted(request=request)

    @public
    @timing_stats()
    def PUT(self, request):
        """Handle HTTP PUT requests for the Swift Object Server."""
        device, partition, account, container, obj = \
            split_and_validate_path(request, 5, 5, True)

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
            disk_file = self._diskfile(
                device, partition, account, container, obj,
                hash_algorithm=get_hash_algorithm(request))
        except DiskFileDeviceUnavailable:
            return HTTPInsufficientStorage(drive=device, request=request)
        with disk_file.open():
            orig_metadata = disk_file.get_metadata()
        old_delete_at = int(orig_metadata.get('X-Delete-At') or 0)
        orig_timestamp = orig_metadata.get('X-Timestamp')
        if orig_timestamp and orig_timestamp >= request.headers['x-timestamp']:
            return HTTPConflict(request=request)
        upload_expiration = time.time() + self.max_upload_time
        etag = md5()
        elapsed_time = 0
        try:
            with disk_file.create(size=fsize) as writer:
                reader = request.environ['wsgi.input'].read
                for chunk in iter(lambda: reader(self.network_chunk_size), ''):
                    start_time = time.time()
                    if start_time > upload_expiration:
                        self.logger.increment('PUT.timeouts')
                        return HTTPRequestTimeout(request=request)
                    etag.update(chunk)
                    writer.write(chunk)
                    sleep()
                    elapsed_time += time.time() - start_time
                upload_size = writer.upload_size
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
                                if val[0].lower().startswith('x-object-meta-')
                                and len(val[0]) > 14)
                for header_key in self.allowed_headers:
                    if header_key in request.headers:
                        header_caps = header_key.title()
                        metadata[header_caps] = request.headers[header_key]
                writer.put(metadata)
        except DiskFileNoSpace:
            return HTTPInsufficientStorage(drive=device, request=request)
        if old_delete_at != new_delete_at:
            if new_delete_at:
                self.delete_at_update(
                    'PUT', new_delete_at, account, container, obj,
                    request, device)
            if old_delete_at:
                self.delete_at_update(
                    'DELETE', old_delete_at, account, container, obj,
                    request, device)
        if not orig_timestamp or \
                orig_timestamp < request.headers['x-timestamp']:
            self.container_update(
                'PUT', account, container, obj, request,
                HeaderKeyDict({
                    'x-size': metadata['Content-Length'],
                    'x-content-type': metadata['Content-Type'],
                    'x-timestamp': metadata['X-Timestamp'],
                    'x-etag': metadata['ETag']}),
                device)
        resp = HTTPCreated(request=request, etag=etag)
        return resp

    @public
    @timing_stats()
    def GET(self, request):
        """Handle HTTP GET requests for the Swift Object Server."""
        device, partition, account, container, obj = \
            split_and_validate_path(request, 5, 5, True)
        try:
            disk_file = self._diskfile(
                device, partition, account, container, obj,
                iter_hook=sleep, hash_algorithm=get_hash_algorithm(request))
        except DiskFileDeviceUnavailable:
            return HTTPInsufficientStorage(drive=device, request=request)
        disk_file.open()
        if disk_file.is_deleted() or disk_file.is_expired():
            if request.headers.get('if-match') == '*':
                return HTTPPreconditionFailed(request=request)
            else:
                return HTTPNotFound(request=request)
        try:
            file_size = disk_file.get_data_file_size()
        except (DiskFileError, DiskFileNotExist):
            disk_file.quarantine()
            return HTTPNotFound(request=request)
        metadata = disk_file.get_metadata()
        if request.headers.get('if-match') not in (None, '*') and \
                metadata['ETag'] not in request.if_match:
            disk_file.close()
            return HTTPPreconditionFailed(request=request)
        if request.headers.get('if-none-match') is not None:
            if metadata['ETag'] in request.if_none_match:
                resp = HTTPNotModified(request=request)
                resp.etag = metadata['ETag']
                disk_file.close()
                return resp
        try:
            if_unmodified_since = request.if_unmodified_since
        except (OverflowError, ValueError):
            # catches timestamps before the epoch
            return HTTPPreconditionFailed(request=request)
        if if_unmodified_since and \
                datetime.fromtimestamp(
                    float(metadata['X-Timestamp']), UTC) > \
                if_unmodified_since:
            disk_file.close()
            return HTTPPreconditionFailed(request=request)
        try:
            if_modified_since = request.if_modified_since
        except (OverflowError, ValueError):
            # catches timestamps before the epoch
            return HTTPPreconditionFailed(request=request)
        if if_modified_since and \
                datetime.fromtimestamp(
                    float(metadata['X-Timestamp']), UTC) < \
                if_modified_since:
            disk_file.close()
            return HTTPNotModified(request=request)
        response = Response(app_iter=disk_file,
                            request=request, conditional_response=True)
        response.headers['Content-Type'] = metadata.get(
            'Content-Type', 'application/octet-stream')
        for key, value in metadata.iteritems():
            if key.lower().startswith('x-object-meta-') or \
                    key.lower() in self.allowed_headers:
                response.headers[key] = value
        response.etag = metadata['ETag']
        response.last_modified = float(metadata['X-Timestamp'])
        response.content_length = file_size
        if response.content_length < self.keep_cache_size and \
                (self.keep_cache_private or
                 ('X-Auth-Token' not in request.headers and
                  'X-Storage-Token' not in request.headers)):
            disk_file.keep_cache = True
        if 'Content-Encoding' in metadata:
            response.content_encoding = metadata['Content-Encoding']
        response.headers['X-Timestamp'] = metadata['X-Timestamp']
        return request.get_response(response)

    @public
    @timing_stats(sample_rate=0.8)
    def HEAD(self, request):
        """Handle HTTP HEAD requests for the Swift Object Server."""
        device, partition, account, container, obj = \
            split_and_validate_path(request, 5, 5, True)
        try:
            disk_file = self._diskfile(
                device, partition, account, container, obj,
                hash_algorithm=get_hash_algorithm(request))
        except DiskFileDeviceUnavailable:
            return HTTPInsufficientStorage(drive=device, request=request)
        with disk_file.open():
            if disk_file.is_deleted() or disk_file.is_expired():
                return HTTPNotFound(request=request)
            try:
                file_size = disk_file.get_data_file_size()
            except (DiskFileError, DiskFileNotExist):
                disk_file.quarantine()
                return HTTPNotFound(request=request)
            metadata = disk_file.get_metadata()
        response = Response(request=request, conditional_response=True)
        response.headers['Content-Type'] = metadata.get(
            'Content-Type', 'application/octet-stream')
        for key, value in metadata.iteritems():
            if key.lower().startswith('x-object-meta-') or \
                    key.lower() in self.allowed_headers:
                response.headers[key] = value
        response.etag = metadata['ETag']
        response.last_modified = float(metadata['X-Timestamp'])
        # Needed for container sync feature
        response.headers['X-Timestamp'] = metadata['X-Timestamp']
        response.content_length = file_size
        if 'Content-Encoding' in metadata:
            response.content_encoding = metadata['Content-Encoding']
        return response

    @public
    @timing_stats()
    def DELETE(self, request):
        """Handle HTTP DELETE requests for the Swift Object Server."""
        device, partition, account, container, obj = \
            split_and_validate_path(request, 5, 5, True)
        if 'x-timestamp' not in request.headers or \
                not check_float(request.headers['x-timestamp']):
            return HTTPBadRequest(body='Missing timestamp', request=request,
                                  content_type='text/plain')
        try:
            disk_file = self._diskfile(
                device, partition, account, container, obj,
                hash_algorithm=get_hash_algorithm(request))
        except DiskFileDeviceUnavailable:
            return HTTPInsufficientStorage(drive=device, request=request)
        with disk_file.open():
            orig_metadata = disk_file.get_metadata()
            is_deleted = disk_file.is_deleted()
            is_expired = disk_file.is_expired()
        if 'x-if-delete-at' in request.headers and \
                int(request.headers['x-if-delete-at']) != \
                int(orig_metadata.get('X-Delete-At') or 0):
            return HTTPPreconditionFailed(
                request=request,
                body='X-If-Delete-At and X-Delete-At do not match')
        old_delete_at = int(orig_metadata.get('X-Delete-At') or 0)
        if old_delete_at:
            self.delete_at_update('DELETE', old_delete_at, account,
                                  container, obj, request, device)
        orig_timestamp = orig_metadata.get('X-Timestamp', 0)
        req_timestamp = request.headers['X-Timestamp']
        if is_deleted or is_expired:
            response_class = HTTPNotFound
        else:
            if orig_timestamp < req_timestamp:
                response_class = HTTPNoContent
            else:
                response_class = HTTPConflict
        if orig_timestamp < req_timestamp:
            disk_file.delete(req_timestamp)
            self.container_update(
                'DELETE', account, container, obj, request,
                HeaderKeyDict({'x-timestamp': req_timestamp}),
                device)
        resp = response_class(request=request)
        return resp

    @public
    @replication
    @timing_stats(sample_rate=0.1)
    def REPLICATE(self, request):
        """
        Handle REPLICATE requests for the Swift Object Server.  This is used
        by the object replicator to get hashes for directories.
        """
        device, partition, suffix = split_and_validate_path(
            request, 2, 3, True)

        if self.mount_check and not check_mount(self.devices, device):
            return HTTPInsufficientStorage(drive=device, request=request)
        path = os.path.join(self.devices, device, DATADIR, partition)
        if not os.path.exists(path):
            mkdirs(path)
        suffixes = suffix.split('-') if suffix else []
        _junk, hashes = self.threadpools[device].force_run_in_thread(
            get_hashes, path, recalculate=suffixes)
        return Response(body=pickle.dumps(hashes))

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
            if req.method == 'REPLICATE':
                self.logger.debug(log_line)
            else:
                self.logger.info(log_line)
        if req.method in ('PUT', 'DELETE'):
            slow = self.slow - trans_time
            if slow > 0:
                sleep(slow)
        return res(env, start_response)


def app_factory(global_conf, **local_conf):
    """paste.deploy app factory for creating WSGI object server apps"""
    conf = global_conf.copy()
    conf.update(local_conf)
    return ObjectController(conf)
