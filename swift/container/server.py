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

from __future__ import with_statement

import os
import time
import traceback
from datetime import datetime
from gettext import gettext as _
from xml.etree.cElementTree import Element, SubElement, tostring

from eventlet import Timeout

import swift.common.db
import swift.common.storage_policy
from swift.common.db import ContainerBroker
from swift.common.request_helpers import get_param
from swift.common.utils import get_logger, hash_path, public, \
    normalize_timestamp, storage_directory, validate_sync_to, \
    config_true_value, validate_device_partition, json, timing_stats, \
    replication, parse_content_type
from swift.common.constraints import CONTAINER_LISTING_LIMIT, \
    check_mount, check_float, check_utf8, FORMAT2CONTENT_TYPE
from swift.common.bufferedhttp import http_connect
from swift.common.exceptions import ConnectionTimeout
from swift.common.db_replicator import ReplicatorRpc
from swift.common.http import HTTP_NOT_FOUND, is_success
from swift.common.swob import HTTPAccepted, HTTPBadRequest, HTTPConflict, \
    HTTPCreated, HTTPInternalServerError, HTTPNoContent, HTTPNotFound, \
    HTTPPreconditionFailed, HTTPMethodNotAllowed, Request, Response, \
    HTTPInsufficientStorage, HTTPNotAcceptable, HTTPException, HeaderKeyDict
from swift.proxy.controllers.base import POLICY_INDEX, POLICY

DATADIR = 'containers'


class ContainerController(object):
    """WSGI Controller for the container server."""

    # Ensure these are all lowercase
    save_headers = ['x-container-read', 'x-container-write',
                    'x-container-sync-key', 'x-container-sync-to',
                    POLICY_INDEX.lower()]

    def __init__(self, conf, storage_policies=None):
        self.logger = get_logger(conf, log_route='container-server')
        self.root = conf.get('devices', '/srv/node/')
        self.mount_check = config_true_value(conf.get('mount_check', 'true'))
        self.node_timeout = int(conf.get('node_timeout', 3))
        self.conn_timeout = float(conf.get('conn_timeout', 0.5))
        replication_server = conf.get('replication_server', None)
        if replication_server is not None:
            replication_server = config_true_value(replication_server)
        self.replication_server = replication_server
        self.allowed_sync_hosts = [
            h.strip()
            for h in conf.get('allowed_sync_hosts', '127.0.0.1').split(',')
            if h.strip()]
        self.replicator_rpc = ReplicatorRpc(
            self.root, DATADIR, ContainerBroker, self.mount_check,
            logger=self.logger)
        self.auto_create_account_prefix = \
            conf.get('auto_create_account_prefix') or '.'
        if config_true_value(conf.get('allow_versions', 'f')):
            self.save_headers.append('x-versions-location')
        swift.common.db.DB_PREALLOCATION = \
            config_true_value(conf.get('db_preallocation', 'f'))
        self.policies = storage_policies or swift.common.storage_policy

    def _get_container_broker(self, drive, part, account, container, **kwargs):
        """
        Get a DB broker for the container.

        :param drive: drive that holds the container
        :param part: partition the container is in
        :param account: account name
        :param container: container name
        :returns: ContainerBroker object
        """
        hsh = hash_path(account, container)
        db_dir = storage_directory(DATADIR, part, hsh)
        db_path = os.path.join(self.root, drive, db_dir, hsh + '.db')
        kwargs.setdefault('account', account)
        kwargs.setdefault('container', container)
        kwargs.setdefault('logger', self.logger)
        return ContainerBroker(db_path, **kwargs)

    # XXX FIXME
    def validate_policy_in_header(self, req):
        if POLICY in req.headers:
            """
            validate and convert given policy name to index to be
            saved with container metadata
            """
            pol = self.policies.get_by_name(req.headers[POLICY])
            if pol:
                req.headers.update({POLICY_INDEX: str(pol.idx)})
            else:
                return False
        return True

    def account_update(self, req, account, container, broker):
        """
        Update the account server(s) with latest container info.

        :param req: swob.Request object
        :param account: account name
        :param container: container name
        :param broker: container DB broker object
        :returns: if all the account requests return a 404 error code,
                  HTTPNotFound response object,
                  if the account cannot be updated due to a malformed header,
                  an HTTPBadRequest response object,
                  otherwise None.
        """
        account_hosts = [h.strip() for h in
                         req.headers.get('X-Account-Host', '').split(',')]
        account_devices = [d.strip() for d in
                           req.headers.get('X-Account-Device', '').split(',')]
        account_partition = req.headers.get('X-Account-Partition', '')

        if len(account_hosts) != len(account_devices):
            # This shouldn't happen unless there's a bug in the proxy,
            # but if there is, we want to know about it.
            self.logger.error(_('ERROR Account update failed: different  '
                                'numbers of hosts and devices in request: '
                                '"%s" vs "%s"' %
                                (req.headers.get('X-Account-Host', ''),
                                 req.headers.get('X-Account-Device', ''))))
            return HTTPBadRequest(req=req)

        if account_partition:
            updates = zip(account_hosts, account_devices)
        else:
            updates = []

        account_404s = 0

        for account_host, account_device in updates:
            account_ip, account_port = account_host.rsplit(':', 1)
            new_path = '/' + '/'.join([account, container])
            info = broker.get_info()
            account_headers = HeaderKeyDict({
                'x-put-timestamp': info['put_timestamp'],
                'x-delete-timestamp': info['delete_timestamp'],
                'x-object-count': info['object_count'],
                'x-bytes-used': info['bytes_used'],
                'x-trans-id': req.headers.get('x-trans-id', '-'),
                'user-agent': 'container-server %s' % os.getpid(),
                'referer': req.as_referer()})
            if req.headers.get('x-account-override-deleted', 'no').lower() == \
                    'yes':
                account_headers['x-account-override-deleted'] = 'yes'
            try:
                with ConnectionTimeout(self.conn_timeout):
                    conn = http_connect(
                        account_ip, account_port, account_device,
                        account_partition, 'PUT', new_path, account_headers)
                with Timeout(self.node_timeout):
                    account_response = conn.getresponse()
                    account_response.read()
                    if account_response.status == HTTP_NOT_FOUND:
                        account_404s += 1
                    elif not is_success(account_response.status):
                        self.logger.error(_(
                            'ERROR Account update failed '
                            'with %(ip)s:%(port)s/%(device)s (will retry '
                            'later): Response %(status)s %(reason)s'),
                            {'ip': account_ip, 'port': account_port,
                             'device': account_device,
                             'status': account_response.status,
                             'reason': account_response.reason})
            except (Exception, Timeout):
                self.logger.exception(_(
                    'ERROR account update failed with '
                    '%(ip)s:%(port)s/%(device)s (will retry later)'),
                    {'ip': account_ip, 'port': account_port,
                     'device': account_device})
        if updates and account_404s == len(updates):
            return HTTPNotFound(req=req)
        else:
            return None

    @public
    @timing_stats()
    def DELETE(self, req):
        """Handle HTTP DELETE request."""
        try:
            drive, part, account, container, obj = req.split_path(4, 5, True)
            validate_device_partition(drive, part)
        except ValueError, err:
            return HTTPBadRequest(body=str(err), content_type='text/plain',
                                  request=req)
        if 'x-timestamp' not in req.headers or \
                not check_float(req.headers['x-timestamp']):
            return HTTPBadRequest(body='Missing timestamp', request=req,
                                  content_type='text/plain')
        if self.mount_check and not check_mount(self.root, drive):
            return HTTPInsufficientStorage(drive=drive, request=req)
        broker = self._get_container_broker(drive, part, account, container)
        if account.startswith(self.auto_create_account_prefix) and obj and \
                not os.path.exists(broker.db_file):
            try:
                broker.initialize(normalize_timestamp(
                    req.headers.get('x-timestamp') or time.time()))
            except swift.common.db.DatabaseAlreadyExists:
                pass
        if not os.path.exists(broker.db_file):
            return HTTPNotFound()
        if obj:     # delete object
            broker.delete_object(obj, req.headers.get('x-timestamp'))
            return HTTPNoContent(request=req)
        else:
            # delete container
            if not broker.empty():
                return HTTPConflict(request=req)
            existed = float(broker.get_info()['put_timestamp']) and \
                not broker.is_deleted()
            broker.delete_db(req.headers['X-Timestamp'])
            if not broker.is_deleted():
                return HTTPConflict(request=req)
            resp = self.account_update(req, account, container, broker)
            if resp:
                return resp
            if existed:
                return HTTPNoContent(request=req)
            return HTTPNotFound()

    @public
    @timing_stats()
    def PUT(self, req):
        """Handle HTTP PUT request."""
        try:
            drive, part, account, container, obj = req.split_path(4, 5, True)
            validate_device_partition(drive, part)
        except ValueError, err:
            return HTTPBadRequest(body=str(err), content_type='text/plain',
                                  request=req)
        if 'x-timestamp' not in req.headers or \
                not check_float(req.headers['x-timestamp']):
            return HTTPBadRequest(body='Missing timestamp', request=req,
                                  content_type='text/plain')
        if 'x-container-sync-to' in req.headers:
            err = validate_sync_to(req.headers['x-container-sync-to'],
                                   self.allowed_sync_hosts)
            if err:
                return HTTPBadRequest(err)
        if self.mount_check and not check_mount(self.root, drive):
            return HTTPInsufficientStorage(drive=drive, request=req)
        if not self.validate_policy_in_header(req):
            return HTTPBadRequest(
                request=req,
                content_type="text/plain",
                body=("Invalid X-Storage-Policy %s" %
                      req.headers[POLICY]))
        timestamp = normalize_timestamp(req.headers['x-timestamp'])
        broker = self._get_container_broker(drive, part, account, container)
        if obj:     # put container object
            if account.startswith(self.auto_create_account_prefix) and \
                    not os.path.exists(broker.db_file):
                try:
                    broker.initialize(timestamp)
                except swift.common.db.DatabaseAlreadyExists:
                    pass
            if not os.path.exists(broker.db_file):
                return HTTPNotFound()
            broker.put_object(obj, timestamp, int(req.headers['x-size']),
                              req.headers['x-content-type'],
                              req.headers['x-etag'])
            return HTTPCreated(request=req)
        else:   # put container
            if not os.path.exists(broker.db_file):
                try:
                    broker.initialize(timestamp)
                    created = True
                except swift.common.db.DatabaseAlreadyExists:
                    pass
            else:
                created = broker.is_deleted()
                broker.update_put_timestamp(timestamp)
                if broker.is_deleted():
                    return HTTPConflict(request=req)
            metadata = {}
            metadata.update(
                (key, (value, timestamp))
                for key, value in req.headers.iteritems()
                if key.lower() in self.save_headers or
                key.lower().startswith('x-container-meta-'))
            if POLICY_INDEX in metadata:
                """ policy specified and pre-validated """
                requested_policy_index = metadata[POLICY_INDEX][0]
                current_policy_index = broker.metadata.get(POLICY_INDEX,
                                                           (None, None))[0]
                if ((current_policy_index is not None) and
                        (requested_policy_index != current_policy_index)):
                    # Modifying a storage policy is prohibited
                    return HTTPConflict(request=req)
                elif POLICY_INDEX not in broker.metadata:
                    metadata[POLICY_INDEX] = (requested_policy_index,
                                              timestamp)
            elif created:
                # new container, no policy specified: add the default
                metadata[POLICY_INDEX] = (self.policies.get_default().idx,
                                          timestamp)
            elif broker.metadata.get(POLICY_INDEX, (None, None))[0] is None:
                # existing container, no policy specified: add policy 0
                metadata[POLICY_INDEX] = (0, timestamp)
            if 'X-Container-Sync-To' in metadata:
                if 'X-Container-Sync-To' not in broker.metadata or \
                        metadata['X-Container-Sync-To'][0] != \
                        broker.metadata['X-Container-Sync-To'][0]:
                    broker.set_x_container_sync_points(-1, -1)
            broker.update_metadata(metadata)
            resp = self.account_update(req, account, container, broker)
            if resp:
                return resp
            if created:
                return HTTPCreated(request=req)
            else:
                return HTTPAccepted(request=req)

    @public
    @timing_stats(sample_rate=0.1)
    def HEAD(self, req):
        """Handle HTTP HEAD request."""
        try:
            drive, part, account, container, obj = req.split_path(4, 5, True)
            validate_device_partition(drive, part)
        except ValueError, err:
            return HTTPBadRequest(body=str(err), content_type='text/plain',
                                  request=req)
        query_format = get_param(req, 'format')
        if query_format:
            req.accept = FORMAT2CONTENT_TYPE.get(
                query_format.lower(), FORMAT2CONTENT_TYPE['plain'])
        out_content_type = req.accept.best_match(
            ['text/plain', 'application/json', 'application/xml', 'text/xml'])
        if not out_content_type:
            return HTTPNotAcceptable(request=req)
        if self.mount_check and not check_mount(self.root, drive):
            return HTTPInsufficientStorage(drive=drive, request=req)
        broker = self._get_container_broker(drive, part, account, container,
                                            pending_timeout=0.1,
                                            stale_reads_ok=True)
        if broker.is_deleted():
            return HTTPNotFound(request=req)
        info = broker.get_info()
        headers = {
            'X-Container-Object-Count': info['object_count'],
            'X-Container-Bytes-Used': info['bytes_used'],
            'X-Timestamp': info['created_at'],
            'X-PUT-Timestamp': info['put_timestamp'],
        }
        headers.update(
            (key, value)
            for key, (value, timestamp) in broker.metadata.iteritems()
            if value != '' and (key.lower() in self.save_headers or
                                key.lower().startswith('x-container-meta-')))
        headers['Content-Type'] = out_content_type
        return HTTPNoContent(request=req, headers=headers, charset='utf-8')

    def update_data_record(self, record):
        """
        Perform any mutations to container listing records that are common to
        all serialization formats, and returns it as a dict.

        Converts created time to iso timestamp.
        Replaces size with 'swift_bytes' content type parameter.

        :params record: object entry record
        :returns: modified record
        """
        (name, created, size, content_type, etag) = record
        if content_type is None:
            return {'subdir': name}
        response = {'bytes': size, 'hash': etag, 'name': name}
        last_modified = datetime.utcfromtimestamp(float(created)).isoformat()
        # python isoformat() doesn't include msecs when zero
        if len(last_modified) < len("1970-01-01T00:00:00.000000"):
            last_modified += ".000000"
        response['last_modified'] = last_modified + 'Z'
        content_type, params = parse_content_type(content_type)
        for key, value in params:
            if key == 'swift_bytes':
                try:
                    response['bytes'] = int(value)
                except ValueError:
                    self.logger.exception("Invalid swift_bytes")
            else:
                content_type += ';%s=%s' % (key, value)
        response['content_type'] = content_type
        return response

    @public
    @timing_stats()
    def GET(self, req):
        """Handle HTTP GET request."""
        try:
            drive, part, account, container, obj = req.split_path(4, 5, True)
            validate_device_partition(drive, part)
        except ValueError, err:
            return HTTPBadRequest(body=str(err), content_type='text/plain',
                                  request=req)
        path = get_param(req, 'path')
        prefix = get_param(req, 'prefix')
        delimiter = get_param(req, 'delimiter')
        if delimiter and (len(delimiter) > 1 or ord(delimiter) > 254):
            # delimiters can be made more flexible later
            return HTTPPreconditionFailed(body='Bad delimiter')
        marker = get_param(req, 'marker', '')
        end_marker = get_param(req, 'end_marker')
        limit = CONTAINER_LISTING_LIMIT
        given_limit = get_param(req, 'limit')
        if given_limit and given_limit.isdigit():
            limit = int(given_limit)
            if limit > CONTAINER_LISTING_LIMIT:
                return HTTPPreconditionFailed(
                    request=req,
                    body='Maximum limit is %d' % CONTAINER_LISTING_LIMIT)
        query_format = get_param(req, 'format')
        if query_format:
            req.accept = FORMAT2CONTENT_TYPE.get(query_format.lower(),
                                                 FORMAT2CONTENT_TYPE['plain'])
        out_content_type = req.accept.best_match(
            ['text/plain', 'application/json', 'application/xml', 'text/xml'])
        if not out_content_type:
            return HTTPNotAcceptable(request=req)
        if self.mount_check and not check_mount(self.root, drive):
            return HTTPInsufficientStorage(drive=drive, request=req)
        broker = self._get_container_broker(drive, part, account, container,
                                            pending_timeout=0.1,
                                            stale_reads_ok=True)
        if broker.is_deleted():
            return HTTPNotFound(request=req)
        info = broker.get_info()
        resp_headers = {
            'X-Container-Object-Count': info['object_count'],
            'X-Container-Bytes-Used': info['bytes_used'],
            'X-Timestamp': info['created_at'],
            'X-PUT-Timestamp': info['put_timestamp'],
        }
        for key, (value, timestamp) in broker.metadata.iteritems():
            if value and (key.lower() in self.save_headers or
                          key.lower().startswith('x-container-meta-')):
                resp_headers[key] = value
        ret = Response(request=req, headers=resp_headers,
                       content_type=out_content_type, charset='utf-8')
        container_list = broker.list_objects_iter(limit, marker, end_marker,
                                                  prefix, delimiter, path)
        if out_content_type == 'application/json':
            ret.body = json.dumps([self.update_data_record(record)
                                   for record in container_list])
        elif out_content_type.endswith('/xml'):
            doc = Element('container', name=container.decode('utf-8'))
            for obj in container_list:
                record = self.update_data_record(obj)
                if 'subdir' in record:
                    name = record['subdir'].decode('utf-8')
                    sub = SubElement(doc, 'subdir', name=name)
                    SubElement(sub, 'name').text = name
                else:
                    obj_element = SubElement(doc, 'object')
                    for field in ["name", "hash", "bytes", "content_type",
                                  "last_modified"]:
                        SubElement(obj_element, field).text = str(
                            record.pop(field)).decode('utf-8')
                    for field in sorted(record.keys()):
                        SubElement(obj_element, field).text = str(
                            record[field]).decode('utf-8')
            ret.body = tostring(doc, encoding='UTF-8')
        else:
            if not container_list:
                return HTTPNoContent(request=req, headers=resp_headers)
            ret.body = '\n'.join(rec[0] for rec in container_list) + '\n'
        return ret

    @public
    @replication
    @timing_stats(sample_rate=0.01)
    def REPLICATE(self, req):
        """
        Handle HTTP REPLICATE request (json-encoded RPC calls for replication.)
        """
        try:
            post_args = req.split_path(3)
            drive, partition, hash = post_args
            validate_device_partition(drive, partition)
        except ValueError, err:
            return HTTPBadRequest(body=str(err), content_type='text/plain',
                                  request=req)
        if self.mount_check and not check_mount(self.root, drive):
            return HTTPInsufficientStorage(drive=drive, request=req)
        try:
            args = json.load(req.environ['wsgi.input'])
        except ValueError, err:
            return HTTPBadRequest(body=str(err), content_type='text/plain')
        ret = self.replicator_rpc.dispatch(post_args, args)
        ret.request = req
        return ret

    @public
    @timing_stats()
    def POST(self, req):
        """Handle HTTP POST request."""
        try:
            drive, part, account, container = req.split_path(4)
            validate_device_partition(drive, part)
        except ValueError, err:
            return HTTPBadRequest(body=str(err), content_type='text/plain',
                                  request=req)
        if 'x-timestamp' not in req.headers or \
                not check_float(req.headers['x-timestamp']):
            return HTTPBadRequest(body='Missing or bad timestamp',
                                  request=req, content_type='text/plain')
        if 'x-container-sync-to' in req.headers:
            err = validate_sync_to(req.headers['x-container-sync-to'],
                                   self.allowed_sync_hosts)
            if err:
                return HTTPBadRequest(err)
        if self.mount_check and not check_mount(self.root, drive):
            return HTTPInsufficientStorage(drive=drive, request=req)
        broker = self._get_container_broker(drive, part, account, container)
        if broker.is_deleted():
            return HTTPNotFound(request=req)
        if not self.validate_policy_in_header(req):
            return HTTPBadRequest(
                request=req,
                content_type="text/plain",
                body=("Invalid X--Storage-Policy %s" %
                      req.headers[POLICY]))
        timestamp = normalize_timestamp(req.headers['x-timestamp'])
        metadata = {}
        metadata.update(
            (key, (value, timestamp)) for key, value in req.headers.iteritems()
            if key.lower() in self.save_headers or
            key.lower().startswith('x-container-meta-'))
        if metadata:
            """ make sure the policy is not being updated """
            if POLICY_INDEX in metadata and POLICY_INDEX in broker.metadata:
                if metadata[POLICY_INDEX][0] != \
                   broker.metadata[POLICY_INDEX][0]:
                    """ policy updates prohibited """
                    return HTTPConflict(request=req)
            if 'X-Container-Sync-To' in metadata:
                if 'X-Container-Sync-To' not in broker.metadata or \
                        metadata['X-Container-Sync-To'][0] != \
                        broker.metadata['X-Container-Sync-To'][0]:
                    broker.set_x_container_sync_points(-1, -1)
            broker.update_metadata(metadata)
        return HTTPNoContent(request=req)

    def __call__(self, env, start_response):
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
            except HTTPException as error_response:
                res = error_response
            except (Exception, Timeout):
                self.logger.exception(_(
                    'ERROR __call__ error with %(method)s %(path)s '),
                    {'method': req.method, 'path': req.path})
                res = HTTPInternalServerError(body=traceback.format_exc())
        trans_time = '%.4f' % (time.time() - start_time)
        log_message = '%s - - [%s] "%s %s" %s %s "%s" "%s" "%s" %s' % (
            req.remote_addr,
            time.strftime('%d/%b/%Y:%H:%M:%S +0000',
                          time.gmtime()),
            req.method, req.path,
            res.status.split()[0], res.content_length or '-',
            req.headers.get('x-trans-id', '-'),
            req.referer or '-', req.user_agent or '-',
            trans_time)
        if req.method.upper() == 'REPLICATE':
            self.logger.debug(log_message)
        else:
            self.logger.info(log_message)
        return res(env, start_response)


def app_factory(global_conf, **local_conf):
    """paste.deploy app factory for creating WSGI container server apps"""
    conf = global_conf.copy()
    conf.update(local_conf)
    return ContainerController(conf)
