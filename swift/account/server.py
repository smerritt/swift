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
from swift import gettext_ as _

from eventlet import Timeout

import swift.common.db
from swift.account.backend import AccountBroker
from swift.account.utils import account_listing_response
from swift.common.db import DatabaseConnectionError, DatabaseAlreadyExists
from swift.common.request_helpers import get_param, get_listing_content_type, \
    split_and_validate_path, get_hash_algorithm
from swift.common.utils import get_logger, public, config_true_value, \
    json, timing_stats, replication
from swift.common.ondisk import hash_path, normalize_timestamp, \
    storage_directory
from swift.common.constraints import ACCOUNT_LISTING_LIMIT, \
    check_mount, check_float, check_utf8
from swift.common.db_replicator import ReplicatorRpc
from swift.common.swob import HTTPAccepted, HTTPBadRequest, \
    HTTPCreated, HTTPForbidden, HTTPInternalServerError, \
    HTTPMethodNotAllowed, HTTPNoContent, HTTPNotFound, \
    HTTPPreconditionFailed, HTTPConflict, Request, \
    HTTPInsufficientStorage, HTTPException


DATADIR = 'accounts'


class AccountController(object):
    """WSGI controller for the account server."""

    def __init__(self, conf):
        self.logger = get_logger(conf, log_route='account-server')
        self.root = conf.get('devices', '/srv/node')
        self.mount_check = config_true_value(conf.get('mount_check', 'true'))
        replication_server = conf.get('replication_server', None)
        if replication_server is not None:
            replication_server = config_true_value(replication_server)
        self.replication_server = replication_server
        self.replicator_rpc = ReplicatorRpc(self.root, DATADIR, AccountBroker,
                                            self.mount_check,
                                            logger=self.logger)
        self.auto_create_account_prefix = \
            conf.get('auto_create_account_prefix') or '.'
        swift.common.db.DB_PREALLOCATION = \
            config_true_value(conf.get('db_preallocation', 'f'))

    def _get_account_broker(self, drive, part, account, hash_algorithm,
                            **kwargs):
        hsh = hash_path(account, hash_algorithm=hash_algorithm)
        db_dir = storage_directory(DATADIR, part, hsh)
        db_path = os.path.join(self.root, drive, db_dir, hsh + '.db')
        kwargs.setdefault('account', account)
        kwargs.setdefault('logger', self.logger)
        return AccountBroker(db_path, **kwargs)

    def _deleted_response(self, broker, req, resp, body=''):
        # We are here since either the account does not exist or
        # it exists but marked for deletion.
        headers = {}
        # Try to check if account exists and is marked for deletion
        try:
            if broker.is_status_deleted():
                # Account does exist and is marked for deletion
                headers = {'X-Account-Status': 'Deleted'}
        except DatabaseConnectionError:
            # Account does not exist!
            pass
        return resp(request=req, headers=headers, charset='utf-8', body=body)

    @public
    @timing_stats()
    def DELETE(self, req):
        """Handle HTTP DELETE request."""
        drive, part, account = split_and_validate_path(req, 3)
        if self.mount_check and not check_mount(self.root, drive):
            return HTTPInsufficientStorage(drive=drive, request=req)
        if 'x-timestamp' not in req.headers or \
                not check_float(req.headers['x-timestamp']):
            return HTTPBadRequest(body='Missing timestamp', request=req,
                                  content_type='text/plain')
        broker = self._get_account_broker(
            drive, part, account,
            hash_algorithm=get_hash_algorithm(req))
        if broker.is_deleted():
            return self._deleted_response(broker, req, HTTPNotFound)
        broker.delete_db(req.headers['x-timestamp'])
        return self._deleted_response(broker, req, HTTPNoContent)

    @public
    @timing_stats()
    def PUT(self, req):
        """Handle HTTP PUT request."""
        drive, part, account, container = split_and_validate_path(req, 3, 4)
        if self.mount_check and not check_mount(self.root, drive):
            return HTTPInsufficientStorage(drive=drive, request=req)
        if container:   # put account container
            pending_timeout = None
            if 'x-trans-id' in req.headers:
                pending_timeout = 3
            broker = self._get_account_broker(
                drive, part, account,
                pending_timeout=pending_timeout,
                hash_algorithm=get_hash_algorithm(req))
            if account.startswith(self.auto_create_account_prefix) and \
                    not os.path.exists(broker.db_file):
                try:
                    broker.initialize(normalize_timestamp(
                        req.headers.get('x-timestamp') or time.time()))
                except DatabaseAlreadyExists:
                    pass
            if req.headers.get('x-account-override-deleted', 'no').lower() != \
                    'yes' and broker.is_deleted():
                return HTTPNotFound(request=req)
            broker.put_container(container, req.headers['x-put-timestamp'],
                                 req.headers['x-delete-timestamp'],
                                 req.headers['x-object-count'],
                                 req.headers['x-bytes-used'])
            if req.headers['x-delete-timestamp'] > \
                    req.headers['x-put-timestamp']:
                return HTTPNoContent(request=req)
            else:
                return HTTPCreated(request=req)
        else:   # put account
            broker = self._get_account_broker(
                drive, part, account,
                hash_algorithm=get_hash_algorithm(req))
            timestamp = normalize_timestamp(req.headers['x-timestamp'])
            if not os.path.exists(broker.db_file):
                try:
                    broker.initialize(timestamp)
                    created = True
                except DatabaseAlreadyExists:
                    pass
            elif broker.is_status_deleted():
                return self._deleted_response(broker, req, HTTPForbidden,
                                              body='Recently deleted')
            else:
                created = broker.is_deleted()
                broker.update_put_timestamp(timestamp)
                if broker.is_deleted():
                    return HTTPConflict(request=req)
            metadata = {}
            metadata.update((key, (value, timestamp))
                            for key, value in req.headers.iteritems()
                            if key.lower().startswith('x-account-meta-'))
            if metadata:
                broker.update_metadata(metadata)
            if created:
                return HTTPCreated(request=req)
            else:
                return HTTPAccepted(request=req)

    @public
    @timing_stats()
    def HEAD(self, req):
        """Handle HTTP HEAD request."""
        drive, part, account = split_and_validate_path(req, 3)
        out_content_type = get_listing_content_type(req)
        if self.mount_check and not check_mount(self.root, drive):
            return HTTPInsufficientStorage(drive=drive, request=req)
        broker = self._get_account_broker(
            drive, part, account,
            pending_timeout=0.1, stale_reads_ok=True,
            hash_algorithm=get_hash_algorithm(req))
        if broker.is_deleted():
            return self._deleted_response(broker, req, HTTPNotFound)
        info = broker.get_info()
        headers = {
            'X-Account-Container-Count': info['container_count'],
            'X-Account-Object-Count': info['object_count'],
            'X-Account-Bytes-Used': info['bytes_used'],
            'X-Timestamp': info['created_at'],
            'X-PUT-Timestamp': info['put_timestamp']}
        headers.update((key, value)
                       for key, (value, timestamp) in
                       broker.metadata.iteritems() if value != '')
        headers['Content-Type'] = out_content_type
        return HTTPNoContent(request=req, headers=headers, charset='utf-8')

    @public
    @timing_stats()
    def GET(self, req):
        """Handle HTTP GET request."""
        drive, part, account = split_and_validate_path(req, 3)
        prefix = get_param(req, 'prefix')
        delimiter = get_param(req, 'delimiter')
        if delimiter and (len(delimiter) > 1 or ord(delimiter) > 254):
            # delimiters can be made more flexible later
            return HTTPPreconditionFailed(body='Bad delimiter')
        limit = ACCOUNT_LISTING_LIMIT
        given_limit = get_param(req, 'limit')
        if given_limit and given_limit.isdigit():
            limit = int(given_limit)
            if limit > ACCOUNT_LISTING_LIMIT:
                return HTTPPreconditionFailed(request=req,
                                              body='Maximum limit is %d' %
                                              ACCOUNT_LISTING_LIMIT)
        marker = get_param(req, 'marker', '')
        end_marker = get_param(req, 'end_marker')
        out_content_type = get_listing_content_type(req)

        if self.mount_check and not check_mount(self.root, drive):
            return HTTPInsufficientStorage(drive=drive, request=req)
        broker = self._get_account_broker(
            drive, part, account,
            pending_timeout=0.1, stale_reads_ok=True,
            hash_algorithm=get_hash_algorithm(req))
        if broker.is_deleted():
            return self._deleted_response(broker, req, HTTPNotFound)
        return account_listing_response(account, req, out_content_type, broker,
                                        limit, marker, end_marker, prefix,
                                        delimiter)

    @public
    @replication
    @timing_stats()
    def REPLICATE(self, req):
        """
        Handle HTTP REPLICATE request.
        Handler for RPC calls for account replication.
        """
        post_args = split_and_validate_path(req, 3)
        drive, partition, hash = post_args
        if self.mount_check and not check_mount(self.root, drive):
            return HTTPInsufficientStorage(drive=drive, request=req)
        try:
            args = json.load(req.environ['wsgi.input'])
        except ValueError as err:
            return HTTPBadRequest(body=str(err), content_type='text/plain')
        ret = self.replicator_rpc.dispatch(post_args, args)
        ret.request = req
        return ret

    @public
    @timing_stats()
    def POST(self, req):
        """Handle HTTP POST request."""
        drive, part, account = split_and_validate_path(req, 3)
        if 'x-timestamp' not in req.headers or \
                not check_float(req.headers['x-timestamp']):
            return HTTPBadRequest(body='Missing or bad timestamp',
                                  request=req,
                                  content_type='text/plain')
        if self.mount_check and not check_mount(self.root, drive):
            return HTTPInsufficientStorage(drive=drive, request=req)
        broker = self._get_account_broker(
            drive, part, account,
            hash_algorithm=get_hash_algorithm(req))
        if broker.is_deleted():
            return self._deleted_response(broker, req, HTTPNotFound)
        timestamp = normalize_timestamp(req.headers['x-timestamp'])
        metadata = {}
        metadata.update((key, (value, timestamp))
                        for key, value in req.headers.iteritems()
                        if key.lower().startswith('x-account-meta-'))
        if metadata:
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
                # disallow methods which are not publicly accessible
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
                self.logger.exception(_('ERROR __call__ error with %(method)s'
                                        ' %(path)s '),
                                      {'method': req.method, 'path': req.path})
                res = HTTPInternalServerError(body=traceback.format_exc())
        trans_time = '%.4f' % (time.time() - start_time)
        additional_info = ''
        if res.headers.get('x-container-timestamp') is not None:
            additional_info += 'x-container-timestamp: %s' % \
                res.headers['x-container-timestamp']
        log_message = '%s - - [%s] "%s %s" %s %s "%s" "%s" "%s" %s "%s"' % (
            req.remote_addr,
            time.strftime('%d/%b/%Y:%H:%M:%S +0000', time.gmtime()),
            req.method, req.path,
            res.status.split()[0], res.content_length or '-',
            req.headers.get('x-trans-id', '-'),
            req.referer or '-', req.user_agent or '-',
            trans_time,
            additional_info)
        if req.method.upper() == 'REPLICATE':
            self.logger.debug(log_message)
        else:
            self.logger.info(log_message)
        return res(env, start_response)


def app_factory(global_conf, **local_conf):
    """paste.deploy app factory for creating WSGI account server apps"""
    conf = global_conf.copy()
    conf.update(local_conf)
    return AccountController(conf)
