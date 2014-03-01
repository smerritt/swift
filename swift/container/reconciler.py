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

import time
from collections import defaultdict

from eventlet import GreenPile

from swift.common.daemon import Daemon
from swift.common.internal_client import InternalClient, UnexpectedResponse
from swift.common.utils import get_logger, split_path, quorum_size, \
    FileLikeIter, last_modified_date_to_timestamp
from swift.common.direct_client import direct_head_container, ClientException


def parse_raw_obj(obj_info):
    raw_obj_name = obj_info['name'].encode('utf-8')

    storage_policy_index, obj_name = raw_obj_name.split(':', 1)
    real_storage_policy_index = int(storage_policy_index)
    account, container, obj = split_path(obj_name, 3, 3, rest_with_last=True)
    return {
        'real_storage_policy_index': real_storage_policy_index,
        'account': account,
        'container': container,
        'obj': obj,
        'q_timestamp': last_modified_date_to_timestamp(
            obj_info['last_modified']),
    }


def get_oldest_storage_policy_index(container_ring, account_name,
                                    container_name):
    def _eat_client_exception(*args):
        try:
            return direct_head_container(*args)
        except ClientException:
            pass

    pile = GreenPile()
    part, nodes = container_ring.get_nodes(account_name, container_name)
    for node in nodes:
        pile.spawn(_eat_client_exception, node, part, account_name,
                   container_name)

    headers = [x for x in pile if x is not None]
    if len(headers) < quorum_size(len(nodes)):
        return
    headers.sort(key=lambda h: h['x-timestamp'])
    return int(headers[0]['x-storage-policy-index'])


class ContainerReconciler(Daemon):
    """Update container information in account listings."""

    def __init__(self, conf):
        self.conf = conf
        conf_path = conf.get('__file__') or \
            '/etc/swift/container-reconciler.conf'
        self.logger = get_logger(conf, log_route='container-reconciler')
        request_tries = int(conf.get('request_tries') or 3)
        self.misplaced_objects_account = '.misplaced_objects'
        self.swift = InternalClient(conf_path,
                                    'Swift Container Reconciler',
                                    request_tries)
        self.stats = defaultdict(int)

    def run_forever(self, *args, **kwargs):
        while True:
            self.run_once(*args, **kwargs)
            time.sleep(0.1)

    def pop_q(self, raw_obj):
        pass

    def throw_tombstones(self, account, container, obj, timestamp,
                         storage_policy_index):
        pass

    def ensure_object_in_right_location(self, real_storage_policy_index,
                                        account, container, obj, q_timestamp):
        dest_storage_policy_index = get_oldest_storage_policy_index(
            self.swift.app.container_ring, account, container)
        if dest_storage_policy_index == real_storage_policy_index:
            self.stats['correct_objects'] += 1
            return True

        # check if object exists in the destination already
        headers = {
            'X-Override-Storage-Policy-Index': dest_storage_policy_index}
        dest_obj = self.swift.get_object_metadata(account, container, obj,
                                                  headers=headers,
                                                  acceptable_statuses=(2, 4))
        dest_ts = float(dest_obj.get('X-Timestamp', '0'))
        if dest_ts >= q_timestamp:
            self.stats['newer_objects'] += 1
            self.throw_tombstones(account, container, obj, q_timestamp,
                                  real_storage_policy_index)
            return True

        # check if object is still available in the real
        self.stats['misplaced_objects'] += 1
        headers = {
            'X-Override-Storage-Policy-Index': real_storage_policy_index}
        real_obj_status, real_obj_info, real_obj_iter = \
            self.swift.get_object(account, container, obj,
                                  headers=headers,
                                  acceptable_statuses=(2, 4))
        real_ts = real_obj_info.get("X-Timestamp")
        if real_ts is None:
            if q_timestamp < time.time() - self.reclaim_age:
                # it's old and there are no tombstones or anything; give up
                return True
            else:
                # try again later
                return False
        real_ts = float(real_ts)

        # the object is newer than the queue entry; do nothing
        if real_ts > q_timestamp:
            return True

        # move the object
        headers = real_obj_info.copy()
        headers['X-Override-Storage-Policy-Index'] = real_storage_policy_index

        try:
            self.swift.upload_object(
                FileLikeIter(real_obj_iter), account, container, obj,
                headers=headers)
        except UnexpectedResponse as err:
            self.logger.warn("Failed to copy /%s/%s/%s to right place: %s",
                             account, container, obj, err)
            return False
        except Exception:
            self.stats['unhandled_errors'] += 1
            self.logger.exception("Unhandled error while copying /%s/%s/%s to "
                                  "the right place", account, container, obj)
            return False

        self.throw_tombstones(account, container, obj, q_timestamp,
                              real_storage_policy_index)
        return True

    def run_once(self, *args, **kwargs):
        """
        Process every entry in the queue.
        """
        self.logger.info('pulling item from the queue')
        for c in self.swift.iter_containers(self.misplaced_objects_account):
            container = c['name'].encode('utf8')  # encoding here is defensive
            for raw_obj in self.swift.iter_objects(
                    self.misplaced_objects_account, container):
                info = parse_raw_obj(raw_obj)
                handled_success = self.ensure_object_in_right_location(**info)
                if handled_success:
                    self.pop_q(raw_obj)
