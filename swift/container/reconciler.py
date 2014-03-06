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

from eventlet import GreenPile, GreenPool

from swift.common.daemon import Daemon
from swift.common.internal_client import InternalClient, UnexpectedResponse
from swift.common.utils import get_logger, split_path, quorum_size, \
    FileLikeIter, last_modified_date_to_timestamp
from swift.common.direct_client import direct_head_container, \
    direct_delete_container_object, ClientException


EPSILON = 0.000001


def slightly_later_timestamp(ts):
    return ts + EPSILON


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


def direct_get_oldest_storage_policy_index(container_ring, account_name,
                                           container_name):
    """
    Talk directly to the primary container servers to figure out the storage
    policy index for a given container. In case of disagreement, the oldest
    container is considered correct.

    :param container_ring: ring in which to look up the container locations
    :param account_name: name of the container's account
    :param container_name: name of the container
    :returns: storage policy index, or None if it couldn't get a quorum
    """
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


def direct_delete_container_entry(container_ring, account_name, container_name,
                                  object_name, headers=None):
    """
    Talk directly to the primary container servers to delete a particular
    object listing. Does not talk to object servers; use this only when a
    container entry does not actually have a corresponding object.
    """

    def _eat_client_exception(*args, **kwargs):
        try:
            return direct_delete_container_object(*args, **kwargs)
        except ClientException:
            pass

    pool = GreenPool()
    part, nodes = container_ring.get_nodes(account_name, container_name)
    for node in nodes:
        pool.spawn(_eat_client_exception, node, part, account_name,
                   container_name, headers=headers)

    # This either worked or it didn't; if it didn't, we'll retry on the next
    # reconciler loop when we see the queue entry again.
    pool.waitall()


class ContainerReconciler(Daemon):
    """
    Move objects that are in the wrong storage policy.
    """

    def __init__(self, conf):
        self.conf = conf
        self.reclaim_age = int(conf.get('reclaim_age', 86400 * 7))
        self.interval = int(conf.get('interval', 300))
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
            time.sleep(self.interval)

    def pop_queue(self, container, obj, q_timestamp):
        headers = {'X-Timestamp': slightly_later_timestamp(q_timestamp)}
        direct_delete_container_entry(
            self.swift.app.container_ring, self.misplaced_objects_account,
            container, obj, headers=headers)

    def throw_tombstones(self, account, container, obj, timestamp,
                         storage_policy_index):
        headers = {'X-Timestamp': timestamp,
                   'X-Override-Storage-Policy-Index': storage_policy_index}
        self.swift.delete_object(account, container, obj,
                                 headers=headers)

    def ensure_object_in_right_location(self, real_storage_policy_index,
                                        account, container, obj, q_timestamp):
        dest_storage_policy_index = direct_get_oldest_storage_policy_index(
            self.swift.app.container_ring, account, container)
        if dest_storage_policy_index == real_storage_policy_index:
            self.stats['noop_objects'] += 1
            return True

        # check if object exists in the destination already
        headers = {
            'X-Override-Storage-Policy-Index': dest_storage_policy_index}
        dest_obj = self.swift.get_object_metadata(account, container, obj,
                                                  headers=headers,
                                                  acceptable_statuses=(2, 4))
        dest_ts = float(dest_obj.get('x-timestamp', '0.0'))
        if dest_ts >= q_timestamp:
            self.stats['newer_objects'] += 1
            self.throw_tombstones(account, container, obj,
                                  slightly_later_timestamp(q_timestamp),
                                  real_storage_policy_index)
            return True

        # check if object is still available in the real
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
                self.stats['lost_object'] += 1
                return True
            else:
                # try again later
                self.stats['unavailable_object'] += 1
                return False
        real_ts = float(real_ts)

        # the source object is newer than the queue entry; do nothing
        if real_ts - q_timestamp >= EPSILON:
            self.stats['source_newer'] += 1
            return True
        elif q_timestamp - real_ts >= EPSILON:
            self.stats['unavailable_object'] += 1
            return False

        # move the object
        self.stats['misplaced_objects'] += 1
        headers = real_obj_info.copy()
        headers['X-Override-Storage-Policy-Index'] = dest_storage_policy_index

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

        self.throw_tombstones(account, container, obj,
                              slightly_later_timestamp(real_ts),
                              real_storage_policy_index)
        return True

    def run_once(self, *args, **kwargs):
        """
        Process every entry in the queue.
        """
        self.logger.debug('pulling item from the queue')
        for c in self.swift.iter_containers(self.misplaced_objects_account):
            container = c['name'].encode('utf8')  # encoding here is defensive
            for raw_obj in self.swift.iter_objects(
                    self.misplaced_objects_account, container):
                info = parse_raw_obj(raw_obj)
                handled_success = self.ensure_object_in_right_location(**info)
                if handled_success:
                    self.pop_queue(container, raw_obj['name'],
                                   info['q_timestamp'])
            # Try to delete the container so the queue doesn't grow without
            # bound. However, be okay if someone else has put things in or our
            # deletions haven't made it everywhere or something.
            self.swift.delete_container(
                self.misplaced_objects_account, container,
                acceptable_statuses=(2, 404, 409, 412))
