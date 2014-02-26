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
from swift.container import server as container_server
from swift.container.backend import ContainerBroker
from swift.container.reconciler import add_to_reconciler_queue, \
    slightly_later_timestamp
from swift.common import db_replicator
from swift.common.http import HTTP_CONFLICT
from swift.common.storage_policy import POLICIES


class ContainerReplicator(db_replicator.Replicator):
    server_type = 'container'
    brokerclass = ContainerBroker
    datadir = container_server.DATADIR
    default_port = 6001

    def report_up_to_date(self, full_info):
        for key in ('put_timestamp', 'delete_timestamp', 'object_count',
                    'bytes_used'):
            if full_info['reported_' + key] != full_info[key]:
                return False
        return True

    def sync_args_from_replication_info(self, replication_info):
        sync_args = super(ContainerReplicator, self).\
            sync_args_from_replication_info(replication_info)
        # If we only have one storage policy, we're in a cluster that isn't
        # using storage policies yet, so we don't send our policy index. The
        # other end is either running new code that will infer a 0 from the
        # policy index's absence *or* is running old code that will blow up if
        # fed a policy index.
        #
        # The point is to keep container replication working during an upgrade
        # from pre-storage-policy code to post-storage-policy code.
        if len(POLICIES) > 1:
            sync_args.append(replication_info['storage_policy_index'])
        return sync_args

    def _handle_sync_response(self, node, response, info, broker, http):
        """
        Handle a sync response from a remote node. If this container needs to
        change its storage policy index, do so. Otherwise, send the necessary
        rows to the remote node.
        """
        if response.status == HTTP_CONFLICT:  # storage policy mismatch
            rinfo = json.loads(response.data)
            local_created_at = info['created_at']
            remote_created_at = rinfo['created_at']

            # The oldest policy index wins
            if local_created_at > remote_created_at:
                local_storage_policy_index = info['storage_policy_index']
                remote_storage_policy_index = rinfo['storage_policy_index']

                # The local DB has the newer policy index, so it loses.
                success = self._drain_into_reconciler_queue(
                    broker, local_storage_policy_index)
                if not success:
                    return False
                broker.set_storage_policy_index(remote_storage_policy_index)
            else:
                # The local DB has the newer policy index, so it wins.
                # Ideally, we'd tell the remote container to change its policy
                # index right here, but that can take a long time if the
                # remote container has a lot of objects in it, and we don't
                # want to do that as part of an HTTP RPC call, so we punt to
                # the remote container's replicator.
                pass
            return True

        return super(ContainerReplicator, self)._handle_sync_response(
            node, response, info, broker, http)

    def _drain_into_reconciler_queue(self, broker, local_storage_policy_index):
        objects = broker.list_objects_iter(
            limit=100, marker=None, end_marker=None,
            prefix=None, delimiter=None)
        while objects:
            for obj in objects:
                obj_name = obj[0]
                obj_ts = obj[1]
                added = add_to_reconciler_queue(
                    self.ring, broker.account, broker.container, obj_name,
                    obj_ts, local_storage_policy_index)
                if not added:
                    return False
                broker.delete_object(
                    obj_name,
                    slightly_later_timestamp(obj_ts))
            objects = broker.list_objects_iter(
                limit=100, marker=None, end_marker=None,
                prefix=None, delimiter=None)
        return True
