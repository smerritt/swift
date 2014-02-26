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

from swift.container import server as container_server
from swift.container.backend import ContainerBroker
from swift.common import db_replicator
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
