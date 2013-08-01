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

from ConfigParser import NoOptionError, ConfigParser
from swift.common.utils import config_true_value


class StoragePolicy(object):
    """
    Represents a storage policy.
    Not meant to be instantiated directly; use get_stor_pols().
    """
    def __init__(self, idx, name, is_default=False, object_ring=None):
        self.name = name
        self.idx = idx
        self.is_default = is_default
        if self.idx == '0':
            ring_name = "object"
            data_dir = "objects"
        else:
            ring_name = "object-" + self.idx
            data_dir = "objects-" + self.idx
        self.ring_name = ring_name
        self.object_ring = object_ring
        self.data_dir = data_dir


class StoragePolicyCollection(object):
    """
    This class reppresents the collection of valid storage policies for
    the cluster and instantiated when swift.conf is parsed; as policy
    objects (StoragePolicy) are added to the collection it assures that
    only one is specified as the default.  It also provides various
    accessor functions for the rest of the code.  Note:
    default:  means that the policy is used when creating a new container
              and no policy was explicitly specified
    0 policy: is the policy that is used when accessing a container where
              no policy was identified in the container metadata
    """
    def __init__(self, pols):
        self.pols = dict((pol.idx, pol) for pol in pols)
        defaults = [pol for pol in pols if pol.is_default]
        if len(defaults) > 1:
            msg = "Too many default storage policies: %s" % \
                (", ".join((pol.name for pol in defaults)))
            raise ValueError(msg)
        self.default = defaults[0]
        """ also save a 'by name' format for quicker name lookups """
        self.pols_by_name = dict((pol.name, pol) for pol in pols)

    def __len__(self):
        return len(self.pols)

    def __getitem__(self, key):
        return self.pols[key]

    def get(self, key):
        return self.pols.get(self.policy_to_index(key))

    def get_def_policy(self):
        return self.default

    def index_to_policy(self, index):
        if index is None:
            index = '0'
        try:
            policy = self.pols[index]
        except KeyError:
            raise ValueError("Fatal: no policy found")
        return policy

    def get_policy_0(self):
        return self.index_to_policy('0')

    def policy_to_index(self, name):
        if name is "":
            return '0'
        try:
            policy = self.pols_by_name[name]
        except KeyError:
            return None
        return policy.idx


def parse_storage_policies(conf):
    """
    Parse storage policies in swift.conf making sure the syntax is correct
    and assuring that a "0 policy" will exist even if not specified and
    also that a "default policy" will exist even if not specified

    :param conf: fconfig parser object for swift.conf
    """
    policies = []
    names = []
    need_default = True
    need_pol0 = True
    for section in conf.sections():
        section_policy = section.split(':')
        if len(section_policy) > 1 and section_policy[0] == 'storage-policy':
            if section_policy[1].isdigit() is True:
                policy_idx = section_policy[1]
                if policy_idx == '0':
                    need_pol0 = False
            else:
                raise ValueError("Malformed storage policy %s" % section)
            try:
                is_default = conf.get(section, 'default')
                need_default = False
            except NoOptionError:
                is_default = 'no'
            try:
                policy_name = conf.get(section, 'name')
            except NoOptionError:
                policy_name = ''

            """ names must be unique if provided """
            if policy_name is not '':
                if policy_name in names:
                    raise ValueError("Duplicate policy name %s" % policy_name)
                else:
                    names.append(policy_name)

            policies.append(StoragePolicy(
                policy_idx,
                policy_name,
                is_default=config_true_value(is_default)))

    # If a 0 policy wasn't explicitly given, or nothing was
    # provided, create the 0 policy now
    if not policies or need_pol0:
        policies.append(StoragePolicy('0', '', '', False))

    # if nothing was specified as default for new containers,
    # specify policy 0 now
    if need_default:
        for policy in policies:
            if policy.idx == '0':
                policy.is_default = 'yes'
    return StoragePolicyCollection(policies)


def get_stor_pols(policies=None):
    global STOR_POLICIES
    """unit test code will pass its policies in"""
    if policies is None:
        return STOR_POLICIES
    else:
        """remove test flag added by unit test code"""
        del policies.pols['unit-test']
        STOR_POLICIES = policies
        return policies

pol_conf = ConfigParser()
if pol_conf.read('/etc/swift/swift.conf'):
    STOR_POLICIES = parse_storage_policies(pol_conf)
else:
    """setup default policy collection for unit test code"""
    STOR_POLICIES = StoragePolicyCollection([StoragePolicy('0', '', 'yes')])
