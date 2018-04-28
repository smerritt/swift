# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy
# of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import argparse
import os
import pprint
import stat
import sys
import traceback

from swift.common import baroque_rpc
from swift.common.manager import RUN_DIR


def interrogate(prefix, directory=RUN_DIR):
    """
    For each management socket matching the prefix, interrogate it and print
    the results.
    """

    num_errors = 0
    filenames = os.listdir(directory)
    for filename in filenames:
        if not filename.startswith(prefix):
            continue
        fullpath = os.path.join(directory, filename)
        try:
            stat_result = os.stat(fullpath)
        except OSError as err:
            sys.stderr.write("Error statting %s: %s" % (fullpath, err))
            continue

        if stat.S_ISSOCK(stat_result.st_mode):
            try:
                with baroque_rpc.unix_socket_client(fullpath) as client:
                    response = client.rpc('status')
                    print("=" * 72)
                    print(fullpath)
                    pprint.pprint(response)
            except Exception:
                num_errors += 1
                sys.stderr.write("Error reading status from %s\n" % fullpath)
                traceback.print_exc(file=sys.stderr)
    return num_errors


def main():
    parser = argparse.ArgumentParser(
        "Get status of the running Swift daemons on this machine")
    parser.add_argument("prefix", help="Prefix of sockets to query",
                        default="", nargs="?")
    parser.add_argument("--run-dir", default=RUN_DIR,
                        help="Directory containing management sockets")

    args = parser.parse_args()

    interrogate(args.prefix, args.run_dir)
