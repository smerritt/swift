#! /usr/bin/env python
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

import contextlib
import eventlet
import mock
import os
import shutil
import socket
import tempfile
import unittest2
from six import StringIO

from swift.cli.interrogate import interrogate
from swift.common import baroque_rpc, utils


class TestRunScenario(unittest2.TestCase):
    @classmethod
    def setUpClass(cls):
        # This is usually done by the time we start, but not if you run just
        # this one test file
        utils.eventlet_monkey_patch()

    def setUp(self):
        self.tempdir = tempfile.mkdtemp()
        self.socket_path = os.path.join(self.tempdir, "some-daemon.sock")

    def tearDown(self):
        shutil.rmtree(self.tempdir, ignore_errors=True)

    @contextlib.contextmanager
    def _test_server_conn(self):
        commands = {'status': lambda: {"me": "doing well, thanks"}}
        with baroque_rpc.unix_socket_server(self.socket_path, commands):
            eventlet.sleep(0)
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            sock.connect(self.socket_path)
            yield
            sock.close()

    def test_it_runs(self):
        fake_stdout = StringIO()
        with self._test_server_conn():
            with mock.patch('sys.stdout', fake_stdout):
                interrogate("some-daemon", self.tempdir)

        self.assertIn("doing well, thanks", fake_stdout.getvalue())
            
    def test_prefix(self):
        fake_stdout = StringIO()
        with self._test_server_conn():
            with mock.patch('sys.stdout', fake_stdout):
                interrogate("other-daemon", self.tempdir)

        self.assertNotIn("doing well, thanks", fake_stdout.getvalue())
