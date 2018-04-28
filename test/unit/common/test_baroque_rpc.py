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
import errno
import eventlet
import json
import os
import shutil
import socket
import struct
import tempfile
import unittest2

from swift.common import baroque_rpc, utils


class TestBaroqueRpcServer(unittest2.TestCase):

    @classmethod
    def setUpClass(cls):
        # This is usually done by the time we start, but not if you run just
        # this one test file
        utils.eventlet_monkey_patch()

    def setUp(self):
        self.tempdir = tempfile.mkdtemp()
        self.socket_path = os.path.join(self.tempdir, "test-socket")

    def tearDown(self):
        shutil.rmtree(self.tempdir, ignore_errors=True)

    @contextlib.contextmanager
    def _test_server_conn(self):

        commands = {'add': lambda a, b: a + b,
                    'subtract': lambda a, b: a - b,
                    'divide_by_zero': lambda a: a / 0}

        with baroque_rpc.unix_socket_server(self.socket_path, commands):
            eventlet.sleep(0)
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            sock.connect(self.socket_path)
            yield sock
            sock.close()

    def _send_json_req(self, req):
        raw_req = json.dumps(req).encode("utf-8")
        with self._test_server_conn() as conn:
            baroque_rpc._write_binary_value(
                conn, baroque_rpc.RPC_TYPE_JSONRPC20, raw_req)
            resp_type, raw_resp = baroque_rpc._read_binary_value(conn)
            self.assertEqual(resp_type, baroque_rpc.RPC_TYPE_JSONRPC20)
        return json.loads(raw_resp.decode("utf-8"))

    def test_stale_socket(self):
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        s.bind(self.socket_path)
        s.close()   # doesn't unlink the socket file

        resp = self._send_json_req({
            'jsonrpc': '2.0',
            'method': 'add',
            'params': [2, 2],
            'id': 1234})
        self.assertEqual(resp['jsonrpc'], '2.0')
        self.assertEqual(resp['result'], 4)
        self.assertNotIn('error', resp)

    def test_invalid_utf8(self):
        bogon = b"\xff\x00"
        with self._test_server_conn() as conn:
            baroque_rpc._write_binary_value(
                conn, baroque_rpc.RPC_TYPE_JSONRPC20, bogon)
            _junk, raw_resp = baroque_rpc._read_binary_value(conn)

        resp = json.loads(raw_resp.decode("utf-8"))
        self.assertEqual(resp['jsonrpc'], '2.0')
        self.assertEqual(resp['error']['code'], -32099)
        self.assertNotIn('result', resp)

    def test_invalid_json(self):
        bogon = b"{{{plecopteran-intemperate"
        with self._test_server_conn() as conn:
            baroque_rpc._write_binary_value(
                conn, baroque_rpc.RPC_TYPE_JSONRPC20, bogon)
            _junk, raw_resp = baroque_rpc._read_binary_value(conn)

        resp = json.loads(raw_resp.decode("utf-8"))
        self.assertEqual(resp['jsonrpc'], '2.0')
        self.assertEqual(resp['error']['code'], -32700)
        self.assertNotIn('result', resp)

    def test_unknown_rpc_type(self):
        value = b"protobufs or something"
        with self._test_server_conn() as conn:
            baroque_rpc._write_binary_value(
                conn, 9876, value)

            # immediate EOF: the server doesn't know what we said, nor does
            # it know how to tell us that
            self.assertEqual(conn.recv(1), b"")

    def test_short_write(self):
        value = b"ponderant-spirket"
        header = struct.pack(baroque_rpc.HEADER_FORMAT,
                             len(value) + 1,
                             baroque_rpc.RPC_TYPE_JSONRPC20)
        msg = header + value

        with self._test_server_conn() as conn:
            conn.sendall(msg)
            conn.shutdown(socket.SHUT_WR)

            # the server didn't get a complete length-type-value record, so
            # it closes the connection
            self.assertEqual(conn.recv(1), b"")

    def test_oversize_rpc(self):
        header = struct.pack(baroque_rpc.HEADER_FORMAT,
                             2 ** 30,  # 1 GiB is much too big
                             baroque_rpc.RPC_TYPE_JSONRPC20)
        with self._test_server_conn() as conn:
            conn.sendall(header)
            with self.assertRaises(socket.error) as cm:
                for _ in range(2 ** 10):
                    conn.sendall(b'X' * (2 ** 20))
            # EPIPE, i.e. the server closed the socket without reading all
            # that garbage
            self.assertEqual(cm.exception.errno, errno.EPIPE)

    def test_missing_jsonrpc_field(self):
        resp = self._send_json_req({
            # no "jsonrpc" field
            'method': 'add',
            'params': [2, 2],
            'id': 1234})
        self.assertEqual(resp['jsonrpc'], '2.0')
        self.assertEqual(resp['error']['code'], -32600)
        self.assertNotIn('result', resp)

    def test_invalid_jsonrpc_field(self):
        resp = self._send_json_req({
            'jsonrpc': u"\U0001f47a",
            'method': 'add',
            'params': [2, 2],
            'id': 1234})
        self.assertEqual(resp['jsonrpc'], '2.0')
        self.assertEqual(resp['error']['code'], -32600)
        self.assertNotIn('result', resp)

    def test_missing_method_field(self):
        resp = self._send_json_req({
            'jsonrpc': '2.0',
            'params': [2, 2],
            'id': 1234})
        self.assertEqual(resp['jsonrpc'], '2.0')
        self.assertEqual(resp['error']['code'], -32600)
        self.assertNotIn('result', resp)

    def test_invalid_method_field(self):
        resp = self._send_json_req({
            'jsonrpc': '2.0',
            'method': [None],
            'params': [2, 2],
            'id': 1234})
        self.assertEqual(resp['jsonrpc'], '2.0')
        self.assertEqual(resp['error']['code'], -32600)
        self.assertNotIn('result', resp)

    def test_invalid_params_field(self):
        resp = self._send_json_req({
            'jsonrpc': '2.0',
            'method': 'add',
            'params': 'a string',
            'id': 1234})
        self.assertEqual(resp['jsonrpc'], '2.0')
        self.assertEqual(resp['error']['code'], -32600)
        self.assertNotIn('result', resp)

    def test_missing_id_field(self):
        resp = self._send_json_req({
            'jsonrpc': '2.0',
            'method': 'add',
            'params': [2, 2]})
        self.assertEqual(resp['jsonrpc'], '2.0')
        self.assertEqual(resp['error']['code'], -32600)
        self.assertNotIn('result', resp)

    def test_not_json_object(self):
        resp = self._send_json_req("counselorship-unpurloined")
        self.assertEqual(resp['jsonrpc'], '2.0')
        self.assertEqual(resp['error']['code'], -32600)
        self.assertNotIn('result', resp)

    def test_no_such_method(self):
        resp = self._send_json_req({
            'jsonrpc': '2.0',
            'method': 'frobnicate',
            'params': ['some', 'thing'],
            'id': 1234})
        self.assertEqual(resp['jsonrpc'], '2.0')
        self.assertEqual(resp['error']['code'], -32601)
        self.assertNotIn('result', resp)

    def test_method_crashes(self):
        resp = self._send_json_req({
            'jsonrpc': '2.0',
            'method': 'divide_by_zero',
            'params': [10],
            'id': 1234})
        self.assertEqual(resp['jsonrpc'], '2.0')
        self.assertEqual(resp['error']['code'], -32603)
        self.assertNotIn('result', resp)

    def test_positional_params(self):
        resp = self._send_json_req({
            'jsonrpc': '2.0',
            'method': 'add',
            'params': [2, 2],
            'id': 1234})
        self.assertEqual(resp['jsonrpc'], '2.0')
        self.assertEqual(resp['result'], 4)
        self.assertNotIn('error', resp)

    def test_named_params(self):
        resp = self._send_json_req({
            'jsonrpc': '2.0',
            'method': 'add',
            'params': {'a': 1, 'b': 2},
            'id': 1234})
        self.assertEqual(resp['jsonrpc'], '2.0')
        self.assertEqual(resp['result'], 3)
        self.assertNotIn('error', resp)

    def test_too_few_params(self):
        resp = self._send_json_req({
            'jsonrpc': '2.0',
            'method': 'add',
            'params': [2],
            'id': 1234})
        self.assertEqual(resp['jsonrpc'], '2.0')
        self.assertEqual(resp['error']['code'], -32602)
        self.assertNotIn('result', resp)

    def test_misnamed_params(self):
        resp = self._send_json_req({
            'jsonrpc': '2.0',
            'method': 'add',
            'params': {'apple': 1, 'banana': 2},
            'id': 1234})
        self.assertEqual(resp['jsonrpc'], '2.0')
        self.assertEqual(resp['error']['code'], -32602)
        self.assertNotIn('result', resp)

    def test_batch(self):
        resp = self._send_json_req([{
            'jsonrpc': '2.0',
            'method': 'add',
            'params': [1, 2],
            'id': 1234,
        }, {
            'jsonrpc': '2.0',
            'method': 'add',
            'params': [10, 20],
            'id': 2345,
        }])
        self.assertEqual(resp, [{
            'jsonrpc': '2.0',
            'id': 1234,
            'result': 3,
        }, {
            'jsonrpc': '2.0',
            'id': 2345,
            'result': 30,
        }])

    def test_batch_with_errors(self):
        resp = self._send_json_req([{
            'jsonrpc': '2.0',
            'method': 'add',
            'params': [1, 2],
            'id': 1234,
        }, {
            'jsonrpc': '2.0',
            'method': 'nonesuch',
            'params': [10, 20],
            'id': 2345,
        }, {
            'triocular': 'unplat',
        }])
        self.assertEqual(resp, [{
            'jsonrpc': '2.0',
            'id': 1234,
            'result': 3,
        }, {
            'jsonrpc': '2.0',
            'id': 2345,
            'error': {
                'message': 'No method nonesuch',
                'code': -32601,
            },
        }, {
            'jsonrpc': '2.0',
            'id': None,
            'error': {
                'message': 'Request is not JSON-RPC 2.0',
                'code': -32600,
            },
        }])


class TestBaroqueRpcClient(unittest2.TestCase):

    @classmethod
    def setUpClass(cls):
        utils.eventlet_monkey_patch()

    def setUp(self):
        self.tempdir = tempfile.mkdtemp()
        self.socket_path = os.path.join(self.tempdir, "test-socket")

    def tearDown(self):
        shutil.rmtree(self.tempdir, ignore_errors=True)

    @contextlib.contextmanager
    def _test_server_conn(self):

        commands = {'add': lambda a, b: a + b,
                    'subtract': lambda a, b: a - b,
                    'echo': lambda *a, **kw: (a, kw),
                    'divide_by_zero': lambda a: a / 0}

        with baroque_rpc.unix_socket_server(self.socket_path, commands):
            eventlet.sleep(0)
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            sock.connect(self.socket_path)
            yield
            sock.close()

    def test_success(self):
        with self._test_server_conn(), \
                baroque_rpc.unix_socket_client(self.socket_path) as client:
            result = client.rpc('add', (4, 5))
        self.assertEqual(result, 9)

    def test_no_such_method(self):
        with self._test_server_conn(), \
                baroque_rpc.unix_socket_client(self.socket_path) as client:
            with self.assertRaises(baroque_rpc.BaroqueRpcError) as cm:
                client.rpc('no_such_method')

        self.assertEqual(cm.exception.errno,
                         baroque_rpc.JSONRPC_METHOD_NOT_FOUND)
        self.assertIn("no_such_method", str(cm.exception))

    def test_positional_params(self):
        with self._test_server_conn(), \
                baroque_rpc.unix_socket_client(self.socket_path) as client:
            result = client.rpc('echo', [1, 2, 3, 4])
        self.assertEqual(result, [[1, 2, 3, 4], {}])

    def test_named_params(self):
        with self._test_server_conn(), \
                baroque_rpc.unix_socket_client(self.socket_path) as client:
            result = client.rpc('echo', {'a': 1, 'b': 2})
        self.assertEqual(result, [[], {'a': 1, 'b': 2}])


class TestBaroqueRpcError(unittest2.TestCase):
    def test_with_errno(self):
        err = baroque_rpc.BaroqueRpcError("oops", 982)
        self.assertEqual(str(err), "[Errno 982] oops")

    def test_without_errno(self):
        err = baroque_rpc.BaroqueRpcError("darn it")
        self.assertEqual(str(err), "darn it")
