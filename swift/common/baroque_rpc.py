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

"""
RPC library for controlling Swift daemons

The protocol uses length-value encoding. Lengths are big-endian unsigned
64-bit integers, and values are UTF-8-encoded JSON-RPC requests or
responses.

Really, it's JSON-RPC 2.0, but with a tiny bit of framing added to allow for
reading a request easily and quickly (no buffering, no reading one byte at a
time): read the length, decode it, then read that many bytes.

TODO: handle notifications

"""

import contextlib
import errno
import eventlet
import itertools
import json
import os
import six
import socket
import struct


DEFAULT_MAX_VALUE_SIZE = 2 ** 22  # 4 MiB per command is probably enough
HEADER_FORMAT = ">QH"
HEADER_LENGTH = struct.calcsize(HEADER_FORMAT)

RPC_TYPE_JSONRPC20 = 1

JSONRPC_PARSE_ERROR = -32700
JSONRPC_INVALID_REQUEST = -32600
JSONRPC_METHOD_NOT_FOUND = -32601
JSONRPC_INVALID_PARAMS = -32602
JSONRPC_INTERNAL_ERROR = -32603
# defined by us, not the JSON-RPC standard. We get -32000 to -32099 for
# implementation-defined server errors.
JSONRPC_INVALID_UTF8 = -32099


@contextlib.contextmanager
def unix_socket_client(socket_path):
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.connect(socket_path)
    try:
        yield BaroqueRpcClient(sock)
    finally:
        sock.close()


@contextlib.contextmanager
def unix_socket_server(socket_path, commands):
    listen_socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        listen_socket.bind(socket_path)
    except socket.error as err:
        if err.errno == errno.EADDRINUSE:
            os.unlink(socket_path)
            listen_socket.bind(socket_path)
        else:
            raise

    listen_socket.listen(1)
    rpc_server = BaroqueRpcServer(listen_socket, commands)
    rpc_server_greenthread = eventlet.spawn(rpc_server.run)

    try:
        yield rpc_server
    finally:
        rpc_server_greenthread.kill()
        listen_socket.close()
        os.unlink(socket_path)


def _read_n(sock, n):
    buf = []
    remaining = n
    while remaining:
        b = sock.recv(remaining)
        if not b:
            raise ValueError(
                ("EOF on socket read; peer did not send enough data "
                 "(wanted %d bytes but only got %d)" % (n, n - remaining)))
        buf.append(b)
        remaining -= len(b)
    return b"".join(buf)


def _read_binary_value(sock, maxlen=DEFAULT_MAX_VALUE_SIZE):
    """
    Read a length-value-encoded value off a socket.

    :param sock: socket to read from
    :param maxlen: maximum number of bytes to return

    :returns: 2-tuple (type indicator, value), e.g. (RPC_TYPE_JSONRPC20,
        b'{"jsonrpc": "2.0", ...}')

    :raises ValueError: if the value is truncated or too long
    """
    vlen, vtype = struct.unpack(HEADER_FORMAT, _read_n(sock, HEADER_LENGTH))
    if vlen > maxlen:
        raise ValueError("RPC command too long")

    value = _read_n(sock, vlen)
    return vtype, value


def _write_binary_value(sock, value_type, value):
    """
    Write some bytes to a socket in length-value format.

    :param sock: socket to write to
    :param value_type: type indicator
    :param value: the bytes to send
    """
    # We'll need to revisit this should we ever add a second type
    header = struct.pack(HEADER_FORMAT, len(value), value_type)
    sock.sendall(header)
    sock.sendall(value)


def _decode_jsonrpc_request(req_bytes):
    """
    Decode a JSON-RPC 2.0 request from raw bytes.

    :returns: a 2-tuple (request, error_response), one of which is None. If
        there is a problem decoding the request, error_response will be set;
        otherwise, request will be set.
    """
    try:
        req_str = req_bytes.decode("utf-8")
    except UnicodeDecodeError as err:
        return (None, _jsonrpc_error_response(
            JSONRPC_INVALID_UTF8,
            "Request is not valid UTF-8 (%s)" % err))
    try:
        req = json.loads(req_str)
    except ValueError as err:
        return (None, _jsonrpc_error_response(
            JSONRPC_PARSE_ERROR,
            "Request is not valid JSON (%s)" % err))
    if not isinstance(req, (list, dict)):
        return (None, _jsonrpc_error_response(
            JSONRPC_INVALID_REQUEST,
            "Request is neither JSON object nor list"))
    return req, None


def _validate_jsonrpc_request(req):
    """
    Validates a JSON-RPC 2.0 request object.

    Does not work on batches; if you have a batch request, call this to
    validate each element of the batch.

    :returns: None for valid request, JSON-RPC error response for invalid
        request.
    """

    if not isinstance(req, dict):
        return _jsonrpc_error_response(
            JSONRPC_INVALID_REQUEST, "Request is not JSON object")
    if req.get('jsonrpc') != '2.0':
        return _jsonrpc_error_response(
            JSONRPC_INVALID_REQUEST, "Request is not JSON-RPC 2.0")
    if 'id' not in req:
        return _jsonrpc_error_response(
            JSONRPC_INVALID_REQUEST, "Request has no id")
    if 'method' not in req:
        return _jsonrpc_error_response(
            JSONRPC_INVALID_REQUEST, "Request has no method")
    if not isinstance(req['method'], six.string_types):
        return _jsonrpc_error_response(
            JSONRPC_INVALID_REQUEST, "Request has non-string method")
    if 'params' in req and not isinstance(req['params'], (list, dict)):
        return _jsonrpc_error_response(
            JSONRPC_INVALID_REQUEST, "Request has invalid params")

    return None


def _jsonrpc_error_response(code, message, request_id=None):
    return {
        "jsonrpc": "2.0",
        "id": request_id,
        "error": {
            "code": code,
            "message": message}}


class BaroqueRpcError(Exception):
    def __init__(self, message="", errno=None):
        if errno is not None:
            message = "[Errno {e}] {m}".format(e=errno, m=message)
        super(BaroqueRpcError, self).__init__(message)
        self.errno = errno


class BaroqueRpcServer(object):
    """
    An RPC server.

    Accepts connections from a listen socket of some type. Client
    connections are each handled in their own greenthreads.

    To provide useful behavior, create the RPC server with a dictionary of
    {command_name: callable} pairs. Upon receipt of a command, the
    corresponding callable is invoked with any provided arguments.

    JSON-RPC notifications are ignored, though one could change that if one
    were so inclined.
    """
    def __init__(self, listen_sock, commands):
        self.listen_sock = listen_sock
        self.commands = commands
        self._running_greenthreads = set()

    def run(self):
        try:
            while True:
                client_sock, _junk = self.listen_sock.accept()
                handler_gt = eventlet.spawn(
                    self.handle_connection, client_sock)
                self._running_greenthreads.add(handler_gt)
                handler_gt.link(self._post_connection_cleanup)
        finally:
            for gt in list(self._running_greenthreads):
                gt.kill()
                self._running_greenthreads.discard(gt)

    def _post_connection_cleanup(self, gt):
        self._running_greenthreads.discard(gt)

    def handle_connection(self, sock):
        """
        Handle a single socket connected to a single client.
        """
        try:
            while True:
                try:
                    # TODO: timeout
                    rpc_type, raw_value = _read_binary_value(sock)
                except ValueError:
                    # If the client can't manage to send us a complete,
                    # reasonably-sized request, then give up.
                    return

                if rpc_type != RPC_TYPE_JSONRPC20:
                    # We've no idea how to handle this value, nor any idea
                    # how to return an error that the client would
                    # understand, so bail out.
                    return

                req, error_response = _decode_jsonrpc_request(raw_value)
                if error_response:
                    _write_binary_value(
                        sock,
                        RPC_TYPE_JSONRPC20,
                        json.dumps(error_response).encode("utf-8"))

                if isinstance(req, list):
                    # it's a batch, i.e. an array of request objects
                    jsonrpc_response = [self.handle_request(x) for x in req]
                else:
                    # it's a single request
                    jsonrpc_response = self.handle_request(req)

                _write_binary_value(
                    sock,
                    RPC_TYPE_JSONRPC20,
                    json.dumps(jsonrpc_response).encode("utf-8"))
        finally:
            sock.close()

    def handle_request(self, req):
        error_response = _validate_jsonrpc_request(req)
        if error_response:
            return error_response

        command = req['method']
        params = req['params']

        if command not in self.commands:
            return {"jsonrpc": "2.0",
                    "id": req["id"],
                    "error": {
                        "code": JSONRPC_METHOD_NOT_FOUND,
                        "message": "No method %s" % command}}

        try:
            if isinstance(params, list):
                result = self.commands[command](*params)
            else:
                result = self.commands[command](**params)
        except TypeError as err:
            return {"jsonrpc": "2.0",
                    "id": req["id"],
                    "error": {
                        "code": JSONRPC_INVALID_PARAMS,
                        "message": str(err)}}
        except (Exception, eventlet.Timeout) as err:
            return {"jsonrpc": "2.0",
                    "id": req["id"],
                    "error": {
                        "code": JSONRPC_INTERNAL_ERROR,
                        "message": str(err)}}

        jsonrpc_response = {"jsonrpc": "2.0",
                            "id": req["id"],
                            "result": result}
        return jsonrpc_response


class BaroqueRpcClient(object):
    def __init__(self, sock):
        self.sock = sock
        self._seq = itertools.count(0)

    def rpc(self, method, params=()):
        """
        Call a remote method/procedure.

        :param method: name of the method to invoke

        :param params: params for the remote method. If params is a list or
        tuple, then the remote method will be invoked with positional
        params, e.g. "func(*params)". If params is a dictionary, then the
        remote method will be invoked with named params, e.g.
        "func(**params)".
        """
        request = {"jsonrpc": "2.0",
                   "id": next(self._seq),
                   "method": method,
                   "params": params}

        _write_binary_value(self.sock,
                            RPC_TYPE_JSONRPC20,
                            json.dumps(request).encode("utf-8"))
        resp_type, raw_resp = _read_binary_value(self.sock)

        if resp_type != RPC_TYPE_JSONRPC20:
            raise ValueError("can't handle RPC type {}".format(resp_type))

        resp = json.loads(raw_resp.decode("utf-8"))
        if "error" in resp:
            msg = resp["error"]["message"]
            code = resp["error"]["code"]
            raise BaroqueRpcError(msg, code)
        else:
            return resp["result"]
