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

"""WSGI tools for use with swift."""

import errno
import os
import signal
import time
import mimetools
from itertools import chain
from StringIO import StringIO

import eventlet
import eventlet.debug
from eventlet import greenio, GreenPool, sleep, wsgi, listen
from paste.deploy import loadapp, appconfig
from eventlet.green import socket, ssl
from urllib import unquote

from swift.common import utils
from swift.common.swob import Request
from swift.common.utils import capture_stdio, disable_fallocate, \
    drop_privileges, get_logger, NullLogger, config_true_value, \
    validate_configuration, get_hub


def monkey_patch_mimetools():
    """
    mimetools.Message defaults content-type to "text/plain"
    This changes it to default to None, so we can detect missing headers.
    """

    orig_parsetype = mimetools.Message.parsetype

    def parsetype(self):
        if not self.typeheader:
            self.type = None
            self.maintype = None
            self.subtype = None
            self.plisttext = ''
        else:
            orig_parsetype(self)

    mimetools.Message.parsetype = parsetype


def get_socket(conf, default_port=8080):
    """Bind socket to bind ip:port in conf

    :param conf: Configuration dict to read settings from
    :param default_port: port to use if not specified in conf

    :returns : a socket object as returned from socket.listen or
               ssl.wrap_socket if conf specifies cert_file
    """
    bind_addr = (conf.get('bind_ip', '0.0.0.0'),
                 int(conf.get('bind_port', default_port)))
    address_family = [addr[0] for addr in socket.getaddrinfo(
        bind_addr[0], bind_addr[1], socket.AF_UNSPEC, socket.SOCK_STREAM)
        if addr[0] in (socket.AF_INET, socket.AF_INET6)][0]
    sock = None
    bind_timeout = int(conf.get('bind_timeout', 30))
    retry_until = time.time() + bind_timeout
    warn_ssl = False
    while not sock and time.time() < retry_until:
        try:
            sock = listen(bind_addr, backlog=int(conf.get('backlog', 4096)),
                          family=address_family)
            if 'cert_file' in conf:
                warn_ssl = True
                sock = ssl.wrap_socket(sock, certfile=conf['cert_file'],
                                       keyfile=conf['key_file'])
        except socket.error, err:
            if err.args[0] != errno.EADDRINUSE:
                raise
            sleep(0.1)
    if not sock:
        raise Exception(_('Could not bind to %s:%s '
                          'after trying for %s seconds') % (
                              bind_addr[0], bind_addr[1], bind_timeout))
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    # in my experience, sockets can hang around forever without keepalive
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    if hasattr(socket, 'TCP_KEEPIDLE'):
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 600)
    if warn_ssl:
        ssl_warning_message = 'WARNING: SSL should only be enabled for ' \
                              'testing purposes. Use external SSL ' \
                              'termination for a production deployment.'
        get_logger(conf).warning(ssl_warning_message)
        print _(ssl_warning_message)
    return sock


# woo, more shit to import
_AlreadyHandled = wsgi._AlreadyHandled
format_date_time = wsgi.format_date_time

class AmazingSpeedyHttpProtocol(wsgi.HttpProtocol):
    def handle_one_response(self):
        start = time.time()
        headers_set = []
        headers_sent = []

        wfile = self.wfile
        result = None
        use_chunked = [False]
        length = [0]
        status_code = [200]

        def write(data, _writelines=wfile.writelines):
            towrite = []
            if not headers_set:
                raise AssertionError("write() before start_response()")
            elif not headers_sent:
                status, response_headers = headers_set
                headers_sent.append(1)
                header_list = [header[0].lower() for header in response_headers]
                towrite.append('%s %s\r\n' % (self.protocol_version, status))
                for header in response_headers:
                    towrite.append('%s: %s\r\n' % header)

                # send Date header?
                if 'date' not in header_list:
                    towrite.append('Date: %s\r\n' % (format_date_time(time.time()),))

                client_conn = self.headers.get('Connection', '').lower()
                send_keep_alive = False
                if self.close_connection == 0 and \
                   self.server.keepalive and (client_conn == 'keep-alive' or \
                    (self.request_version == 'HTTP/1.1' and
                     not client_conn == 'close')):
                        # only send keep-alives back to clients that sent them,
                        # it's redundant for 1.1 connections
                        send_keep_alive = (client_conn == 'keep-alive')
                        self.close_connection = 0
                else:
                    self.close_connection = 1

                if 'content-length' not in header_list:
                    if self.request_version == 'HTTP/1.1':
                        use_chunked[0] = True
                        towrite.append('Transfer-Encoding: chunked\r\n')
                    elif 'content-length' not in header_list:
                        # client is 1.0 and therefore must read to EOF
                        self.close_connection = 1

                if self.close_connection:
                    towrite.append('Connection: close\r\n')
                elif send_keep_alive:
                    towrite.append('Connection: keep-alive\r\n')
                towrite.append('\r\n')
                # end of header writing

            if data:
                if use_chunked[0]:
                    ## Write the chunked encoding
                    towrite.append("%x\r\n%s\r\n" % (len(data), data))
                else:
                    towrite.append(data)
            try:
                _writelines(towrite)
                length[0] = length[0] + sum(map(len, towrite))
            except UnicodeEncodeError:
                self.server.log_message("Encountered non-ascii unicode while attempting to write wsgi response: %r" % [x for x in towrite if isinstance(x, unicode)])
                self.server.log_message(traceback.format_exc())
                _writelines(
                    ["HTTP/1.1 500 Internal Server Error\r\n",
                    "Connection: close\r\n",
                    "Content-type: text/plain\r\n",
                    "Content-length: 98\r\n",
                    "Date: %s\r\n" % format_date_time(time.time()),
                    "\r\n",
                    ("Internal Server Error: wsgi application passed "
                     "a unicode object to the server instead of a string.")])

        def start_response(status, response_headers, exc_info=None):
            status_code[0] = status.split()[0]
            if exc_info:
                try:
                    if headers_sent:
                        # Re-raise original exception if headers sent
                        raise exc_info[0], exc_info[1], exc_info[2]
                finally:
                    # Avoid dangling circular ref
                    exc_info = None

            capitalized_headers = [('-'.join([x.capitalize()
                                              for x in key.split('-')]), value)
                                   for key, value in response_headers]

            headers_set[:] = [status, capitalized_headers]
            return write

        try:
            try:
                result = self.application(self.environ, start_response)
                if (isinstance(result, _AlreadyHandled)
                    or isinstance(getattr(result, '_obj', None), _AlreadyHandled)):
                    self.close_connection = 1
                    return
                if not headers_sent and hasattr(result, '__len__') and \
                        'Content-Length' not in [h for h, _v in headers_set[1]]:
                    headers_set[1].append(('Content-Length',
                                           str(sum(map(len, result)))))

                # THIS IS WHERE THE MAGIC HAPPENS
                if getattr(result, 'lord_of_the_sendfiles', None):
                    write('')  # force headers out
                    result.lord_of_the_sendfiles(self.wfile)
                    return

                towrite = []
                towrite_size = 0
                just_written_size = 0
                for data in result:
                    towrite.append(data)
                    towrite_size += len(data)
                    if towrite_size >= self.minimum_chunk_size:
                        write(''.join(towrite))
                        towrite = []
                        just_written_size = towrite_size
                        towrite_size = 0
                if towrite:
                    just_written_size = towrite_size
                    write(''.join(towrite))
                if not headers_sent or (use_chunked[0] and just_written_size):
                    write('')
            except Exception, e:
                self.close_connection = 1
                tb = traceback.format_exc()
                self.server.log_message(tb)
                if not headers_set:
                    err_body = ""
                    if(self.server.debug):
                        err_body = tb
                    start_response("500 Internal Server Error",
                                   [('Content-type', 'text/plain'),
                                    ('Content-length', len(err_body))])
                    write(err_body)
        finally:
            if hasattr(result, 'close'):
                result.close()
            if (self.environ['eventlet.input'].chunked_input or
                    self.environ['eventlet.input'].position \
                    < self.environ['eventlet.input'].content_length):
                ## Read and discard body if there was no pending 100-continue
                if not self.environ['eventlet.input'].wfile:
                    while self.environ['eventlet.input'].read(MINIMUM_CHUNK_SIZE):
                        pass
            finish = time.time()

            for hook, args, kwargs in self.environ['eventlet.posthooks']:
                hook(self.environ, *args, **kwargs)

            if self.server.log_output:

                self.server.log_message(self.server.log_format % dict(
                    client_ip=self.get_client_ip(),
                    date_time=self.log_date_time_string(),
                    request_line=self.requestline,
                    status_code=status_code[0],
                    body_length=length[0],
                    wall_seconds=finish - start))



# TODO: pull pieces of this out to test
def run_wsgi(conf_file, app_section, *args, **kwargs):
    """
    Runs the server using the specified number of workers.

    :param conf_file: Path to paste.deploy style configuration file
    :param app_section: App name from conf file to load config from
    """
    # Load configuration, Set logger and Load request processor
    try:
        (app, conf, logger, log_name) = \
            init_request_processor(conf_file, app_section, *args, **kwargs)
    except ConfigFileError, e:
        print e
        return

    # bind to address and port
    sock = get_socket(conf, default_port=kwargs.get('default_port', 8080))
    # remaining tasks should not require elevated privileges
    drop_privileges(conf.get('user', 'swift'))
    # set utils.FALLOCATE_RESERVE if desired
    reserve = int(conf.get('fallocate_reserve', 0))
    if reserve > 0:
        utils.FALLOCATE_RESERVE = reserve
    # redirect errors to logger and close stdio
    capture_stdio(logger)

    def run_server():
        wsgi.HttpProtocol.default_request_version = "HTTP/1.0"
        # Turn off logging requests by the underlying WSGI software.
        wsgi.HttpProtocol.log_request = lambda *a: None
        # Redirect logging other messages by the underlying WSGI software.
        wsgi.HttpProtocol.log_message = \
            lambda s, f, *a: logger.error('ERROR WSGI: ' + f % a)
        wsgi.WRITE_TIMEOUT = int(conf.get('client_timeout') or 60)

        eventlet.hubs.use_hub(get_hub())
        eventlet.patcher.monkey_patch(all=False, socket=True)
        eventlet_debug = config_true_value(conf.get('eventlet_debug', 'no'))
        eventlet.debug.hub_exceptions(eventlet_debug)
        app = loadapp('config:%s' % conf_file,
                      global_conf={'log_name': log_name})
        pool = GreenPool(size=1024)
        try:
            wsgi.server(sock, app, NullLogger(), custom_pool=pool,
                        protocol=AmazingSpeedyHttpProtocol)
        except socket.error, err:
            if err[0] != errno.EINVAL:
                raise
        pool.waitall()

    worker_count = int(conf.get('workers', '1'))
    # Useful for profiling [no forks].
    if worker_count == 0:
        run_server()
        return

    def kill_children(*args):
        """Kills the entire process group."""
        logger.error('SIGTERM received')
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        running[0] = False
        os.killpg(0, signal.SIGTERM)

    def hup(*args):
        """Shuts down the server, but allows running requests to complete"""
        logger.error('SIGHUP received')
        signal.signal(signal.SIGHUP, signal.SIG_IGN)
        running[0] = False

    running = [True]
    signal.signal(signal.SIGTERM, kill_children)
    signal.signal(signal.SIGHUP, hup)
    children = []
    while running[0]:
        while len(children) < worker_count:
            pid = os.fork()
            if pid == 0:
                signal.signal(signal.SIGHUP, signal.SIG_DFL)
                signal.signal(signal.SIGTERM, signal.SIG_DFL)
                run_server()
                logger.notice('Child %d exiting normally' % os.getpid())
                return
            else:
                logger.notice('Started child %s' % pid)
                children.append(pid)
        try:
            pid, status = os.wait()
            if os.WIFEXITED(status) or os.WIFSIGNALED(status):
                logger.error('Removing dead child %s' % pid)
                children.remove(pid)
        except OSError, err:
            if err.errno not in (errno.EINTR, errno.ECHILD):
                raise
        except KeyboardInterrupt:
            logger.notice('User quit')
            break
    greenio.shutdown_safe(sock)
    sock.close()
    logger.notice('Exited')


class ConfigFileError(Exception):
    pass


def init_request_processor(conf_file, app_section, *args, **kwargs):
    """
    Loads common settings from conf
    Sets the logger
    Loads the request processor

    :param conf_file: Path to paste.deploy style configuration file
    :param app_section: App name from conf file to load config from
    :returns: the loaded application entry point
    :raises ConfigFileError: Exception is raised for config file error
    """
    try:
        conf = appconfig('config:%s' % conf_file, name=app_section)
    except Exception, e:
        raise ConfigFileError("Error trying to load config %s: %s" %
                              (conf_file, e))

    validate_configuration()

    # pre-configure logger
    log_name = conf.get('log_name', app_section)
    if 'logger' in kwargs:
        logger = kwargs.pop('logger')
    else:
        logger = get_logger(conf, log_name,
                            log_to_console=kwargs.pop('verbose', False),
                            log_route='wsgi')

    # disable fallocate if desired
    if config_true_value(conf.get('disable_fallocate', 'no')):
        disable_fallocate()

    monkey_patch_mimetools()
    app = loadapp('config:%s' % conf_file, global_conf={'log_name': log_name})
    return (app, conf, logger, log_name)


class WSGIContext(object):
    """
    This class provides a means to provide context (scope) for a middleware
    filter to have access to the wsgi start_response results like the request
    status and headers.
    """
    def __init__(self, wsgi_app):
        self.app = wsgi_app

    def _start_response(self, status, headers, exc_info=None):
        """
        Saves response info without sending it to the remote client.
        Uses the same semantics as the usual WSGI start_response.
        """
        self._response_status = status
        self._response_headers = headers
        self._response_exc_info = exc_info

    def _app_call(self, env):
        """
        Ensures start_response has been called before returning.
        """
        self._response_status = None
        self._response_headers = None
        self._response_exc_info = None
        resp = self.app(env, self._start_response)
        # if start_response has been called, just return the iter
        if self._response_status is not None:
            return resp
        resp = iter(resp)
        try:
            first_chunk = resp.next()
        except StopIteration:
            return iter([])
        else:  # We got a first_chunk
            return chain([first_chunk], resp)

    def _get_status_int(self):
        """
        Returns the HTTP status int from the last called self._start_response
        result.
        """
        return int(self._response_status.split(' ', 1)[0])

    def _response_header_value(self, key):
        "Returns str of value for given header key or None"
        for h_key, val in self._response_headers:
            if h_key.lower() == key.lower():
                return val
        return None


def make_pre_authed_request(env, method=None, path=None, body=None,
                            headers=None, agent='Swift', swift_source=None):
    """
    Makes a new swob.Request based on the current env but with the
    parameters specified. Note that this request will be preauthorized.

    :param env: The WSGI environment to base the new request on.
    :param method: HTTP method of new request; default is from
                   the original env.
    :param path: HTTP path of new request; default is from the
                 original env. path should be compatible with what you
                 would send to Request.blank. path should be quoted and it
                 can include a query string. for example:
                 '/a%20space?unicode_str%E8%AA%9E=y%20es'
    :param body: HTTP body of new request; empty by default.
    :param headers: Extra HTTP headers of new request; None by
                    default.
    :param agent: The HTTP user agent to use; default 'Swift'. You
                  can put %(orig)s in the agent to have it replaced
                  with the original env's HTTP_USER_AGENT, such as
                  '%(orig)s StaticWeb'. You also set agent to None to
                  use the original env's HTTP_USER_AGENT or '' to
                  have no HTTP_USER_AGENT.
    :param swift_source: Used to mark the request as originating out of
                         middleware. Will be logged in proxy logs.
    :returns: Fresh swob.Request object.
    """
    query_string = None
    if path and '?' in path:
        path, query_string = path.split('?', 1)
    newenv = make_pre_authed_env(env, method, path=unquote(path), agent=agent,
                                 query_string=query_string,
                                 swift_source=swift_source)
    if not headers:
        headers = {}
    if body:
        return Request.blank(path, environ=newenv, body=body, headers=headers)
    else:
        return Request.blank(path, environ=newenv, headers=headers)


def make_pre_authed_env(env, method=None, path=None, agent='Swift',
                        query_string=None, swift_source=None):
    """
    Returns a new fresh WSGI environment with escalated privileges to
    do backend checks, listings, etc. that the remote user wouldn't
    be able to accomplish directly.

    :param env: The WSGI environment to base the new environment on.
    :param method: The new REQUEST_METHOD or None to use the
                   original.
    :param path: The new path_info or none to use the original. path
                 should NOT be quoted. When building a url, a Webob
                 Request (in accordance with wsgi spec) will quote
                 env['PATH_INFO'].  url += quote(environ['PATH_INFO'])
    :param query_string: The new query_string or none to use the original.
                         When building a url, a Webob Request will append
                         the query string directly to the url.
                         url += '?' + env['QUERY_STRING']
    :param agent: The HTTP user agent to use; default 'Swift'. You
                  can put %(orig)s in the agent to have it replaced
                  with the original env's HTTP_USER_AGENT, such as
                  '%(orig)s StaticWeb'. You also set agent to None to
                  use the original env's HTTP_USER_AGENT or '' to
                  have no HTTP_USER_AGENT.
    :param swift_source: Used to mark the request as originating out of
                         middleware. Will be logged in proxy logs.
    :returns: Fresh WSGI environment.
    """
    newenv = {}
    for name in ('eventlet.posthooks', 'HTTP_USER_AGENT', 'HTTP_HOST',
                 'PATH_INFO', 'QUERY_STRING', 'REMOTE_USER', 'REQUEST_METHOD',
                 'SCRIPT_NAME', 'SERVER_NAME', 'SERVER_PORT',
                 'SERVER_PROTOCOL', 'swift.cache', 'swift.source',
                 'swift.trans_id'):
        if name in env:
            newenv[name] = env[name]
    if method:
        newenv['REQUEST_METHOD'] = method
    if path:
        newenv['PATH_INFO'] = path
        newenv['SCRIPT_NAME'] = ''
    if query_string is not None:
        newenv['QUERY_STRING'] = query_string
    if agent:
        newenv['HTTP_USER_AGENT'] = (
            agent % {'orig': env.get('HTTP_USER_AGENT', '')}).strip()
    elif agent == '' and 'HTTP_USER_AGENT' in newenv:
        del newenv['HTTP_USER_AGENT']
    if swift_source:
        newenv['swift.source'] = swift_source
    newenv['swift.authorize'] = lambda req: None
    newenv['swift.authorize_override'] = True
    newenv['REMOTE_USER'] = '.wsgi.pre_authed'
    newenv['wsgi.input'] = StringIO('')
    if 'SCRIPT_NAME' not in newenv:
        newenv['SCRIPT_NAME'] = ''
    return newenv
