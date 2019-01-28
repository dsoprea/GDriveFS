import logging
import threading
import webbrowser
import time

import io

import gdrivefs.oauth_authorize
import gdrivefs.conf

try:
    # Python 3
    import socketserver
except ImportError:
    # Python 2
    import SocketServer as socketserver

try:
    # Python 3
    import http.server
except ImportError:
    # Python 2
    import BaseHTTPServer
    _BaseHTTPRequestHandler = BaseHTTPServer.BaseHTTPRequestHandler
else:
    _BaseHTTPRequestHandler = http.server.BaseHTTPRequestHandler

try:
    # Python 3
    import urllib.parse
except ImportError:
    # Python 2
    import urlparse

    def arguments_from_url(url):
        u = urlparse.urlparse(url)
        arguments = urlparse.parse_qs(u.query)
        
        return arguments
else:
    def arguments_from_url(url):
        u = urllib.parse.urlparse(url)
        arguments = urllib.parse.parse_qs(u.query)

        return arguments

_LOGGER = logging.getLogger(__name__)


class _HTTPRequest(_BaseHTTPRequestHandler):
    def __init__(self, request_text):
        self.rfile = io.BytesIO(request_text)
        self.raw_requestline = self.rfile.readline()
        self.error_code = self.error_message = None
        self.parse_request()


class _WebserverMonitor(object):
    def __init__(self, filepath):
        self.__filepath = filepath

        # Allows us to be in sync when starting and stopping the thread.
        self.__server_state_e = threading.Event()

        self.__t = threading.Thread(target=self.__thread)
        self._port = None

        # Signaled when the authorization response is received.
        self._request_state_e = threading.Event()

        # Will be assigned with the response from Google.
        self._http_status_raw = None

    def start(self):
        self.__t.start()

        # Wait for the loop to change the event state.
        _LOGGER.debug("Waiting for thread to start.")
        self.__server_state_e.wait()

        _LOGGER.debug("Server is now running.")

        self.__server_state_e.clear()

    def stop(self):
        assert \
            self.__server_state_e is not None, \
            "Thread doesn't appear to have ever been started."

        assert \
            self.__t.is_alive() is True, \
            "Thread doesn't appear to be running."

        self.__server_state_e.clear()
        self.__s.shutdown()

        # Wait for the loop to change the event state.
        _LOGGER.debug("Waiting for thread to stop.")
        self.__server_state_e.wait()

        _LOGGER.debug("Server is no longer running.")

        self.__server_state_e.clear()

    def __thread(self):
        """Where the main loop lives."""

        _LOGGER.debug("Webserver is starting.")

        monitor = self

        # Embedding this because it's so trivial.
        class Handler(_BaseHTTPRequestHandler):
            def do_GET(self):

                # We have the first line of the response with the authorization code
                # passed as a query argument.
                #
                # Example:
                #
                # GET /?code=4/clwm0rESq8sqeC-JxIcfiSdjh2593hLej9CZxAcbe1A HTTP/1.1
                #

                # Use Python to parse the request. We need to add one newline for the
                # line and another for a subsequent blank line to terminate the block
                # and conform with the RFC.
                hr = _HTTPRequest(self.requestline.encode() + b"\n\n")
                arguments = arguments_from_url(hr.path)

                # It's not an authorization response. Bail with the same error
                # the library would normally send for unhandled requests.
                if 'code' not in arguments:
                    self.send_error(
                        501,
                        "Unsupported method ({}): {}".format(
                        self.command, hr.path))

                    return

                authcode = arguments['code'][0]
                _LOGGER.debug("Received authcode [{}]".format(authcode))

                monitor._authcode = authcode

                monitor._request_state_e.set()

                self.send_response(200, message='OK')

                self.send_header("Content-type", 'text/html')
                self.end_headers()

                self.wfile.write(b"""\
<html>
<head></head>
<body>
GDFS authorization recorded.
</body>
</html>
""")

            def log_message(self, format, *args):
                pass


        class Server(socketserver.TCPServer):
            def server_activate(self, *args, **kwargs):
                r = socketserver.TCPServer.server_activate(self, *args, **kwargs)

                # Sniff the port, now that we're running.
                monitor._port = self.server_address[1]

                return r

        # Our little webserver. (0) for the port will automatically assign it
        # to some unused port.
        binding = ('localhost', 0)
        self.__s = Server(binding, Handler)

        _LOGGER.debug("Created server.")

        # Signal the startup routine that we're starting.
        self.__server_state_e.set()

        _LOGGER.debug("Running server.")
        self.__s.serve_forever()

        _LOGGER.debug("Webserver is stopping.")

        # Signal the startup routine that we're stopping.
        self.__server_state_e.set()

    @property
    def port(self):
        assert \
            self._port is not None, \
            "Thread hasn't been started or a port hasn't been assigned."

        return self._port

    @property
    def request_state_e(self):
        return self._request_state_e

    @property
    def authcode(self):
        return self._authcode


class AutoAuth(object):
    """Knows how to open the browser, authorize the application (prompting the
    user if necessary), redirect, receive the response, and store the
    credentials.
    """

    def get_and_write_creds(self):
        _LOGGER.info("Requesting authorization.")

        creds_filepath = gdrivefs.conf.Conf.get('auth_cache_filepath')
        wm = _WebserverMonitor(creds_filepath)

        # Start the webserver.
        wm.start()

        # Open a browser window to request authorization.

        redirect_uri = 'http://localhost:{}'.format(wm.port)
        oa = gdrivefs.oauth_authorize.OauthAuthorize(
                redirect_uri=redirect_uri)

        url = oa.step1_get_auth_url()
        _LOGGER.debug("Opening browser: [{}]".format(url))

        webbrowser.open(url)

        # Wait for the response from Google. We implement this as a loop rather
        # than a blocking call so that the user can terminate this with a
        # simple break (in contract, blocking on an event makes us
        # unresponsive).

        try:
            while 1:
                if wm.request_state_e.is_set() is True:
                    break

                time.sleep(1)
        except:
            raise
        else:
            authcode = wm.authcode
        finally:
            # Shutdown the webserver.
            wm.stop()

        # Finish the authorization from our side and record.
        oa.step2_doexchange(authcode)

        _LOGGER.info("Authorization complete.")
