import logging
import time
import random

try:
  from oauth2client import util
except ImportError:
  from oauth2client import _helpers as util
import apiclient.http
import apiclient.errors

DEFAULT_CHUNK_SIZE = 1024 * 512

_logger = logging.getLogger(__name__)

# TODO(Dustin): Refactor this to be nice. It's largely just copy+pasted.


class ChunkedDownload(object):
    """"Download an entry, chunk by chunk. This code is mostly identical to
    MediaIoBaseDownload, which couldn't be used because we have a specific URL
    that needs to be downloaded (not a request object, which doesn't apply here).
    """

    @util.positional(4)
    def __init__(self, fd, http, uri, chunksize=DEFAULT_CHUNK_SIZE, start_at=0):
        """Constructor.

        Args:
          fd: io.Base or file object, The stream in which to write the downloaded
            bytes.
          http: The httplib2 resource.
          uri: The URL to be downloaded.
          chunksize: int, File will be downloaded in chunks of this many bytes.
        """

        self._fd = fd
        self._http = http
        self._uri = uri
        self._chunksize = chunksize
        self._progress = start_at
        self._total_size = None
        self._done = False

        # Stubs for testing.
        self._sleep = time.sleep
        self._rand = random.random

    @util.positional(1)
    def next_chunk(self, num_retries=0):
        """Get the next chunk of the download.

        Args:
          num_retries: Integer, number of times to retry 500's with randomized
                exponential backoff. If all retries fail, the raised HttpError
                represents the last request. If zero (default), we attempt the
                request only once.

        Returns:
          (status, done): (MediaDownloadStatus, boolean)
             The value of 'done' will be True when the media has been fully
             downloaded.

        Raises:
          apiclient.errors.HttpError if the response was not a 2xx.
          httplib2.HttpLib2Error if a transport error has occured.
        """

        headers = {
            'range': 'bytes=%d-%d' % (
                self._progress, self._progress + self._chunksize)
            }

        for retry_num in xrange(num_retries + 1):
            _logger.debug("Attempting to read chunk. ATTEMPT=(%d)/(%d)", 
                          retry_num + 1, num_retries + 1)

            if retry_num > 0:
                self._sleep(self._rand() * 2**retry_num)
                _logger.warning("Retry #%d for media download: GET %s, "
                                "following status: %d", 
                                retry_num, self._uri, resp.status)

            resp, content = self._http.request(self._uri, headers=headers)
            if resp.status < 500:
                break

        _logger.debug("Received chunk of size (%d).", len(content))

        if resp.status in [200, 206]:
            try:
                if resp['content-location'] != self._uri:
                    self._uri = resp['content-location']
            except KeyError:
                pass

            received_size_b = len(content)
            self._progress += received_size_b
            self._fd.write(content)

            # This seems to be the most correct method to get the filesize, but 
            # we've seen it not exist.
            if 'content-range' in resp:
                if self._total_size is None:
                    content_range = resp['content-range']
                    length = content_range.rsplit('/', 1)[1]
                    length = int(length)

                    self._total_size = length

                    _logger.debug("Received download size (content-range): "
                                  "(%d)", self._total_size)

            # There's a chance that "content-range" will be omitted for zero-
            # length files (or maybe files that are complete within the first 
            # chunk).

            else:
# TODO(dustin): Is this a valid assumption, or should it be an error?
                _logger.warning("No 'content-range' found in response. "
                                "Assuming that we've received all data.")

                self._total_size = received_size_b

# TODO(dustin): We were using this for a while, but it appears to be no larger 
#               then a single chunk.
#
#            # This method doesn't seem documented, but we've seen cases where 
#            # this is available, but "content-range" isn't.
#            if 'content-length' in resp:
#                self._total_size = int(resp['content-length'])
#
#                _logger.debug("Received download size (content-length): "
#                              "(%d)", self._total_size)


            assert self._total_size is not None, \
                   "File-size was not provided."

            _logger.debug("Checking if done. PROGRESS=(%d) TOTAL-SIZE=(%d)", 
                          self._progress, self._total_size)

            if self._progress == self._total_size:
                self._done = True

            return (apiclient.http.MediaDownloadProgress(
                        self._progress, 
                        self._total_size), \
                    self._done, \
                    self._total_size)
        else:
            raise apiclient.errors.HttpError(resp, content, uri=self._uri)
