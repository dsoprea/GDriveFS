import logging

from time import time, sleep
from random import random

from oauth2client import util
from apiclient.http import MediaDownloadProgress
from apiclient.errors import HttpError

DEFAULT_CHUNK_SIZE = 1024 * 512

_logger = logging.getLogger(__name__)


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
    self._sleep = sleep
    self._rand = random

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
        if retry_num > 0:
            self._sleep(self._rand() * 2**retry_num)
            logging.warning(
                'Retry #%d for media download: GET %s, following status: %d' %
                (retry_num, self._uri, resp.status))

        resp, content = self._http.request(self._uri, headers=headers)
        if resp.status < 500:
            break

    if resp.status not in (200, 206):
        raise HttpError(resp, content, uri=self._uri)

    if 'content-location' in resp and resp['content-location'] != self._uri:
        self._uri = resp['content-location']

    self._progress += len(content)
    self._fd.write(content)

    if 'content-length' in resp:
        self._total_size = int(resp['content-length'])
    elif 'content-range' in resp:
        content_range = resp['content-range']
        length = content_range.rsplit('/', 1)[1]
        self._total_size = int(length)

    if self._progress == self._total_size:
        self._done = True

    return MediaDownloadProgress(self._progress, self._total_size), self._done

