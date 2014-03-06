import gevent
import multiprocessing

from multiprocessing import Process, Manager, Queue
from Queue import Empty
from time import time
from threading import Lock
from collections import namedtuple
from os.path import join, exists, basename
from os import makedirs, stat, utime
from datetime import datetime
from glob import glob

from gevent.pool import Pool

from apiclient.http import MediaIoBaseDownload

from gdrivefs.config import download_agent
from gdrivefs.http_pool import HttpPool
from gdrivefs.utility import utility
from gdrivefs.gdtool.chunked_download import ChunkedDownload
from gdrivefs.gdtool.drive import GdriveAuth

DownloadRequest = namedtuple('DownloadRequestInfo', 
                             ['typed_entry', 'url', 'bytes', 
                              'current_mtime_dt'])


class DownloadAgentDownloadException(Exception):
    pass

class DownloadAgentDownloadError(DownloadAgentDownloadException):
    pass

class DownloadAgentDownloadAgentError(DownloadAgentDownloadError):
    pass

class DownloadAgentDownloadWorkerError(DownloadAgentDownloadError):
    pass


class _DownloadAgent(object):
    """Exclusively manages downloading files from Drive within another process.
    """
# TODO(dustin): We'll have to use multiprocessing's logging wrappers.
    def __init__(self, request_q, stop_ev):
        self.__request_q = request_q
        self.__stop_ev = stop_ev
        self.__kill_ev = gevent.event.Event()
        self.__worker_pool = Pool(size=download_agent.NUM_WORKERS)
        self.__http_pool = HttpPool(download_agent.HTTP_POOL_SIZE)
        self.__http = GdriveAuth().get_authed_http()

    def download_worker(self, download_request, request_ev, download_stop_ev, 
                        ns):
# TODO(dustin): We're just assuming that we can signal a multiprocessing event
#               from a green thread (the event still has value switching 
#               through green threads.

        file_path = ('/tmp/gdrivefs/downloaded/%s' % 
                     (download_request.typed_entry.entry_id))

        with open(file_path, 'wb') as f:
            downloader = ChunkedDownload(f, 
                                         self.__http, 
                                         download_request.url, 
                                         chunksize=download_agent.CHUNK_SIZE)

        try:
            while 1:
                # Stop downloading because the process is coming down.
                if self.__kill_ev.is_set() is True:
                    raise DownloadAgentDownloadWorkerError(
                        "Download worker terminated.")

                # Stop downloading this file, probably because all handles were 
                # closed.
                if download_stop_ev.is_set() is True:
                    raise DownloadAgentDownloadWorkerError(
                        "Download worker was told to stop downloading.")

# TODO(dustin): We'll have to provide an option for "revision assurance" to ensure that we download the same revision of a file from chunk to chunk. Otherwise, we won't have the guarantee.

# TODO(dustin): Support reauthing, when necessary.
# TODO(dustin): Support resumability.

                status, done = downloader.next_chunk()
                ns.bytes_written = status.resumable_progress

                if done is True:
                    break

# TODO(dustin): Finish this, and make sure the timezone matches the current system.
            mtime_epoch = 0#download_request.current_mtime_dt
            utime(file_path, (mtime_epoch, mtime_epoch))

        except Exception as e:
            error = ("[%s] %s" % (e.__class__.__name__, str(e)))
        else:
            error = None

        ns.error = error
        if error is None:
            ns.file_path = file_path

        request_ev.set()

    def loop(self):
        while self.__stop_ev.is_set() is False:
            try:
                request_info = self.__request_q.get(
                    timeout=download_agent.REQUEST_QUEUE_TIMEOUT_S)
            except Empty:
                continue

            if self.__worker_pool.free_count() == 0:
                logging.warn("It looks like we'll have to wait for a download "
                             "worker to free up.")

            self.__worker_pool.spawn(self.download_worker, *request_info)

        # The download loop has exited (we were told to stop).

        # Signal the workers to stop what they're doing.

        self.__kill_ev.set()
        start_epoch = time()
        all_exited = False
        while (time() - start_epoch) < 
                download_agent.GRACEFUL_WORKER_EXIT_WAIT_S:
            if self.__worker_pool.size <= self.__worker_pool.free_count():
                all_exited = True
                break

        if all_exited is False:
            logging.error("Not all download workers exited in time: %d != %d" % 
                          (self.__worker_pool.size,
                           self.__worker_pool.free_count()))

        # Kill and join the unassigned (and stubborn, still-assigned) workers.
# TODO(dustin): We're assuming this is a hard kill that will always kill all workers.
        self.__worker_pool.kill()

        logging.info("Download agent is terminating. (%d) requested files "
                     "will be abandoned." % (self.__request_q.qsize()))

def _agent_boot(request_q, stop_ev):
    """Boots the agent once it's given its own process."""

    agent = _DownloadAgent(request_q, stop_ev)
    agent.loop()


class _DownloadAgentExternal(object):
    """A class that runs in the same process as the rest of the application."""

    def __init__(self):
        self.__p = None
    	self.__m = Manager()

        self.__request_q = Queue()
        self.__request_loop_ev = multiprocessing.Event()
        
        self.__request_registry_context = { }
        self.__request_registry_locker = Lock()

        makedirs(download_agent.DOWNLOAD_PATH)

    def start(self):
        if self.__p is not None:
            raise ValueError("The download-worker is already started.")

        args = (self.__request_q, 
                self.__request_loop_ev)

        self.__p = Process(target=agent_boot, args=args)
        self.__p.start()

    def stop(self):
        if self.__p is None:
            raise ValueError("The download-worker is already stopped.")

        self.__request_loop_ev.set()
        
        start_epoch = time()
        is_exited = False
        while (time() - start_epoch) < \
                download_agent.GRACEFUL_WORKER_EXIT_WAIT_S:
            if self.__p.is_alive() is False:
                is_exited = True
                break

        if is_exited is False:
            logging.error("Download agent did not exit in time (%d)." %
                          (download_agent.GRACEFUL_WORKER_EXIT_WAIT_S))

            self.__p.terminate()

        self.__p.join()
        
        logging.info("Download agent exited with code: %d" % 
                     (self.__p.exitcode))
        
        self.__p = None

    def stop_sync(self, typed_entry):
        """If all handles for a file have been closed, then there's no reason 
        to keep downloading a file. Stop immediately, even though there might, 
        inexplicately, still be readers. This will stop the download, and 
        result in an error being sent down to watchers.
        """

        with self.__request_registry_locker:
            # Don't error out if the file isn't being downloaded (it might be 
            # fully available).
            if typed_entry not in self.__request_registry_context:
                return

            context = self.__request_registry_context[typed_entry]
            
            download_stop_ev = context['download_stop_event']
            download_stop_ev.set()

    def __find_stored_files_for_entry(self, entry_id):
        pattern = ('%s:*' % (utility.make_safe_for_filename(entry_id)))
        full_pattern = join(download_agent.DOWNLOAD_PATH, filename)

        return [basename(file_path) for file_path in glob(full_pattern)]

    def __get_stored_file_path(self, typed_entry):
        filename = ('%s:%s' % (
                    utility.make_safe_for_filename(typed_entry.entry_id), 
                    utility.make_safe_for_filename(
                        typed_entry.mime_type.lower())))

        return join(download_agent.DOWNLOAD_PATH, filename)

    def get_physical_access(self, typed_entry, expected_mtime_dt, bytes=None):
# TODO(dustin): Finish. The open-file machanism needs to call here first in order to get the file-path. By doing so, we keep a reference count. If we get a signal from Google that the file has changed, we can fault the current file, allow for sync_to_local to be called, and throw an exception if something tries to read/write here for the old mtime.
        raise NotImplementedError()

    def sync_to_local(self, download_request):
        """Send the download request to the download-agent, and wait for it to
        finish.
        """

        # We keep a registry/index of everything we're downloading so that all
        # subsequent attempts to download the same file will block on the same
        # request.

        typed_entry = download_request.typed_entry
        file_path = self.__get_stored_file_path(typed_entry)

# TODO(dustin): Do a check here for whether we already have accurate data available. This will include a) checking that the mtimes match, and whether there's a tracking stamp indicating the last mtime that the file was downloaded for, and b) the state of that download (partial/complete). We might be able to resume the download (resume is supported by the Drive API).

        if exists(file_path):
            mtime_dt = datetime.fromtimestamp(stat(file_path).st_mtime)

        # The file is downloaded, and still accurate.
# TODO(dustin): We still have to check the state of the file to determine if it's all there.
# TODO(dustin): If the file already exists, check that a) the mtime from the entry matches, and b) the requested number of bytes are available locally (even if it's a partial file).
            if mtime_dt == download_request.current_mtime_dt:
# TODO(dustin): We need to return the same resource as get_physical_access().
                return None

            if mtime_dt > download_request.current_mtime_dt:
                logging.warn("The modified-time [%s] of the locally "
                             "available file is greater than the "
                             "requested file [%s]." % 
                             (mtime_dt, download_request.current_mtime_dt))

        with self.__request_registry_locker:
            if typed_entry not in self.__request_registry_context:
                finish_ev = self.__m.Event()
                download_stop_ev = self.__m.Event()
                ns = self.__m.Namespace()
                ns.bytes_written = 0

                self.__request_registry_context[typed_entry] = {
                            'watchers': 1,
                            'finish_event': finish_ev,
                            'download_stop_event': download_stop_ev,
                            'ns': ns,
                            'file_path': file_path
                        }
            else:
                context = self.__request_registry_context[typed_entry]

                context['watchers'] += 1
                finish_ev = context['finish_event']
                download_stop_ev = context['download_stop_event']
                ns = context['ns']

                # Push the request to the worker process.
                self.__request_q.put((download_request, 
                                      finish_ev, 
                                      download_stop_ev, 
                                      ns))

        # Wait until the file is downloaded. Allow for watchers to cut-out 
        # after a specific number of bytes has been downloaded.

        while 1:
            is_done = finish_ev.wait(download_agent.REQUEST_WAIT_PERIOD_S)
            
            if ns.error is not None:
                raise DownloadAgentDownloadAgentError(
                    "Download failed for [%s]: %s" % 
                    (typed_entry.entry_id, ns.error))

            elif is_done is True:
                break

            elif download_request.bytes is not None and \
                 ns.bytes_received >= download_request.bytes:
                break

        # Decrement the reference count.
# TODO(dustin): Weak-references may reduce code here.
        with self.__request_registry_locker:
            context = self.__request_registry_context[typed_entry]
            context['watchers'] -= 1

            # If we're the last watcher, remove the entry. Note that the file
            # may still be downloading at the worker. This just manages 
            # request-concurrency.
            if context['watchers'] <= 0:
                del self.__request_registry_context[typed_entry]

# TODO(dustin): We need to return the same resource as get_physical_access(), but we need to maintain a reference count that will allow us to go right from verifying that the file is there and accurate for each viewing party to incrementing a reference count that will ONLY ever get incremented when the data is still accurate. We might need to acquire a separate lock while still within the registry one, here.
        return None

    def notify_changed_mtime(self, entry_id, mtime_dt):
# TODO(dustin): Receive a notification that the mtime for an entry has changed. We may have no downloaded data for it and might ignore it. Else, mark all of the accessors for it as faulted. Accessor objects of every mime-type that has been downloaded will have to see the mtime difference and throw an exception on read/write.

        raise NotImplementedError()

download_agent_external = _DownloadAgentExternal()

