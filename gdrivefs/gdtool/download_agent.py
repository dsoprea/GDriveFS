"""This file describes the communication interface to the download-worker, and
the download-worker itself. Both are singleton classes.
"""

import gevent
import multiprocessing

from multiprocessing import Process, Manager, Queue
from Queue import Empty
from time import time, mktime
from threading import Lock
from collections import namedtuple
from os.path import join, exists, basename
from os import makedirs, stat, utime, unlink
from datetime import datetime
from glob import glob
from contextlib import contextmanager

from gevent.pool import Pool

from apiclient.http import MediaIoBaseDownload

from gdrivefs.config import download_agent
from gdrivefs.http_pool import HttpPool
from gdrivefs.utility import utility
from gdrivefs.gdtool.chunked_download import ChunkedDownload
from gdrivefs.gdtool.drive import GdriveAuth

DownloadRequest = namedtuple('DownloadRequestInfo', 
                             ['typed_entry', 'url', 'bytes', 
                              'expected_mtime_dt'])


class DownloadAgentDownloadException(Exception):
    pass

class DownloadAgentDownloadError(DownloadAgentDownloadException):
    pass

class DownloadAgentDownloadAgentError(DownloadAgentDownloadError):
    pass

class DownloadAgentDownloadWorkerError(DownloadAgentDownloadError):
    pass

class DownloadAgentResourceFaultedException(DownloadAgentDownloadException):
    pass


class _DownloadedFileState(object):
    """This class is in charge of knowing where to store downloaded files, and
    how to check validness.
    """

    def __init__(self, download_request):
        self.__file_marker_locker = Lock()

        self.__typed_entry = download_request.typed_entry
        self.__file_path = self.__get_stored_filepath()
        self.__stamp_file_path = self.__get_downloading_stamp_filepath()

        self.__expected_mtime_dt = download_request.expected_mtime_dt
        self.__expected_mtime_epoch = \
            mktime(self.__expected_mtime_dt.timetuple())

    def is_up_to_date(self, bytes=None):
        with self.__file_marker_locker:
            # If the requested file doesn't exist, at all, we're out of luck.
            if exists(self.__file_path) is False:
                return False

            # If the mtime of the requested file matches, we have the whole
            # thing (the mtime can only be set after the file has been 
            # completely written).

            main_stat = stat(self.__file_path)
            mtime_dt = datetime.fromtimestamp(main_stat.st_mtime)

            if mtime_dt == self.__expected_mtime_dt:
                return True

            if mtime_dt > self.__expected_mtime_dt:
                logging.warn("The modified-time [%s] of the locally "
                             "available file is greater than the "
                             "requested file [%s]." % 
                             (mtime_dt, self.__expected_mtime_dt))

            # If they want the whole file (not just a specific number of 
            # bytes), then we definitely don't have it.
            if bytes is None:
                return False

            # The file is not up to date, but check if we're, downloading it, 
            # at least.

            if exists(self.__stamp_file_path) is False:
                return False

            # Determine if we're downloading (or recently attempted to 
            # download) the same version that was requested.

            stamp_stat = stat(self.__stamp_file_path)
            stamp_mtime_dt = datetime.fromtimestamp(
                                stamp_stat.st_mtime)

            if stamp_mtime_dt != self.__expected_mtime_dt:
                if stamp_mtime_dt > self.__expected_mtime_dt:
                    logging.warn("The modified-time [%s] of the locally "
                                 "available file's STAMP is greater than the "
                                 "requested file [%s]." % 
                                 (stamp_mtime_dt, self.__expected_mtime_dt))
                return False

            # We were/are downloading the right version. Did we download enough 
            # of it?
            if main_stat.st_size < bytes:
                return False

        # We haven't downloaded the whole file, but we've downloaded enough.
        return True

    def get_partial_offset(self):
        with self.__file_marker_locker:
            if exists(self.__file_path) is False:
                return 0

            main_stat = stat(self.__file_path)
            mtime_dt = datetime.fromtimestamp(main_stat.st_mtime)

            # Assume that if the "downloading" stamp isn't present, the file is 
            # completely downloaded.
            if exists(self.__stamp_file_path) is False:
                return None

            # Determine if we're downloading (or recently attempted to 
            # download) the same version that was requested.

            stamp_stat = stat(self.__stamp_file_path)
            stamp_mtime_dt = datetime.fromtimestamp(
                                stamp_stat.st_mtime)

            if stamp_mtime_dt != self.__expected_mtime_dt:
                return 0

            # If we're in the middle of downloading the right version, return 
            # the current size (being the start offset of a resumed download).
            return main_stat.st_size

    def __get_stored_filename(self):
        filename = ('%s:%s' % (
                    utility.make_safe_for_filename(
                        self.__typed_entry.entry_id), 
                    utility.make_safe_for_filename(
                        self.__typed_entry.mime_type.lower())))

        return filename

    def __get_stored_filepath(self):
        filename = self.__get_stored_filename()
        return join(download_agent.DOWNLOAD_PATH, filename)

    def __get_downloading_stamp_filename(self):
        filename = self.__get_stored_filename(self.__typed_entry)
        stamp_filename = ('.%s.%s' % 
                          (filename, FILE_STATE_STAMP_SUFFIX_DOWNLOADING))

        return stamp_filename

    def __get_downloading_stamp_filepath():
        stamp_filename = self.__get_downloading_stamp_filename()
        return join(download_agent.DOWNLOAD_PATH, stamp_filename)

    def stage_download(self):
        """Called before a download has started."""
    
        # Initialize our start state. This ensures that any concurrent
        # requests can read partial data without having to wait for the
        # whole download.
        with self.__file_marker_locker:
            try:
                stamp_stat = stat(self.__stamp_file_path)
            except OSError:
                existing_mtime_epoch = None
            else:
                existing_mtime_epoch = stamp_stat.st_mtime

            # THEN create a stamp file...
            with open(self.__stamp_file_path, 'w'):
                pass

            # ...and set its mtime.
# TODO(dustin): Make sure the timezone matches the current system.
            utime(self.__stamp_file_path, (self.__expected_mtime_epoch, 
                                           self.__expected_mtime_epoch))

            # If we either didn't have a stamp file or or we did and the mtime 
            # doesn't match, create an empty download file or truncate the 
            # existing.
            if self.__expected_mtime_epoch != existing_mtime_epoch:
                with open(self.__file_path, 'w'):
                    pass

    def finish_download(self):
        """Called after a download has completed."""

        with self.__file_marker_locker:
            utime(self.__file_path, (self.__expected_mtime_epoch, 
                                     self.__expected_mtime_epoch))
# TODO(dustin): Make sure the timezone matches the current system.
            unlink(self.__stamp_file_path)

    @property
    def file_path(self):
        return self.__file_path


class _DownloadAgent(object):
    """Exclusively manages downloading files from Drive within another process.
    This is a singleton class (and there's only one worker process).
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

# TODO(dustin): Support reauthing, when necessary.

        # This will allow us to determine how up to date we are, as well as to
        # to resume an existing, partial download (if possible).
        dfs = _DownloadedFileState(download_request)

        error = None
        if dfs.is_up_to_date() is False:
            dfs.stage_download()

            with open(dfs.file_path, 'wb') as f:
                try:
                    downloader = ChunkedDownload(
                        f, 
                        self.__http, 
                        download_request.url, 
                        chunksize=download_agent.CHUNK_SIZE,
                        start_at=dfs.get_partial_offset())

                    while 1:
                        # Stop downloading if the process is coming down.
                        if self.__kill_ev.is_set() is True:
                            raise DownloadAgentDownloadWorkerError(
                                "Download worker terminated.")

                        # Stop downloading this file, prhaps if all handles 
                        # were closed and the file is no longer needed.
                        if download_stop_ev.is_set() is True:
                            raise DownloadAgentDownloadWorkerError(
                                "Download worker was told to stop downloading.")

                        status, done = downloader.next_chunk()
                        ns.bytes_written = status.resumable_progress

                        if done is True:
                            break

                    dfs.finish_download()
                except Exception as e:
                    error = ("[%s] %s" % (e.__class__.__name__, str(e)))

        ns.error = error
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


class _SyncedResource(object):
    """This is the singleton object stored within the external agent that is
    flagged if/when a file is faulted."""

    def __init__(self, external_download_agent, entry_id, resource_key):
        self.__eda = external_download_agent
        self.__entry_id = entry_id
        self.__key = resource_key
        self.__handles = []

    def register_handle(self, handle):
        """Indicate that one handle is ready to go."""

        self.__handles.append(handle)

    def decr_ref_count(self):
        """Indicate that one handle has been destroyed."""

        self.__eda.deregister_resource(self)

    def set_faulted(self):
        """Tell each of our handles that they're no longer looking at an 
        accurate/valid copy of the file.
        """

        for handle in self.__handles:
            handle.set_fault()

    @property
    def entry_id(self):
        return self.__entry_id

    @property
    def key(self):
        return self.__key
    

class _SyncedResourceHandle(object):
    """This object:
    
    - represents access to a synchronized file
    - will raise a DownloadAgentResourceFaultedException if the file has been 
      changed.
    - is the internal resource that will be associated with a physical file-
      handle.
    """

    def __init__(self, resource, download_request):
        self.__resource = resource
        self.__download_request = download_request

        # Start off opened.
        self.__open = True

        self.__is_faulted = False

        self.__resource.register_handle(self)

    def set_faulted(self):
        """Indicate that another sync operation will have to occur in order to
        continue reading."""

        self.__is_faulted = True

    def __check_state(method):
        def wrap(self, *args, **kwargs):
            if self.__is_faulted is True:
                raise DownloadAgentResourceFaultedException()

            return method(self, *args, **kwargs)
        
        return wrap

    def __del__(self):
        if self.__open is True:
            self.close()

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()
        
# TODO(dustin): Implement these.
    @__check_state
    def close(self):
        self.__open = False
        self.__resource.decr_ref_count()

        raise NotImplementedError()
    
    @__check_state
    def flush(self):
        raise NotImplementedError()
    
    @__check_state
    def next(self):
        raise NotImplementedError()
    
    @__check_state
    def read(self):
        raise NotImplementedError()
    
    @__check_state
    def readline(self):
        raise NotImplementedError()
    
    @__check_state
    def readlines(self):
        raise NotImplementedError()
    
    @__check_state
    def seek(self):
        raise NotImplementedError()
    
    @__check_state
    def tell(self):
        raise NotImplementedError()
    
    @__check_state
    def truncate(self):
        raise NotImplementedError()
    
    @__check_state
    def write(self):
        raise NotImplementedError()
    
    @__check_state
    def writelines(self):
        raise NotImplementedError()


class _DownloadAgentExternal(object):
    """A class that runs in the same process as the rest of the application, 
    and acts as an interface to the worker process. This is a singleton.
    """

    def __init__(self):
        self.__p = None
    	self.__m = Manager()

        self.__request_q = Queue()
        self.__request_loop_ev = multiprocessing.Event()
        
        self.__request_registry_context = { }
        self.__request_registry_types = { }
        self.__request_registry_locker = Lock()

        # [entry_id] = [resource, counter]
        self.__accessor_resources = {}
        self.__accessor_resources_locker = Lock()

        makedirs(download_agent.DOWNLOAD_PATH)

    def deregister_resource(self, resource):
        """Called at the end of a file-resource's lifetime (on close)."""

        with self.__accessor_resources_locker:
            self.__accessor_resources[resource.key][1] -= 1
            
            if self.__accessor_resources[resource.key][1] <= 0:
                del self.__accessor_resources[resource.key]

    def __get_resource(self, entry_id, expected_mtime_dt):
        """Get the file resource and increment the reference count. Note that
        the resources are keyed by entry-ID -and- mtime, so the moment that
        an entry is faulted, we can fault the resource for all of the current
        handles while dispensing new handles with an entirely-new resource.
        """

        key = ('%s-%s' % (entry_id, 
                          mktime(expected_mtime_dt.timetuple())))

        with self.__accessor_resources_locker:
            try:
                self.__accessor_resources[key][1] += 1
                return self.__accessor_resources[key][0]
            except KeyError:
                resource = _SyncedResource(self, entry_id, key)
                self.__accessor_resources[key] = [resource, 1]
                
                return resource

    def set_resource_faulted(self, entry_id):
        """Called when a file has changed. Will notify any open file-handle 
        that the file has faulted.
        """

        with self.__accessor_resources_locker:
            try:
                self.__accessor_resources[entry_id][0].set_faulted()
            except KeyError:
                pass

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

# TODO(dustin): We still need to consider removing faulted files (at least if
#               not being actively engaged/watched).

# TODO(dustin): We need to consider periodically pruning unaccessed, localized
#               files.
    def __find_stored_files_for_entry(self, entry_id):
        pattern = ('%s:*' % (utility.make_safe_for_filename(entry_id)))
        full_pattern = join(download_agent.DOWNLOAD_PATH, filename)

        return [basename(file_path) for file_path in glob(full_pattern)]

    @contextmanager
    def sync_to_local(self, download_request):
        """Send the download request to the download-agent, and wait for it to
        finish. If the file is already found locally, and the number of request 
        bytes is up to date, a download will not be performed. All requests
        will funnel here, and all requests for the same entry/mime-type 
        combination will essentially be synchronized, while they all are given
        the discretion to render a result (a yield) as soon as the requested
        number of bytes is available.
        
        If/when notify_changed() is called, all open handles to any mime-type 
        of that entry will be faulted, and all downloads will error out in a
        way that can be caught and restarted.
        """

        # We keep a registry/index of everything we're downloading so that all
        # subsequent attempts to download the same file will block on the same
        # request.
        #
        # We'll also rely on the separate process to tell us if the file is
        # already local and up-to-date (everything is simpler/cleaner that
        # way), but will also do a ceck, ourselves, once we've submitted an
        # official request for tracking/locking purposes.

        typed_entry = download_request.typed_entry
        dfs = _DownloadedFileState(download_request)

        with self.__request_registry_locker:
            try:
                context = self.__request_registry_context[typed_entry]
            except KeyError:
                # This is the first download of this entry/mime-type.

                finish_ev = self.__m.Event()
                download_stop_ev = self.__m.Event()
                ns = self.__m.Namespace()
                ns.bytes_written = 0

                self.__request_registry_context[typed_entry] = {
                            'watchers': 1,
                            'finish_event': finish_ev,
                            'download_stop_event': download_stop_ev,
                            'ns': ns
                        }
            else:
                # There was already another download of this entry/mime-type.

                context['watchers'] += 1
                finish_ev = context['finish_event']
                download_stop_ev = context['download_stop_event']
                ns = context['ns']

                # Push the request to the worker process.
                self.__request_q.put((download_request, 
                                      finish_ev, 
                                      download_stop_ev, 
                                      ns))

            try:
                # We weren't already download this entry for any mime-type.
                self.__request_registry_types[typed_entry.entry_id] = \
                    set([typed_entry])
            except KeyError:
                # We were already downloading this entry. Make sure we're in 
                # the list of mime-types being downloaded.
                self.__request_registry_types[typed_entry.entry_id].add(
                    typed_entry)
        try:
            # Though we've now properly submitted a download/verification 
            # request, only wait for download progress if necessary. If we 
            # already have the whole file, the worker should just signal as 
            # much, anyways. If we're still downloading the file, it only 
            # matters if we have enough of it. If something happens and that 
            # file is no longer valid, the file-resource will still be faulted 
            # correctly.

            if dfs.is_up_to_date(download_request.bytes) is False:
                while 1:
                    is_done = finish_ev.wait(download_agent.REQUEST_WAIT_PERIOD_S)

                    if ns.error is not None:
# TODO(dustin): We need to catch and raise a specific exception when the 
#               download errored out due to faulting, so that our caller can 
#               restart us.
                        raise DownloadAgentDownloadAgentError(
                            "Download failed for [%s]: %s" % 
                            (typed_entry.entry_id, ns.error))

                    elif is_done is True:
                        break

                    elif download_request.bytes is not None and \
                         ns.bytes_received >= download_request.bytes:
                        break

            # We've now downloaded enough bytes, or already had enough bytes 
            # available.
            #
            # There are two reference-counts at this point: 1) the number of 
            # threads that are watching/accessing a particular mime-type of the
            # given entry (allows the downloads to be synchronized), and 2) the 
            # number of threads that have handles to an entry, regardless of 
            # mime-type (can be faulted when the content changes).

# TODO(dustin): This resource still needs to know what typed-file and file-path were requested.
            resource = self.__get_resource(typed_entry.entry_id, 
                                           download_request.expected_mtime_dt)
            yield _SyncedResourceHandle(resource)
        finally:
            # Decrement the reference count.
            with self.__request_registry_locker:
                context = self.__request_registry_context[typed_entry]
                context['watchers'] -= 1

                # If we're the last watcher, remove the entry. Note that the 
                # file may still be downloading at the worker. This just 
                # manages request-concurrency.
                if context['watchers'] <= 0:
                    # In the event that all of the requests weren't concerned 
                    # with getting the whole file, send a signal to stop 
                    # downloading.
                    download_stop_ev.set()

                    # Remove registration information. If the worker is still
                    # downloading the file, it'll have a reference/copy of the
                    # event above.
                    del self.__request_registry_context[typed_entry]

                    self.__request_registry_types[typed_entry.entry_id].pop(
                        typed_entry)

                    if not self.__request_registry_types[typed_entry.entry_id]:
                        del self.__request_registry_types[typed_entry.entry_id]

    def notify_changed(self, entry_id, mtime_dt):
        """Invoked by another thread when a file has changed, most likely by 
        information reported by the "changes" API. At this time, we don't check 
        whether anything has actually ever accessed this entry... Just whether 
        something currently has it open. It's essentially the same, and cheap.
        """

        self.set_resource_faulted(entry_id)

        with self.__request_registry_locker:
            try:
                types = self.__request_registry_types[entry_id]
            except KeyError:
                pass
            else:
                # Stop downloads of all active mime-types for the given entry.
                for typed_entry in types:
                    context = self.__request_registry_context[typed_entry]
                    
                    download_stop_ev = context['download_stop_event']
                    download_stop_ev.set()

download_agent_external = _DownloadAgentExternal()

