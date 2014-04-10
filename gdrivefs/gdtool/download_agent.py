"""This file describes the communication interface to the download-worker, and
the download-worker itself. Both are singleton classes.
"""

import multiprocessing
import logging
import Queue
import time
import threading
import collections
import os
import os.path
import datetime
import glob
import contextlib
import dateutil.tz

import gevent
import gevent.lock
import gevent.pool
import gevent.monkey
import gevent.queue

from gdrivefs.config import download_agent
from gdrivefs.utility import utility
from gdrivefs.gdtool.chunked_download import ChunkedDownload
from gdrivefs.gdtool.drive import GdriveAuth

_RT_PROGRESS = 'p'
_RT_ERROR = 'e'
_RT_DONE = 'd'
_RT_THREAD_KILL = 'k'
_RT_THREAD_STOP = 's'

DownloadRegistration = collections.namedtuple(
                        'DownloadRegistration', 
                        ['typed_entry', 
                         'url', 
                         'bytes', 
                         'expected_mtime_tuple'])

DownloadRequest = collections.namedtuple(
                        'DownloadRequest', 
                        ['typed_entry', 'url', 'bytes', 'expected_mtime_dt'])


class DownloadAgentDownloadException(Exception):
    """Base exception for all user-defined functions."""

    pass


class DownloadAgentDownloadError(DownloadAgentDownloadException):
    """Base error for download errors."""

    pass


class DownloadAgentDownloadAgentError(DownloadAgentDownloadError):
    """Raised to external callers when a sync failed."""

    pass


class DownloadAgentWorkerShutdownException(DownloadAgentDownloadException):
    """Raised by download worker when it's told to shutdown."""

    pass


class DownloadAgentResourceFaultedException(DownloadAgentDownloadException):
    """Raised externally by _SyncedResourceHandle when the represented file 
    has faulted.
    """

    pass


class DownloadAgentDownloadStopException(DownloadAgentDownloadException):
    """Raised to external callers when the file being actively downloaded has 
    faulted, and must be restarted.
    """

    pass


class _DownloadedFileState(object):
    """This class is in charge of knowing where to store downloaded files, and
    how to check validness.
    """

    def __init__(self, download_reg):
        self.__typed_entry = download_reg.typed_entry
        self.__log = logging.getLogger('%s(%s)' % 
                        (self.__class__.__name__, self.__typed_entry))

        self.__file_marker_locker = threading.Lock()
        self.__file_path = self.get_stored_filepath()
        self.__stamp_file_path = self.__get_downloading_stamp_filepath()

        # The mtime should've already been adjusted to the local TZ.

        self.__expected_mtime_epoch = time.mktime(
                                        download_reg.expected_mtime_tuple)
        self.__expected_mtime_dt = datetime.datetime.fromtimestamp(
                                    self.__expected_mtime_epoch).\
                                    replace(tzinfo=dateutil.tz.tzlocal())

    def __str__(self):
        return ('<DOWN-FILE-STATE %s>' % (self.__typed_entry,))

    def is_up_to_date(self, bytes_=None):
        with self.__file_marker_locker:
            self.__log.debug('is_up_to_date()')

            # If the requested file doesn't exist, at all, we're out of luck.
            if os.path.exists(self.__file_path) is False:
                return False

            # If the mtime of the requested file matches, we have the whole
            # thing (the mtime can only be set after the file has been 
            # completely written).

            main_stat = os.stat(self.__file_path)
            mtime_dt = datetime.datetime.fromtimestamp(main_stat.st_mtime)

            if mtime_dt == self.__expected_mtime_dt:
                return True

            if mtime_dt > self.__expected_mtime_dt:
                logging.warn("The modified-time [%s] of the locally "
                             "available file is greater than the "
                             "requested file [%s].",
                             mtime_dt, self.__expected_mtime_dt)

            # If they want the whole file (not just a specific number of 
            # bytes), then we definitely don't have it.
            if bytes_ is None:
                return False

            # The file is not up to date, but check if we're, downloading it, 
            # at least.

            if os.path.exists(self.__stamp_file_path) is False:
                return False

            # Determine if we're downloading (or recently attempted to 
            # download) the same version that was requested.

            stamp_stat = os.stat(self.__stamp_file_path)
            stamp_mtime_dt = datetime.datetime.fromtimestamp(
                                stamp_stat.st_mtime)

            if stamp_mtime_dt != self.__expected_mtime_dt:
                if stamp_mtime_dt > self.__expected_mtime_dt:
                    logging.warn("The modified-time [%s] of the locally "
                                 "available file's STAMP is greater than the "
                                 "requested file [%s].",
                                 stamp_mtime_dt, self.__expected_mtime_dt)
                return False

            # We were/are downloading the right version. Did we download enough 
            # of it?
            if main_stat.st_size < bytes_:
                return False

        # We haven't downloaded the whole file, but we've downloaded enough.
        return True

    def get_partial_offset(self):
        with self.__file_marker_locker:
            self.__log.debug('get_partial_offset()')

            if os.path.exists(self.__file_path) is False:
                return 0

            main_stat = os.stat(self.__file_path)

            # Assume that if the "downloading" stamp isn't present, the file is 
            # completely downloaded.
            if os.path.exists(self.__stamp_file_path) is False:
                return None

            # Determine if we're downloading (or recently attempted to 
            # download) the same version that was requested.

            stamp_stat = os.stat(self.__stamp_file_path)
            stamp_mtime_dt = datetime.datetime.fromtimestamp(
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

    def get_stored_filepath(self):
        filename = self.__get_stored_filename()
        return os.path.join(download_agent.DOWNLOAD_PATH, filename)

    def __get_downloading_stamp_filename(self):
        filename = self.__get_stored_filename()
        stamp_filename = ('.%s.%s' % 
                          (filename, 
                           download_agent.FILE_STATE_STAMP_SUFFIX_DOWNLOADING))

        return stamp_filename

    def __get_downloading_stamp_filepath(self):
        stamp_filename = self.__get_downloading_stamp_filename()
        return os.path.join(download_agent.DOWNLOAD_PATH, stamp_filename)

    def stage_download(self):
        """Called before a download has started."""
    
        # Initialize our start state. This ensures that any concurrent
        # requests can read partial data without having to wait for the
        # whole download.
        with self.__file_marker_locker:
            self.__log.debug('stage_download()')

            try:
                stamp_stat = os.stat(self.__stamp_file_path)
            except OSError:
                existing_mtime_epoch = None
            else:
                existing_mtime_epoch = stamp_stat.st_mtime

            # THEN create a stamp file...
            with open(self.__stamp_file_path, 'w'):
                pass

            # ...and set its mtime.
            os.utime(self.__stamp_file_path, 
                     (self.__expected_mtime_epoch,) * 2)

            # If we either didn't have a stamp file or or we did and the mtime 
            # doesn't match, create an empty download file or truncate the 
            # existing.
            if self.__expected_mtime_epoch != existing_mtime_epoch:
                with open(self.__file_path, 'w'):
                    pass

    def finish_download(self):
        """Called after a download has completed."""

        with self.__file_marker_locker:
            self.__log.debug('finish_download()')

            os.utime(self.__file_path, (self.__expected_mtime_epoch,) * 2) 
            os.unlink(self.__stamp_file_path)

    @property
    def file_path(self):
        return self.__file_path


class _DownloadAgent(object):
    """Exclusively manages downloading files from Drive within another process.
    This is a singleton class (and there's only one worker process).
    """

    def __init__(self, request_q, stop_ev):
        # This patches the socket library. Only the agent needs gevent and it 
        # might interrupt multiprocessing if we put it as the top of the 
        # module.

# TODO(dustin): Using gevent in the worker is interrupting the worker's ability 
#               to communicate with the main process. Try to send a standard 
#               Python Unix pipe to the process while still using 
#               multiprocessing to manage it. 

#        gevent.monkey.patch_socket()
#        from gdrivefs.http_pool import HttpPool

        self.__log = logging.getLogger(self.__class__.__name__)
        self.__request_q = request_q
        self.__stop_ev = stop_ev
        self.__kill_ev = gevent.event.Event()
        self.__worker_pool = gevent.pool.Pool(size=download_agent.NUM_WORKERS)
#        self.__http_pool = HttpPool(download_agent.HTTP_POOL_SIZE)
#        self.__http = GdriveAuth().get_authed_http()

        # This allows multiple green threads to communicate over the same IPC 
        # resources.
        self.__ipc_sem = gevent.lock.Semaphore()

        self.__ops = {}

    def download_worker(self, download_reg, download_id, sender, receiver):
        self.__log.info("Worker thread downloading: %s" % (download_reg,))

# TODO(dustin): We're just assuming that we can signal a multiprocessing event
#               from a green thread (the event still has value switching 
#               through green threads.

# TODO(dustin): Support reauthing, when necessary.

        # This will allow us to determine how up to date we are, as well as to
        # to resume an existing, partial download (if possible).
        dfs = _DownloadedFileState(download_reg)

        if dfs.is_up_to_date() is False:
            self.__log.info("File is not up-to-date: %s" % (str(dfs)))

            dfs.stage_download()

            with open(dfs.file_path, 'wb') as f:
                try:
                    downloader = ChunkedDownload(
                        f, 
                        self.__http, 
                        download_reg.url, 
                        chunksize=download_agent.CHUNK_SIZE,
                        start_at=dfs.get_partial_offset())

                    self.__log.info("Beginning download loop: %s" % (str(dfs)))

                    while 1:
                        try:
                            (report_type, datum) = receiver.get(False)
                        except Queue.Empty:
                            pass
                        else:
                            self.__log.debug("Worker thread [%s] received "
                                             "report: [%s]" % 
                                             (download_reg, report_type))

                            if report_type == _RT_THREAD_KILL:
                                # Stop downloading if the process is coming 
                                # down.
                                self.__log.info("Download loop has been "
                                                "terminated because we're "
                                                "shutting down.")
                                raise DownloadAgentWorkerShutdownException(
                                    "Download worker terminated.")
                            elif report_type == _RT_THREAD_STOP:
                                # Stop downloading this file, prhaps if all handles 
                                # were closed and the file is no longer needed.
                                self.__log.info("Download loop has been "
                                                "terminated because we were"
                                                "told to stop (the agent is"
                                                "still running, though).")
                                raise DownloadAgentDownloadStopException(
                                    "Download worker was told to stop "
                                    "downloading.")
                            else:
                                raise ValueError("Worker thread does not "
                                                 "understand report-type: %s" % 
                                                 (report_type))

                        status, done = downloader.next_chunk()

                        sender.put(download_id, 
                                   _RT_PROGRESS, 
                                   status.resumable_progress)

                        if done is True:
                            break

                    self.__log.info("Download finishing: %s" % (str(dfs)))

                    dfs.finish_download()
                except DownloadAgentDownloadException as e:
                    self.__log.exception("Download exception.")

                    sender.put(download_id, 
                               _RT_ERROR, 
                               (e.__class__.__name__, str(e)))
        else:
            self.__log.info("Local copy is already up-to-date: %s" % 
                            (download_reg))

        sender.put(download_id, 
                   _RT_DONE, 
                   ())

    def loop(self):
        global_receiver = gevent.queue.Queue()
        while True:
            if self.__stop_ev.is_set() is True:
                self.__log.debug("Download-agent stop-flag has been set.")
                break
            
            # Check if we've received a message from a worker thread.
            
            try:
                (id_, report_type, datum) = global_receiver.get(False)
            except Queue.Empty:
                pass
            else:
                # We have. Translate it to a message back to the request
                # interface.

                dr = self.__ops[id_][0]

                self.__log.debug("Worker received report [%s] from thread: "
                                 "%s" % (report_type, dr))

                if report_type == _RT_PROGRESS:
                    (bytes,) = datum
                    ns = dr[3]
                    ns.bytes_written = bytes
                elif report_type == _RT_ERROR:
                    (err_type, err_msg) = datum
                    ns = dr[3]
                    ns.error = (err_type, err_msg)
                elif report_type == _RT_DONE:
                    finish_ev = dr[1]
                    finish_ev.set()
                else:
                    raise ValueError("Worker process does not "
                                     "understand report-type: %s" % 
                                     (report_type))

            # Check for a kill event to be broadcast.
            if self.__kill_ev.set() is True:
                for id_, op in self.__ops.items():
                    op[1].put((_RT_THREAD_KILL, ()))

            # Check for a stop event on specific downloads.
            for id_, op in self.__ops.items():
                download_stop_ev = op[0][2]
                if download_stop_ev.is_set() is True:
                    op[1].put((_RT_THREAD_STOP, ()))

            try:
                request_info = self.__request_q.get(
                    timeout=download_agent.REQUEST_QUEUE_TIMEOUT_S)
            except Queue.Empty:
                self.__log.debug("Didn't find any new download requests.")
                continue

            sender = gevent.queue.Queue()
            id_ = len(self.__ops)
            self.__ops[id_] = (request_info, sender)

#            if self.__worker_pool.free_count() == 0:
#                self.__log.warn("It looks like we'll have to wait for a "
#                                "download worker to free up.")

            self.__log.debug("Spawning download worker.")


#            self.__worker_pool.spawn(self.download_worker, 
#                                     request_info[0],
#                                     id_, 
#                                     global_receiver,
#                                     sender)

            self.__log.info("DEBUG!")

        # The download loop has exited (we were told to stop). Signal the 
        # workers to stop what they're doing.

# TODO(dustin): For some reason, this isn't making it into the log (the above 
#               it, though). Tried flushing, but didn't work.
        self.__log.info("Download agent is shutting down.")

        self.__kill_ev.set()

#        start_epoch = time.time()
#        all_exited = False
#        while (time.time() - start_epoch) < \
#                download_agent.GRACEFUL_WORKER_EXIT_WAIT_S:
#            if self.__worker_pool.size <= self.__worker_pool.free_count():
#                all_exited = True
#                break
#
#        if all_exited is False:
#            self.__.error("Not all download workers exited in time: %d != %d",
#                          self.__worker_pool.size,
#                          self.__worker_pool.free_count())

        # Kill and join the unassigned (and stubborn, still-assigned) workers.
# TODO(dustin): We're assuming this is a hard kill that will always kill all 
#               workers.
#        self.__worker_pool.kill()

        self.__log.info("Download agent is terminating. (%d) requested files "
                        "will be abandoned.", self.__request_q.qsize())

def _agent_boot(request_q, stop_ev):
    """Boots the agent once it's given its own process."""

    logging.info("Starting download agent.")

    agent = _DownloadAgent(request_q, stop_ev)
    agent.loop()

    logging.info("Download agent loop has ended.")


class _SyncedResource(object):
    """This is the singleton object stored within the external agent that is
    flagged if/when a file is faulted."""

    def __init__(self, external_download_agent, entry_id, mtime_dt, 
                 resource_key):
        self.__eda = external_download_agent
        self.__entry_id = entry_id
        self.__mtime_dt = mtime_dt
        self.__key = resource_key
        self.__handles = []

    def __str__(self):
        return ('<RES %s %s>' % (self.__entry_id, self.__mtime_dt))

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
            handle.set_faulted()

    @property
    def entry_id(self):
        return self.__entry_id

    @property
    def key(self):
        return self.__key

def _check_resource_state(method):
    def wrap(self, *args, **kwargs):
        if self.is_faulted is True:
            raise DownloadAgentResourceFaultedException()

        return method(self, *args, **kwargs)
    
    return wrap
    

class _SyncedResourceHandle(object):
    """This object:
    
    - represents access to a synchronized file
    - will raise a DownloadAgentResourceFaultedException if the file has been 
      changed.
    - is the internal resource that will be associated with a physical file-
      handle.
    """

    def __init__(self, resource, dfs):
        self.__resource = resource
        self.__dfs = dfs
        self.__is_faulted = False
        self.__f = open(self.__dfs.get_stored_filepath(), 'rb')      

        # Start off opened.
        self.__open = True

        # We've finished initializing. Register ourselves as a handle on the 
        # resource.        
        self.__resource.register_handle(self)

    def set_faulted(self):
        """Indicate that another sync operation will have to occur in order to
        continue reading."""

        self.__is_faulted = True

    def __del__(self):
        if self.__open is True:
            self.close()

    def __enter__(self):
        return self

    def __exit__(self, type_, value, traceback):
        self.close()

    def close(self):
        self.__resource.decr_ref_count()
        self.__f.close()
        self.__open = False

    @_check_resource_state
    def __getattr__(self, name):
        return getattr(self.__f, name)

    @property
    def is_faulted(self):
        return self.__is_faulted


class _DownloadAgentExternal(object):
    """A class that runs in the same process as the rest of the application, 
    and acts as an interface to the worker process. This is a singleton.
    """

    def __init__(self):
        self.__log = logging.getLogger(self.__class__.__name__)
        self.__p = None
        self.__m = multiprocessing.Manager()

#        self.__abc = self.__m.Event()

        self.__request_q = multiprocessing.Queue()
        self.__request_loop_ev = multiprocessing.Event()
        
        self.__request_registry_context = { }
        self.__request_registry_types = { }
        self.__request_registry_locker = threading.Lock()

        # [entry_id] = [resource, counter]
        self.__accessor_resources = {}
        self.__accessor_resources_locker = threading.Lock()

        if os.path.exists(download_agent.DOWNLOAD_PATH) is False:
            os.makedirs(download_agent.DOWNLOAD_PATH)

    def deregister_resource(self, resource):
        """Called at the end of a file-resource's lifetime (on close)."""

        with self.__accessor_resources_locker:
            self.__accessor_resources[resource.key][1] -= 1
            
            if self.__accessor_resources[resource.key][1] <= 0:
                self.__log.debug("Resource is now being GC'd: %s", 
                                 str(resource))
                del self.__accessor_resources[resource.key]

    def __get_resource(self, entry_id, expected_mtime_dt):
        """Get the file resource and increment the reference count. Note that
        the resources are keyed by entry-ID -and- mtime, so the moment that
        an entry is faulted, we can fault the resource for all of the current
        handles while dispensing new handles with an entirely-new resource.
        """

        key = ('%s-%s' % (entry_id, 
                          time.mktime(expected_mtime_dt.timetuple())))

        with self.__accessor_resources_locker:
            try:
                self.__accessor_resources[key][1] += 1
                return self.__accessor_resources[key][0]
            except KeyError:
                resource = _SyncedResource(self, 
                                           entry_id, 
                                           expected_mtime_dt, 
                                           key)

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
        self.__log.debug("Sending start signal to download agent.")

        if self.__p is not None:
            raise ValueError("The download-worker is already started.")

        args = (self.__request_q, 
                self.__request_loop_ev)

        self.__p = multiprocessing.Process(target=_agent_boot, args=args)
        self.__p.start()

    def stop(self):
        self.__log.debug("Sending stop signal to download agent.")

        if self.__p is None:
            raise ValueError("The download-agent is already stopped.")

        self.__request_loop_ev.set()
        
        start_epoch = time.time()
        is_exited = False
        while (time.time() - start_epoch) <= \
                download_agent.GRACEFUL_WORKER_EXIT_WAIT_S:
            if self.__p.is_alive() is False:
                is_exited = True
                break

        if is_exited is True:
            self.__log.debug("Download agent exited gracefully.")
        else:
            self.__log.error("Download agent did not exit in time (%d).",
                             download_agent.GRACEFUL_WORKER_EXIT_WAIT_S)

            self.__p.terminate()

        self.__p.join()
        
        self.__log.info("Download agent joined with return-code: %d", 
                        self.__p.exitcode)
        
        self.__p = None

# TODO(dustin): We still need to consider removing faulted files (at least if
#               not being actively engaged/watched).

# TODO(dustin): We need to consider periodically pruning unaccessed, localized
#               files.
    def __find_stored_files_for_entry(self, entry_id):
        pattern = ('%s:*' % (utility.make_safe_for_filename(entry_id)))
        full_pattern = os.path.join(download_agent.DOWNLOAD_PATH, pattern)

        return [os.path.basename(file_path) 
                for file_path 
                in glob.glob(full_pattern)]

    @contextlib.contextmanager
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

        if download_request.expected_mtime_dt.tzinfo is None:
            raise ValueError("expected_mtime_dt must be timezone-aware.")

        # We keep a registry/index of everything we're downloading so that all
        # subsequent attempts to download the same file will block on the same
        # request.
        #
        # We'll also rely on the separate process to tell us if the file is
        # already local and up-to-date (everything is simpler/cleaner that
        # way), but will also do a ceck, ourselves, once we've submitted an
        # official request for tracking/locking purposes.

        typed_entry = download_request.typed_entry
        expected_mtime_dt = download_request.expected_mtime_dt.astimezone(
                                dateutil.tz.tzlocal())
        bytes_ = download_request.bytes

        download_reg = DownloadRegistration(typed_entry=typed_entry,
                                            expected_mtime_tuple=\
                                                expected_mtime_dt.timetuple(),
                                            url=download_request.url,
                                            bytes=bytes_)

        dfs = _DownloadedFileState(download_reg)

        with self.__request_registry_locker:
            try:
                context = self.__request_registry_context[typed_entry]
            except KeyError:
                # This is the first download of this entry/mime-type.

                self.__log.info("Initiating a new download: %s", download_reg)

# TODO(dustin): We might need to send a semaphore in order to control access.
                finish_ev = self.__m.Event()
                download_stop_ev = self.__m.Event()
                ns = self.__m.Namespace()
                ns.bytes_written = 0
                ns.error = None

                self.__request_registry_context[typed_entry] = {
                            'watchers': 1,
                            'finish_event': finish_ev,
                            'download_stop_event': download_stop_ev,
                            'ns': ns
                        }

                # Push the request to the worker process.
                self.__request_q.put((download_reg, 
                                      finish_ev, 
                                      download_stop_ev, 
                                      ns))
            else:
                # There was already another download of this entry/mime-type.

                self.__log.info("Watching an existing download: %s",
                                download_reg)

                context['watchers'] += 1
                finish_ev = context['finish_event']
                download_stop_ev = context['download_stop_event']
                ns = context['ns']

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

            if dfs.is_up_to_date(bytes_) is False:
                self.__log.debug("Waiting for file to be up-to-date (enough): "
                                 "%s", str(dfs))

                while 1:
                    is_done = finish_ev.wait(
                                download_agent.REQUEST_WAIT_PERIOD_S)

                    if ns.error is not None:
                        # If the download was stopped while there was at least 
                        # one watcher (which generally means that the file has 
                        # changed and the  message has propagated from the 
                        # notifying thread, to the download worker, to us), 
                        # emit an exception so those callers can re-call us.
                        #
                        # If this error occurred when there were no watchers,
                        # the stop most likely occurred because there -were-
                        # no watchers.
                        if ns.error[0] == 'DownloadAgentDownloadStopException':
                            raise DownloadAgentDownloadStopException(
                                ns.error[1])

                        raise DownloadAgentDownloadAgentError(
                            "Download failed for [%s]: [%s] %s" % 
                            (typed_entry.entry_id, ns.error[0], ns.error[1]))

                    elif is_done is True:
                        break

                    elif bytes_ is not None and \
                         ns.bytes_received >= bytes_:
                        break

            # We've now downloaded enough bytes, or already had enough bytes 
            # available.
            #
            # There are two reference-counts at this point: 1) the number of 
            # threads that are watching/accessing a particular mime-type of the
            # given entry (allows the downloads to be synchronized), and 2) the 
            # number of threads that have handles to an entry, regardless of 
            # mime-type (can be faulted when the content changes).

            self.__log.info("Data is now available: %s", str(dfs))

            resource = self.__get_resource(typed_entry.entry_id, 
                                           expected_mtime_dt)

            with _SyncedResourceHandle(resource, dfs) as srh:
                yield srh
        finally:
            # Decrement the reference count.
            with self.__request_registry_locker:
                context = self.__request_registry_context[typed_entry]
                context['watchers'] -= 1

                # If we're the last watcher, remove the entry. Note that the 
                # file may still be downloading at the worker. This just 
                # manages request-concurrency.
                if context['watchers'] <= 0:
                    self.__log.debug("Watchers have dropped to zero: %s", 
                                     typed_entry)

                    # In the event that all of the requests weren't concerned 
                    # with getting the whole file, send a signal to stop 
                    # downloading.
                    download_stop_ev.set()

                    # Remove registration information. If the worker is still
                    # downloading the file, it'll have a reference/copy of the
                    # event above.
                    del self.__request_registry_context[typed_entry]

                    self.__request_registry_types[typed_entry.entry_id].remove(
                        typed_entry)

                    if not self.__request_registry_types[typed_entry.entry_id]:
                        del self.__request_registry_types[typed_entry.entry_id]

        self.__log.debug("Sync is complete, and reference-counts have been "
                         "decremented.")

    def notify_changed(self, entry_id, mtime_dt):
        """Invoked by another thread when a file has changed, most likely by 
        information reported by the "changes" API. At this time, we don't check 
        whether anything has actually ever accessed this entry... Just whether 
        something currently has it open. It's essentially the same, and cheap.
        """

        if mtime_dt.tzinfo is None:
            raise ValueError("mtime_dt must be timezone-aware.")

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

def get_download_agent_external():
    try:
        return get_download_agent_external.__instance
    except AttributeError:
        get_download_agent_external.__instance = _DownloadAgentExternal()
        return get_download_agent_external.__instance

