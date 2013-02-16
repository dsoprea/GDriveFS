import logging
import resource

from errno import *
from threading import Lock, RLock
from collections import deque
from fuse import FuseOSError
from tempfile import NamedTemporaryFile
from os import unlink, utime

from gdrivefs.conf import Conf
from gdrivefs.errors import ExportFormatError, GdNotFoundError
from gdrivefs.utility import dec_hint
from gdrivefs.gdfs.displaced_file import DisplacedFile
from gdrivefs.gdfs.fsutility import get_temp_filepath, split_path
from gdrivefs.cache.volume import PathRelations, EntryCache, path_resolver, \
                                  CLAUSE_ID
from gdrivefs.gdtool.drive import drive_proxy
from gdrivefs.general.buffer_segments import BufferSegments

_static_log = logging.getLogger().getChild('(OF)')


class OpenedManager(object):

    instance = None
    singleton_lock = Lock()
    __log = None
    opened = { }
    opened_lock = RLock()
    fh_counter = 1

    @staticmethod
    def get_instance():
        with OpenedManager.singleton_lock:
            if OpenedManager.instance == None:
                try:
                    OpenedManager.instance = OpenedManager()
                except:
                    _static_log.exception("Could not create singleton "
                                          "instance of OpenedManager.")
                    raise

            return OpenedManager.instance

    def __init__(self):
        self.__log = logging.getLogger().getChild('OpenMan')

    def __get_max_handles(self):

        return resource.getrlimit(resource.RLIMIT_NOFILE)[0]

    def get_new_handle(self):
        """Get a handle for a file that's about to be opened. Note that the 
        handles start at (1), so there are a lot of "+ 1" occurrences below.
        """

        max_handles = self.__get_max_handles()

        with self.opened_lock:
            if len(self.opened) >= (max_handles + 1):
                raise FuseOSError(EMFILE)

            safety_counter = max_handles
            while safety_counter >= 1:
                self.fh_counter += 1

                if self.fh_counter >= (max_handles + 1):
                    self.fh_counter = 1

                if self.fh_counter not in self.opened:
                    logging.debug("Assigning file-handle (%d)." % 
                                  (self.fh_counter))
                    return self.fh_counter
                
        message = "Could not allocate new file handle. Safety breach."

        self.__log.error(message)
        raise Exception(message)

    def add(self, opened_file, fh=None):
        """Registered an OpenedFile object."""

        if opened_file.__class__.__name__ != 'OpenedFile':
            message = "Can only register an OpenedFile as an opened-file."

            self.__log.error(message)
            raise Exception(message)

        with self.opened_lock:
            if not fh:
                try:
                    fh = self.get_new_handle()
                except:
                    self.__log.exception("Could not acquire handle for "
                                      "OpenedFile to be registered.")
                    raise

            elif fh in self.opened:
                message = ("Opened-file with file-handle (%d) has already been"
                           " registered." % (opened_file.fh))

                self.__log.error(message)
                raise Exception(message)

            self.opened[fh] = opened_file

            return fh

    def remove_by_fh(self, fh):
        """Remove an opened-file, by the handle."""

        with self.opened_lock:
            self.__log.debug("Closing opened-file with handle (%d)." % (fh))

            if fh not in self.opened:
                message = ("Opened-file with file-handle (%d) is not "
                          "registered (remove_by_fh)." % (fh))

                self.__log.error(message)
                raise Exception(message)

            del self.opened[fh]

    def get_by_fh(self, fh):
        """Retrieve an opened-file, by the handle."""

        with self.opened_lock:
            if fh not in self.opened:
                message = ("Opened-file with file-handle (%d) is not "
                          "registered (get_by_fh)." % (fh))

                self.__log.error(message)
                raise Exception(message)

            return self.opened[fh]

            
class OpenedFile(object):
    """This class describes a single open file, and manages changes."""

    updates         = deque()
    update_lock     = Lock()
    download_lock   = Lock()

    @staticmethod
    def create_for_requested_filepath(filepath):
        """Process the file/path that was requested (potential export-type 
        directive, dot-prefix, etc..), and build an opened-file object using 
        the information.
        """

        _static_log.debug("Creating OpenedFile for [%s]." % (filepath))

        # Process/distill the requested file-path.

        try:
            (parent_clause, path, filename, extension, mime_type, is_hidden, \
             just_info) = split_path(filepath, path_resolver)
        except GdNotFoundError:
            _static_log.exception("Could not process [%s] (create).")
            raise FuseOSError(ENOENT)
        except:
            _static_log.exception("Could not split path [%s] (create)." % 
                                  (filepath))
            raise

        distilled_filepath = ("%s%s" % (path, filename))

        # Look-up the requested entry.

        path_relations = PathRelations.get_instance()

        try:
            entry_clause = path_relations.get_clause_from_path(distilled_filepath)
        except:
            _static_log.exception("Could not try to get clause from path [%s] "
                                  "(OpenedFile)." % (distilled_filepath))
            raise FuseOSError(EIO)

        if not entry_clause:
            _static_log.debug("Path [%s] does not exist for stat()." % (path))
            raise FuseOSError(ENOENT)

        # Build the object.

        try:
            return OpenedFile(entry_clause[CLAUSE_ID], path, filename, 
                              is_hidden, mime_type, just_info)
        except:
            _static_log.exception("Could not create OpenedFile for requested "
                                  "file [%s]." % (distilled_filepath))
            raise

    def __init__(self, entry_id, path, filename, is_hidden, mime_type, 
                 just_info=False):

        self.__log = logging.getLogger().getChild('OpenFile')

        self.__log.info("Opened-file object created for entry-ID [%s] and path "
                     "(%s)." % (entry_id, path))
# TODO: Refactor this to being all obfuscated property names.
        self.entry_id = entry_id
        self.path = path
        self.filename = filename
        self.is_hidden = is_hidden
        self.__mime_type = mime_type
        self.cache = EntryCache.get_instance().cache
        self.__just_info = just_info
        self.buffer = None
        self.__is_loaded = False

# TODO: !! Make sure the "changes" thread is still going, here.

    def __get_entry_or_raise(self):
        """We can never be sure that the entry will still be known to the 
        system. Grab it and throw an error if it's not available. 
        Simultaneously, this allows us to lazy-load the entry.
        """

        self.__log.debug("Retrieving entry for opened-file with entry-ID "
                         "[%s]." % (self.entry_id))

        try:
            return self.cache.get(self.entry_id)
        except:
            self.__log.exception("Could not retrieve entry with ID [%s] for "
                                 "the opened-file." % (self.entry_id))
            raise 

    @property
    def mime_type(self):
    
        if self.__mime_type:
            return self.__mime_type
        
        try:
            entry = self.__get_entry_or_raise()
        except:
            self.__log.exception("Could not get entry with ID [%s] for "
                                 "mime_type." % (self.entry_id))
            raise

        return entry.normalized_mime_type

    @dec_hint(prefix='OF')
    def __write_stub_file(self, file_path, normalized_entry, mime_type):

        try:
            displaced = DisplacedFile(normalized_entry)
        except:
            self.__log.exception("Could not wrap entry in DisplacedFile.")
            raise

        try:
            stub_data = displaced.get_stub(mime_type, file_path=file_path)
        except:
            self.__log.exception("Could not do displaced-file download.")
            raise

        with file(file_path, 'w') as f:
            f.write(stub_data)

        return len(stub_data)

    @dec_hint(prefix='OF')
    def __load_base_from_remote(self):
        """Download the data for the entry that we represent. This is probably 
        a file, but could also be a stub for -any- entry.
        """

        self.__log.debug("Retrieving entry for load_base_from_remote.")

        try:
            entry = self.__get_entry_or_raise()
        except:
            self.__log.exception("Could not get entry with ID [%s] for "
                              "write-flush." % (self.entry_id))
            raise

        mime_type = self.mime_type
        temp_file_path = get_temp_filepath(entry, self.__just_info, mime_type)

        with self.download_lock:
            # Get the current version of the write-cache file, or note that we 
            # don't have it.

            self.__log.info("Attempting local cache update of file [%s] for "
                            "entry [%s] and mime-type [%s]." % 
                            (temp_file_path, entry, mime_type))

            # The output path is predictable. It shouldn't change.

            if self.__just_info:
                try:
                    length = self.__write_stub_file(temp_file_path, 
                                                    entry, 
                                                    mime_type)
                    cache_fault = True
                except:
                    self.__log.exception("Could not build info for entry "
                                         "[%s] being read." % (entry))
                    raise
            else:
                try:
                    result = drive_proxy('download_to_local', 
                                         output_file_path=temp_file_path,
                                         normalized_entry=entry,
                                         mime_type=mime_type)
                    (length, cache_fault) = result
                except ExportFormatError:
                    raise FuseOSError(ENOENT)
                except:
                    self.__log.exception("Could not localize file with entry "
                                         "[%s]." % (entry))
                    raise

            # We've either not loaded it, yet, or it has changed.
            if cache_fault or not self.__is_loaded:
                if cache_fault:
                    with self.update_lock:
                        if self.updates:
                            self.__log.error("Entry [%s] has been changed. "
                                             "Forcing buffer updates, and "
                                             "clearing (%d) queued updates." % 
                                             (entry, len(self.updates)))

                            self.updates = []
                        else:
                            self.__log.debug("Entry [%s] has changed. "
                                             "Updating buffers." % (entry))

                self.__log.debug("Updating local cache file.")

                with open(temp_file_path, 'rb') as f:
                    # Read the locally cached file in.

                    try:
# TODO: Read in steps?
                        data = f.read()

                        read_blocksize = Conf.get('default_buffer_read_blocksize')
                        self.buffer = BufferSegments(data, read_blocksize)
                    except:
                        self.__log.exception("Could not read current cached "
                                             "file into buffer.")
                        raise

                self.__is_loaded = True

        return cache_fault

    @dec_hint(['offset', 'data'], ['data'], 'OF')
    def add_update(self, offset, data):
        """Queue an update to this file."""

        self.__marker('add_update', { 'offset': offset, 
                                      'actual_length': len(data) })

        try:
            self.__load_base_from_remote()
        except:
            self.__log.exception("Could not load write-cache file [%s]." % 
                              (self.temp_file_path))
            raise

# TODO: Immediately apply updates to buffer. Add a "dirty" flag.
        with self.update_lock:
            self.buffer.apply_update(offset, data)
        #    self.updates.append((offset, data))

        self.__log.debug("(%d) updates have been queued." % 
                         (len(self.updates)))

    @dec_hint(prefix='OF')
    def flush(self):
        """The OS wants to effect any changes made to the file."""

        #print("Flushing (%d) updates." % (len(self.updates)))

        self.__log.debug("Retrieving entry for write-flush.")

        try:
            entry = self.__get_entry_or_raise()
        except:
            self.__log.exception("Could not get entry with ID [%s] for "
                              "write-flush." % (self.entry_id))
            raise

        try:
             cache_fault = self.__load_base_from_remote()
        except:
            self.__log.exception("Could not load write-cache file [%s]." % 
                              (self.temp_file_path))
            raise
    
        with self.update_lock:
            if not self.updates:
                self.__log.debug("Flush will be skipped due to empty write-"
                              "queue.")
                return

            if cache_fault:
                logging.warn("File updates can no longer be applied. The file "
                             "has been changed, remotely. Dumping queued "
                             "updates.")
                self.updates = []
# TODO: Raise an exception?
                return

            # Apply updates to the data.

            self.__log.debug("Applying (%d) updates." % (len(self.updates)))

            # Write back out to the temporary file.

            self.__log.debug("Writing buffer to temporary file.")
# TODO: Make sure to uncache the temp data if self.temp_file_path is not None.

            mime_type = self.mime_type

            # If we've already opened a work file, use it. Else, use a 
            # temporary file that we'll close at the end of the method.
            if self.__is_loaded:
                is_temp = False

                temp_file_path = get_temp_filepath(entry, 
                                                   self.__just_info, 
                                                   mime_type)
                                                   
                with file(temp_file_path, 'w') as f:
                    for block in self.buffer:
                        f.write(block)
                                                   
                write_file_path = temp_file_path
            else:
                is_temp = True
            
                with NamedTemporaryFile(delete=False) as f:
                    write_file_path = f.name
                    for block in self.buffer:
                        f.write(block)

            # Push to GD.

            self.__log.debug("Pushing (%d) bytes for entry with ID from [%s] "
                             "to GD for file-path [%s]." % (len(buffer), 
                                                            entry.id, 
                                                            write_file_path))

#            print("Sending updates.")

            try:
                entry = drive_proxy('update_entry', 
                                    normalized_entry=entry, 
                                    filename=entry.title, 
                                    data_filepath=write_file_path, 
                                    mime_type=mime_type, 
                                    parents=entry.parents, 
                                    is_hidden=self.is_hidden)
            except:
                self.__log.exception("Could not localize displaced file with "
                                     "entry having ID [%s]." % (entry.id))
                raise

            if not is_temp:
                unlink(write_file_path)
            else:
                # Update the write-cache file to the official mtime. We won't 
                # redownload it on the next flush if it wasn't changed, 
                # elsewhere.

                self.__log.debug("Updating local write-cache file to official "
                                 "mtime [%s]." % (entry.modified_date_epoch))

                try:
                    utime(write_file_path, (entry.modified_date_epoch, 
                                            entry.modified_date_epoch))
                except:
                    self.__log.exception("Could not update mtime of write-"
                                         "cache [%s] for entry with ID [%s], "
                                         "post-flush." % 
                                         (entry.modified_date_epoch, entry.id))
                    raise

        # Immediately update our current cached entry.

        self.__log.debug("Update successful. Updating local cache.")

        path_relations = PathRelations.get_instance()

        try:
            path_relations.register_entry(entry)
        except:
            self.__log.exception("Could not register updated file in cache.")
            raise

        self.__log.info("Update complete on entry with ID [%s]." % (entry.id))

    @dec_hint(['offset', 'length'], prefix='OF')
    def read(self, offset, length):
        
        self.__log.debug("Checking write-cache file (flush).")

        try:
            self.__load_base_from_remote()
        except:
            self.__log.exception("Could not load write-cache file.")
            raise

# TODO: Refactor this into a paging mechanism.
        buffer_len = self.buffer.length
        if offset >= buffer_len:
            raise IndexError("Offset (%d) exceeds length of data (%d)." % 
                             (offset, buffer_len))

        if (offset + length) > buffer_len:
            self.__log.debug("Requested length (%d) from offset (%d) exceeds "
                             "file length (%d). Truncated." % (length, offset, 
                                                               buffer_len)) 
            length = buffer_len

        data_blocks = [block for block in self.buffer.read(offset, length)]
        data = ''.join(data_blocks)

        self.__log.debug("(%d) bytes retrieved from slice (%d):(%d)/(%d)." % 
                         (len(data), offset, length, self.buffer.length))

        return data

