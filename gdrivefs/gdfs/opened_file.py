import logging
import resource
import re

from errno import *
from threading import Lock, RLock
from collections import deque
from fuse import FuseOSError
from tempfile import NamedTemporaryFile
from os import unlink, utime, makedirs
from os.path import isdir

from gdrivefs.conf import Conf
from gdrivefs.errors import ExportFormatError, GdNotFoundError
from gdrivefs.utility import dec_hint
from gdrivefs.gdfs.fsutility import split_path
from gdrivefs.gdfs.displaced_file import DisplacedFile
from gdrivefs.cache.volume import PathRelations, EntryCache, path_resolver, \
                                  CLAUSE_ID, CLAUSE_ENTRY
from gdrivefs.gdtool.drive import drive_proxy
from gdrivefs.general.buffer_segments import BufferSegments

_static_log = logging.getLogger().getChild('(OF)')

temp_path = ("%s/local" % (Conf.get('file_download_temp_path')))
if isdir(temp_path) is False:
    makedirs(temp_path)

def get_temp_filepath(normalized_entry, mime_type):
    temp_filename = ("%s.%s" % 
                     (normalized_entry.id, mime_type.replace('/', '+'))).\
                    encode('ascii')

    temp_path = Conf.get('file_download_temp_path')
    return ("%s/local/%s" % (temp_path, temp_filename))


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
            result = split_path(filepath, path_resolver)
            (parent_clause, path, filename, mime_type, is_hidden) = result
        except GdNotFoundError:
            _static_log.exception("Could not process [%s] "
                                  "(create_for_requested)." % (filepath))
            raise FuseOSError(ENOENT)
        except:
            _static_log.exception("Could not split path [%s] "
                                  "(create_for_requested)." % (filepath))
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

        entry = entry_clause[CLAUSE_ENTRY]

        if mime_type is None and entry.requires_mimetype:
            _static_log.error("Mime-type needs to be specified. Options: %s" % 
                              (entry.download_types))
            raise FuseOSError(EIO)

        # Normalize the mime-type by considering what's available for download.

        try:
            final_mimetype = entry.normalize_download_mimetype(mime_type)
        except:
            _static_log.exception("Could not normalize mime-type [%s] for "
                                  "entry [%s]." % (mime_type, entry))
            raise FuseOSError(EIO)

        if final_mimetype != mime_type:
            _static_log.info("Entry being opened will be opened as [%s] "
                             "rather than [%s]." % (final_mimetype, mime_type))

        # Build the object.

        try:
            return OpenedFile(entry_clause[CLAUSE_ID], path, filename, 
                              is_hidden, final_mimetype)
        except:
            _static_log.exception("Could not create OpenedFile for requested "
                                  "file [%s]." % (distilled_filepath))
            raise

    def __init__(self, entry_id, path, filename, is_hidden, mime_type):

        self.__log = logging.getLogger().getChild('OpenFile')

        self.__log.info("Opened-file object created for entry-ID [%s] and "
                        "path (%s)." % (entry_id, path))
# TODO: Refactor this to being all obfuscated property names.
        self.__entry_id = entry_id
        self.__path = path
        self.__filename = filename
        self.__is_hidden = is_hidden
        
        self.__mime_type = mime_type
        self.__cache = EntryCache.get_instance().cache
        self.__buffer = None
        self.__is_loaded = False
        self.__is_dirty = False

    def __repr__(self):
        replacements = {'entry_id': self.__entry_id, 
                        'filename': self.__filename, 
                        'mime_type': self.__mime_type, 
                        'is_loaded': self.__is_loaded, 
                        'is_dirty': self.__is_dirty }

        return ("<OF [%(entry_id)s] F=[%(filename)s] MIME=[%(mime_type)s] "
                "LOADED=[%(is_loaded)s] DIRTY= [%(is_dirty)s]>" % replacements)

# TODO: !! Make sure the "changes" thread is still going, here.

    def __get_entry_or_raise(self):
        """We can never be sure that the entry will still be known to the 
        system. Grab it and throw an error if it's not available. 
        Simultaneously, this allows us to lazy-load the entry.
        """

        self.__log.debug("Retrieving entry for opened-file with entry-ID "
                         "[%s]." % (self.__entry_id))

        try:
            return self.__cache.get(self.__entry_id)
        except:
            self.__log.exception("Could not retrieve entry with ID [%s] for "
                                 "the opened-file." % (self.__entry_id))
            raise 

    def __load_base_from_remote(self):
        """Download the data for the entry that we represent. This is probably 
        a file, but could also be a stub for -any- entry.
        """

        try:
            entry = self.__get_entry_or_raise()
        except:
            self.__log.exception("Could not get entry with ID [%s] for "
                                 "write-flush." % (self.__entry_id))
            raise

        self.__log.debug("Ensuring local availability of [%s]." % (entry))

        temp_file_path = get_temp_filepath(entry, self.mime_type)

        self.__log.debug("__load_base_from_remote about to download.")

        with self.download_lock:
            # Get the current version of the write-cache file, or note that we 
            # don't have it.

            self.__log.info("Attempting local cache update of file [%s] for "
                            "entry [%s] and mime-type [%s]." % 
                            (temp_file_path, entry, self.mime_type))

            if entry.requires_mimetype:
                length = DisplacedFile.file_size

                try:
                    stub_data = DisplacedFile(entry).deposit_file(self.mime_type)

                    with file(temp_file_path, 'w') as f:
                        f.write(stub_data)
                except:
                    self.__log.exception("Could not deposit to file [%s] from "
                                         "entry [%s]." % (temp_file_path, 
                                                          entry))
                    raise

# TODO: Accomodate the cache for displaced-files.
                cache_fault = True

            else:
                self.__log.debug("Executing the download.")
                
                try:
                    result = drive_proxy('download_to_local', 
                                         output_file_path=temp_file_path,
                                         normalized_entry=entry,
                                         mime_type=self.mime_type)
                    self.__log.debug("download_to_local succeeded.")

                    (length, cache_fault) = result
                except ExportFormatError:
                    self.__log.exception("There was an export-format error.")
                    raise FuseOSError(ENOENT)
                except:
                    self.__log.exception("Could not localize file with entry "
                                         "[%s]." % (entry))
                    raise

            self.__log.debug("Download complete.  cache_fault= [%s] "
                             "__is_loaded= [%s]" % (cache_fault, self.__is_loaded))

            # We've either not loaded it, yet, or it has changed.
            if cache_fault or not self.__is_loaded:
                with self.update_lock:
                    self.__log.debug("Checking queued items for fault.")

                    if cache_fault:
                        if self.__is_dirty:
                            self.__log.error("Entry [%s] has been changed. "
                                             "Forcing buffer updates, and "
                                             "clearing uncommitted updates." % 
                                             (entry))
                        else:
                            self.__log.debug("Entry [%s] has changed. "
                                             "Updating buffers." % (entry))

                    self.__log.debug("Loading buffers.")

                    with open(temp_file_path, 'rb') as f:
                        # Read the locally cached file in.

                        try:
    # TODO: Read in steps?
                            data = f.read()

                            read_blocksize = Conf.get('default_buffer_read_blocksize')
                            self.__buffer = BufferSegments(data, read_blocksize)
                        except:
                            self.__log.exception("Could not read current cached "
                                                 "file into buffer.")
                            raise

                        self.__is_dirty = False

                    self.__is_loaded = True

        self.__log.debug("__load_base_from_remote complete.")
        return cache_fault

    @dec_hint(['offset', 'data'], ['data'], 'OF')
    def add_update(self, offset, data):
        """Queue an update to this file."""

        self.__log.debug("Applying update for offset (%d) and length (%d)." % 
                         (offset, len(data)))

        try:
            self.__load_base_from_remote()
        except:
            self.__log.exception("Could not load entry to local cache [%s]." % 
                                 (self.temp_file_path))
            raise

        self.__log.debug("Base loaded for add_update.")

        with self.update_lock:
            self.__buffer.apply_update(offset, data)
            self.__is_dirty = True

    @dec_hint(prefix='OF')
    def flush(self):
        """The OS wants to effect any changes made to the file."""

        self.__log.debug("Retrieving entry for write-flush.")

        try:
            entry = self.__get_entry_or_raise()
        except:
            self.__log.exception("Could not get entry with ID [%s] for "
                                 "write-flush." % (self.__entry_id))
            raise

        try:
             cache_fault = self.__load_base_from_remote()
        except:
            self.__log.exception("Could not load local cache for entry [%s]." % 
                                 (entry))
            raise
    
        with self.update_lock:
            if self.__is_dirty is False:
                self.__log.debug("Flush will be skipped because there are no "
                                 "changes.")
# TODO: Raise an exception?
                return

            # Write back out to the temporary file.

            self.__log.debug("Writing buffer to temporary file.")
# TODO: Make sure to uncache the temp data if self.temp_file_path is not None.

            mime_type = self.mime_type

            # If we've already opened a work file, use it. Else, use a 
            # temporary file that we'll close at the end of the method.
            if self.__is_loaded:
                is_temp = False

                temp_file_path = get_temp_filepath(entry, mime_type)
                                                   
                with file(temp_file_path, 'w') as f:
                    for block in self.__buffer.read():
                        f.write(block)
                                                   
                write_file_path = temp_file_path
            else:
                is_temp = True
            
                with NamedTemporaryFile(delete=False) as f:
                    write_file_path = f.name
                    for block in self.__buffer.read():
                        f.write(block)

            # Push to GD.

            self.__log.debug("Pushing (%d) bytes for entry with ID from [%s] "
                             "to GD for file-path [%s]." % 
                             (self.__buffer.length, entry.id, write_file_path))

#            print("Sending updates.")

            try:
                entry = drive_proxy('update_entry', 
                                    normalized_entry=entry, 
                                    filename=entry.title, 
                                    data_filepath=write_file_path, 
                                    mime_type=mime_type, 
                                    parents=entry.parents, 
                                    is_hidden=self.__is_hidden)
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

        self.__is_dirty = False

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

        buffer_len = self.__buffer.length

        # Some files may have a length of (0) untill a particular type is 
        # chosen (the download-links).
        if buffer_len > 0:
            if offset >= buffer_len:
                raise IndexError("Offset (%d) exceeds length of data (%d)." % 
                                 (offset, buffer_len))

            if (offset + length) > buffer_len:
                self.__log.debug("Requested length (%d) from offset (%d) exceeds "
                                 "file length (%d). Truncated." % (length, offset, 
                                                                   buffer_len)) 
                length = buffer_len

        data_blocks = [block for block in self.__buffer.read(offset, length)]
        data = ''.join(data_blocks)

        self.__log.debug("(%d) bytes retrieved from slice (%d):(%d)/(%d)." % 
                         (len(data), offset, length, self.__buffer.length))

        return data

    @property
    def mime_type(self):
        return self.__mime_type

