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
from gdrivefs.gdfs.fsutility import dec_hint, split_path, build_filepath
from gdrivefs.gdfs.displaced_file import DisplacedFile
from gdrivefs.cache.volume import PathRelations, EntryCache, path_resolver, \
                                  CLAUSE_ID, CLAUSE_ENTRY
from gdrivefs.gdtool.drive import drive_proxy
from gdrivefs.general.buffer_segments import BufferSegments

_logger = logging.getLogger(__name__)

temp_path = ("%s/local" % (Conf.get('file_download_temp_path')))
if isdir(temp_path) is False:
    makedirs(temp_path)

def get_temp_filepath(normalized_entry, mime_type):
    temp_filename = ("%s.%s" % 
                     (normalized_entry.id, mime_type.replace('/', '+'))).\
                    encode('ascii')

    temp_path = Conf.get('file_download_temp_path')
    return ("%s/local/%s" % (temp_path, temp_filename))



# TODO(dustin): LCM runs in a greenlet pool. When we open a file that needs the
#               existing data for a file (read, append), a switch is done to an
#               LCM worker. If the data is absent or faulted, download the
#               content. Then, switch back.

class LocalCopyManager(object):
    """Manages local copies of files."""
    
#    def 
    pass


class OpenedManager(object):
    """Manages all of the currently-open files."""

    __instance = None
    __singleton_lock = Lock()
    __opened_lock = RLock()
    __fh_counter = 1

    @staticmethod
    def get_instance():
        with OpenedManager.__singleton_lock:
            if OpenedManager.__instance is None:
                OpenedManager.__instance = OpenedManager()

            return OpenedManager.__instance

    def __init__(self):
        self.__opened = {}
        self.__opened_byfile = {}

    def __get_max_handles(self):

        return resource.getrlimit(resource.RLIMIT_NOFILE)[0]

    def get_new_handle(self):
        """Get a handle for a file that's about to be opened. Note that the 
        handles start at (1), so there are a lot of "+ 1" occurrences below.
        """

        max_handles = self.__get_max_handles()

        with OpenedManager.__opened_lock:
            if len(self.__opened) >= (max_handles + 1):
                raise FuseOSError(EMFILE)

            safety_counter = max_handles
            while safety_counter >= 1:
                OpenedManager.__fh_counter += 1

                if OpenedManager.__fh_counter >= (max_handles + 1):
                    OpenedManager.__fh_counter = 1

                if OpenedManager.__fh_counter not in self.__opened:
                    _logger.debug("Assigning file-handle (%d).",
                                  OpenedManager.__fh_counter)

                    return OpenedManager.__fh_counter
                
        message = "Could not allocate new file handle. Safety breach."
        _logger.error(message)
        raise Exception(message)

    def add(self, opened_file, fh=None):
        """Registered an OpenedFile object."""

        if opened_file.__class__.__name__ != 'OpenedFile':
            message = "Can only register an OpenedFile as an opened-file."

            _logger.error(message)
            raise Exception(message)

        with OpenedManager.__opened_lock:
            if not fh:
                fh = self.get_new_handle()

            elif fh in self.__opened:
                message = ("Opened-file with file-handle (%d) has already been"
                           " registered." % (opened_file.fh))

                _logger.error(message)
                raise Exception(message)

            self.__opened[fh] = opened_file

            file_path = opened_file.file_path
            if file_path in self.__opened_byfile:
                self.__opened_byfile[file_path].append(fh)
            else:
                self.__opened_byfile[file_path] = [fh]

            return fh

    def remove_by_fh(self, fh):
        """Remove an opened-file, by the handle."""

        with OpenedManager.__opened_lock:
            _logger.debug("Closing opened-file with handle (%d).", fh)

            self.__opened[fh].cleanup()

            file_path = self.__opened[fh].file_path
            del self.__opened[fh]
            
            try:
                self.__opened_byfile[file_path].remove(fh)
            except ValueError:
                raise ValueError("Could not remove handle (%d) from list of "
                                 "open-handles for file-path [%s]: %s" % 
                                 (fh, file_path, 
                                  self.__opened_byfile[file_path]))

            if not self.__opened_byfile[file_path]:
                del self.__opened_byfile[file_path]

    def remove_by_filepath(self, file_path):

        _logger.debug("Removing all open handles for file-path [%s].",
                      file_path)

        count = 0

        with OpenedManager.__opened_lock:
            try:
                for fh in self.__opened_byfile[file_path]:
                    self.remove_by_fh(fh)
                    count += 1
            except KeyError:
                pass

        _logger.debug("(%d) file-handles removed for file-path [%s].",
                      count, file_path)

    def get_by_fh(self, fh):
        """Retrieve an opened-file, by the handle."""

        with OpenedManager.__opened_lock:
            if fh not in self.__opened:
                message = ("Opened-file with file-handle (%d) is not "
                          "registered (get_by_fh)." % (fh))

                _logger.error(message)
                raise Exception(message)

            return self.__opened[fh]

            
class OpenedFile(object):
    """This class describes a single open file, and manages changes."""

    __update_lock = Lock()
    __download_lock = Lock()

    @staticmethod
    def create_for_requested_filepath(filepath):
        """Process the file/path that was requested (potential export-type 
        directive, dot-prefix, etc..), and build an opened-file object using 
        the information.
        """

        _logger.debug("Creating OpenedFile for [%s].", filepath)

        # Process/distill the requested file-path.

        try:
            result = split_path(filepath, path_resolver)
            (parent_clause, path, filename, mime_type, is_hidden) = result
        except GdNotFoundError:
            _logger.exception("Could not process [%s] (create_for_requested).",
                              filepath)

            raise FuseOSError(ENOENT)

        distilled_filepath = build_filepath(path, filename)

        # Look-up the requested entry.

        path_relations = PathRelations.get_instance()

        try:
            entry_clause = path_relations.get_clause_from_path(distilled_filepath)
        except:
            _logger.exception("Could not try to get clause from path [%s] "
                              "(OpenedFile).", distilled_filepath)

            raise FuseOSError(EIO)

        if not entry_clause:
            _logger.debug("Path [%s] does not exist for stat().", path)
            raise FuseOSError(ENOENT)

        entry = entry_clause[CLAUSE_ENTRY]

        # Normalize the mime-type by considering what's available for download. 
        # We're going to let the requests that didn't provide a mime-type fail 
        # right here. It will give us the opportunity to try a few options to 
        # get the file.

        try:
            final_mimetype = entry.normalize_download_mimetype(mime_type)
        except ExportFormatError:
            _logger.exception("There was an export-format error "
                              "(create_for_requested_filesystem).")

            raise FuseOSError(ENOENT)
        except:
            _logger.exception("Could not normalize mime-type [%s] for entry"
                              "[%s].", mime_type, entry)

            raise FuseOSError(EIO)

        if final_mimetype != mime_type:
            _logger.info("Entry being opened will be opened as [%s] rather "
                         "than [%s].", final_mimetype, mime_type)

        # Build the object.

        return OpenedFile(
                entry_clause[CLAUSE_ID], 
                path, 
                filename, 
                is_hidden, 
                final_mimetype)

    def __init__(self, entry_id, path, filename, is_hidden, mime_type):

        _logger.info("Opened-file object created for entry-ID [%s] and path "
                     "(%s).", entry_id, path)

        self.__entry_id = entry_id
        self.__path = path
        self.__filename = filename
        self.__is_hidden = is_hidden
        
        self.__mime_type = mime_type
        self.__cache = EntryCache.get_instance().cache

        self.reset_state()

    def reset_state(self):
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

    def cleanup(self):
        """Remove temporary files."""
    
        pass

    def __get_entry_or_raise(self):
        """We can never be sure that the entry will still be known to the 
        system. Grab it and throw an error if it's not available. 
        Simultaneously, this allows us to lazy-load the entry.
        """

        _logger.debug("Retrieving entry for opened-file with entry-ID "
                      "[%s].", self.__entry_id)

        return self.__cache.get(self.__entry_id)

    def __load_base_from_remote(self):
        """Download the data for the entry that we represent. This is probably 
        a file, but could also be a stub for -any- entry.
        """

        entry = self.__get_entry_or_raise()

        _logger.debug("Ensuring local availability of [%s].", entry)

        temp_file_path = get_temp_filepath(entry, self.mime_type)

        _logger.debug("__load_base_from_remote about to download.")

        with self.__class__.__download_lock:
            # Get the current version of the write-cache file, or note that we 
            # don't have it.

            _logger.info("Attempting local cache update of file [%s] for entry"
                         "[%s] and mime-type [%s].",
                         temp_file_path, entry, self.mime_type)

            if entry.requires_mimetype:
                length = DisplacedFile.file_size

                d = DisplacedFile(entry)
                stub_data = d.deposit_file(self.mime_type)

                with file(temp_file_path, 'w') as f:
                    f.write(stub_data)

# TODO: Accommodate the cache for displaced-files.
                cache_fault = True

            else:
                _logger.debug("Executing the download.")
                
                try:
# TODO(dustin): We're not inheriting an existing file (same mtime, same size).
                    result = drive_proxy('download_to_local', 
                                         output_file_path=temp_file_path,
                                         normalized_entry=entry,
                                         mime_type=self.mime_type)

                    (length, cache_fault) = result
                except ExportFormatError:
                    _logger.exception("There was an export-format error.")
                    raise FuseOSError(ENOENT)

            _logger.debug("Download complete.  cache_fault= [%s] "
                          "__is_loaded= [%s]", cache_fault, self.__is_loaded)

            # We've either not loaded it, yet, or it has changed.
            if cache_fault or not self.__is_loaded:
                with self.__class__.__update_lock:
                    _logger.debug("Checking queued items for fault.")

                    if cache_fault:
                        if self.__is_dirty:
                            _logger.error("Entry [%s] has been changed. "
                                          "Forcing buffer updates, and "
                                          "clearing uncommitted updates.",
                                          entry)
                        else:
                            _logger.debug("Entry [%s] has changed. "
                                          "Updating buffers.", entry)

                    _logger.debug("Loading buffers.")

                    with open(temp_file_path, 'rb') as f:
                        # Read the locally cached file in.

# TODO(dustin): This is the source of:
# 1) An enormous slowdown where we first have to write the data, and then have to read it back.
# 2) An enormous resource burden.
                        data = f.read()

                        read_blocksize = \
                            Conf.get('default_buffer_read_blocksize')
                        
                        self.__buffer = BufferSegments(
                                            data, 
                                            read_blocksize)

                        self.__is_dirty = False

                    self.__is_loaded = True

        _logger.debug("__load_base_from_remote complete.")
        return cache_fault

    @dec_hint(['offset', 'data'], ['data'], 'OF')
    def add_update(self, offset, data):
        """Queue an update to this file."""

        _logger.debug("Applying update for offset (%d) and length (%d).",
                      offset, len(data))

        self.__load_base_from_remote()

        _logger.debug("Base loaded for add_update.")

        with self.__class__.__update_lock:
            self.__buffer.apply_update(offset, data)
            self.__is_dirty = True

    @dec_hint(prefix='OF')
    def flush(self):
        """The OS wants to effect any changes made to the file."""

        _logger.debug("Retrieving entry for write-flush.")

        entry = self.__get_entry_or_raise()
        cache_fault = self.__load_base_from_remote()
    
        with self.__class__.__update_lock:
            if self.__is_dirty is False:
                _logger.debug("Flush will be skipped because there are no "
                              "changes.")
# TODO: Raise an exception?
                return

            # Write back out to the temporary file.

            _logger.debug("Writing buffer to temporary file.")
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
                                                   
                write_filepath = temp_file_path
            else:
                is_temp = True
            
                with NamedTemporaryFile(delete=False) as f:
                    write_filepath = f.name
                    for block in self.__buffer.read():
                        f.write(block)

            # Push to GD.

            _logger.debug("Pushing (%d) bytes for entry with ID from [%s] to"
                          "GD for file-path [%s].",
                          self.__buffer.length, entry.id, write_filepath)

# TODO: Update mtime?
            entry = drive_proxy('update_entry', 
                                normalized_entry=entry, 
                                filename=entry.title, 
                                data_filepath=write_filepath, 
                                mime_type=mime_type, 
                                parents=entry.parents, 
                                is_hidden=self.__is_hidden)

            if not is_temp:
                unlink(write_filepath)
            else:
                # Update the write-cache file to the official mtime. We won't 
                # redownload it on the next flush if it wasn't changed, 
                # elsewhere.

                _logger.debug("Updating local write-cache file to official "
                              "mtime [%s].", entry.modified_date_epoch)

                utime(
                    write_filepath, 
                    (entry.modified_date_epoch, 
                     entry.modified_date_epoch))

        # Immediately update our current cached entry.

        _logger.debug("Update successful. Updating local cache.")

        path_relations = PathRelations.get_instance()

        path_relations.register_entry(entry)

        self.__is_dirty = False

        _logger.info("Update complete on entry with ID [%s].", entry.id)

    @dec_hint(['offset', 'length'], prefix='OF')
    def read(self, offset, length):
        
        _logger.debug("Checking write-cache file (flush).")

        self.__load_base_from_remote()

# TODO: Refactor this into a paging mechanism.

        buffer_len = self.__buffer.length

        # Some files may have a length of (0) untill a particular type is 
        # chosen (the download-links).
        if buffer_len > 0:
            if offset >= buffer_len:
                raise IndexError("Offset (%d) exceeds length of data (%d)." % 
                                 (offset, buffer_len))

            if (offset + length) > buffer_len:
                _logger.debug("Requested length (%d) from offset (%d) "
                              "exceeds file length (%d). Truncated.",
                              length, offset, buffer_len)
                length = buffer_len

        data_blocks = [block for block in self.__buffer.read(offset, length)]
        data = ''.join(data_blocks)

        _logger.debug("(%d) bytes retrieved from slice (%d):(%d)/(%d).",
                      len(data), offset, length, self.__buffer.length)

        return data

    @property
    def mime_type(self):
        return self.__mime_type

    @property
    def entry_id(self):
        return self.__entry_id

    @property
    def file_path(self):
        return build_filepath(self.__path, self.__filename)
