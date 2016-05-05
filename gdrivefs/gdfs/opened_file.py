import logging
import resource
import re
import os
import tempfile
import shutil
import threading

import fuse

from errno import *

from gdrivefs.conf import Conf
from gdrivefs.errors import ExportFormatError, GdNotFoundError
from gdrivefs.gdfs.fsutility import dec_hint, split_path, build_filepath
from gdrivefs.gdfs.displaced_file import DisplacedFile
from gdrivefs.cache.volume import PathRelations, EntryCache, path_resolver, \
                                  CLAUSE_ID, CLAUSE_ENTRY
from gdrivefs.gdtool.drive import get_gdrive
from gdrivefs.general.buffer_segments import BufferSegments

_LOGGER = logging.getLogger(__name__)

# TODO(dustin): LCM runs in a greenlet pool. When we open a file that needs the
#               existing data for a file (read, append), a switch is done to an
#               LCM worker. If the data is absent or faulted, download the
#               content. Then, switch back.


class _OpenedManager(object):
    """Manages all of the currently-open files."""

    __opened_lock = threading.RLock()
    __fh_counter = 1

    def __init__(self):
        self.__opened = {}
        self.__opened_byfile = {}
        self.__counter = 0

        self.__temp_path = tempfile.mkdtemp()
        _LOGGER.debug("Opened-file working directory: [%s]", self.__temp_path)

    def __del__(self):
        shutil.rmtree(self.__temp_path)

    def __get_max_handles(self):

        return resource.getrlimit(resource.RLIMIT_NOFILE)[0]

    def get_new_handle(self):
        """Get a handle for a file that's about to be opened. Note that the 
        handles start at (1), so there are a lot of "+ 1" occurrences below.
        """

        cls = self.__class__
        max_handles = self.__get_max_handles()

        self.__counter += 1

        with cls.__opened_lock:
            if len(self.__opened) >= (max_handles + 1):
                raise fuse.FuseOSError(EMFILE)

            safety_counter = max_handles
            while safety_counter >= 1:
                cls.__fh_counter += 1

                if cls.__fh_counter >= (max_handles + 1):
                    cls.__fh_counter = 1

                if cls.__fh_counter not in self.__opened:
                    _LOGGER.debug("Assigning file-handle (%d).",
                                  cls.__fh_counter)

                    return cls.__fh_counter
                
        message = "Could not allocate new file handle. Safety breach."
        _LOGGER.error(message)
        raise Exception(message)

    def add(self, opened_file, fh=None):
        """Registered an OpenedFile object."""

        cls = self.__class__

        assert issubclass(opened_file.__class__, OpenedFile) is True, \
               "Can only register an OpenedFile as an opened-file."

        with cls.__opened_lock:
            if not fh:
                fh = self.get_new_handle()

            elif fh in self.__opened:
                message = ("Opened-file with file-handle (%d) has already been"
                           " registered." % (opened_file.fh))

                _LOGGER.error(message)
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

        cls = self.__class__

        with cls.__opened_lock:
            _LOGGER.debug("Closing opened-file with handle (%d).", fh)

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

        cls = self.__class__

        _LOGGER.debug("Removing all open handles for file-path [%s].",
                      file_path)

        count = 0

        with cls.__opened_lock:
            try:
                for fh in self.__opened_byfile[file_path]:
                    self.remove_by_fh(fh)
                    count += 1
            except KeyError:
                pass

        _LOGGER.debug("(%d) file-handles removed for file-path [%s].",
                      count, file_path)

    def get_by_fh(self, fh):
        """Retrieve an opened-file, by the handle."""

        cls = self.__class__

        with cls.__opened_lock:
            if fh not in self.__opened:
                message = ("Opened-file with file-handle (%d) is not "
                          "registered (get_by_fh)." % (fh))

                _LOGGER.error(message)
                raise Exception(message)

            return self.__opened[fh]

    @property
    def opened_count(self):
        return self.__counter

    @property
    def temp_path(self):
        return self.__temp_path

_OPENED_ENTRIES_LOCK = threading.Lock()
_OPENED_ENTRIES = set()


class OpenedFile(object):
    """This class describes a single open file, and manages changes."""

    def __init__(self, entry_id, path, filename, is_hidden, mime_type):
# TODO(dustin): Until we can gracely orchestrate concurrent handles on the same 
#               entry, we can't allow it. This is referenced, just below.
        with _OPENED_ENTRIES_LOCK:
            assert entry_id not in _OPENED_ENTRIES, \
                   "Access to the same file from multiple file-handles is "\
                   "not currently supported."

            _OPENED_ENTRIES.add(entry_id)

        _LOGGER.info("Opened-file object created for entry-ID [%s] and path "
                     "(%s).", entry_id, path)

        self.__entry_id = entry_id
        self.__path = path
        self.__filename = filename
        self.__is_hidden = is_hidden
        
        self.__mime_type = mime_type
        self.__cache = EntryCache.get_instance().cache

        self.__is_loaded = False
        self.__is_dirty = False

        # Use the monotonically incremented `opened_count` to produce a unique 
        # temporary filepath.

        om = get_om()
        self.__temp_filepath = \
            os.path.join(om.temp_path, str(om.opened_count))

        self.__fh = None

        # We need to load this up-front. Since we can't do partial updates, we 
        # have to keep one whole, local copy, apply updates to it, and then 
        # post it on flush.
# TODO(dustin): Until we finish working on the download-agent so that we can 
#               have a way to orchestrate concurrent handles on the same file, 
#               we'll just have to accept the fact that concurrent access will 
#               require multiple downloads of the same file to multiple 
#               temporary files (one for each).
        self.__load_base_from_remote()

    def __del__(self):
        """This handle is being closed. Notice that we don't flush here because 
        we expect that the VFS will.
        """

        if self.__fh is not None:
            _LOGGER.debug("Removing temporary file [%s] ([%s]).", 
                          self.__temp_filepath, self.file_path)

            self.__fh.close()
            os.unlink(self.__temp_filepath)

        with _OPENED_ENTRIES_LOCK:
            _OPENED_ENTRIES.remove(self.__entry_id)

    def __repr__(self):
        replacements = { 
            'entry_id': self.__entry_id, 
            'filename': self.__filename, 
            'mime_type': self.__mime_type, 
            'is_loaded': self.__is_loaded, 
            'is_dirty': self.__is_dirty
        }

        return ("<OF [%(entry_id)s] F=[%(filename)s] MIME=[%(mime_type)s] "
                "LOADED=[%(is_loaded)s] DIRTY= [%(is_dirty)s]>" % replacements)

# TODO: We should be able to safely assume that we won't get a change event for 
#       a file until its been entirely updated, online. Therefore, the change 
#       processor should checkin, here, and make sure that any handles are 
#       closed for changed files.
#
#       We should also make sure to remove temporary file-paths in the OM temp-
#       path (if one exists) if we get a "delete" change.

    def __load_base_from_remote(self):
        """Download the data for the entry that we represent. This is probably 
        a file, but could also be a stub for -any- entry.
        """

        # If it's loaded and not-changed, don't do anything.
        if self.__is_loaded is True and self.__is_dirty is False:
            _LOGGER.debug("Not syncing-down non-dirty file.")
            return

        if self.__fh is not None:
            self.__fh.close()
            self.__fh = None

        entry = self.__cache.get(self.__entry_id)

        _LOGGER.debug("Ensuring local availability of [%s]: [%s]", 
                      entry, self.__temp_filepath)

        # Get the current version of the write-cache file, or note that we 
        # don't have it.

        _LOGGER.info("Attempting local cache update of file [%s] for entry "
                     "[%s] and mime-type [%s].",
                     self.__temp_filepath, entry, self.mime_type)

        if entry.requires_mimetype:
            length = DisplacedFile.file_size

            d = DisplacedFile(entry)
            stub_data = d.deposit_file(self.mime_type)

            self.__fh = open(self.__temp_filepath, 'w+')
            self.__fh.write(stub_data)
        else:
            _LOGGER.debug("Executing the download: [%s] => [%s]", 
                          entry.id, self.__temp_filepath)
            
            try:
# TODO(dustin): We need to inherit a file that we might've already cached by 
#               opening.
# TODO(dustin): Any call to download_to_local should use a local, temporarily 
#               file is already established. We can't use it in the reverse 
#               order though: It's one thing to already have a cache from 
#               having opened it, and it's a another thing to maintain a cache 
#               of every file that is copied.
                gd = get_gdrive()
                result = gd.download_to_local(
                            self.__temp_filepath,
                            entry,
                            self.mime_type)

                (length, cache_fault) = result
            except ExportFormatError:
                _LOGGER.exception("There was an export-format error.")
                raise fuse.FuseOSError(ENOENT)

            self.__fh = open(self.__temp_filepath, 'r+')

            self.__is_dirty = False
            self.__is_loaded = True

        _LOGGER.debug("Established base file-data for [%s]: [%s]", 
                      entry, self.__temp_filepath)

    @dec_hint(['offset', 'data'], ['data'], 'OF')
    def add_update(self, offset, data):
        """Queue an update to this file."""

        _LOGGER.debug("Applying update for offset (%d) and length (%d).",
                      offset, len(data))

        self.__is_dirty = True
        self.__fh.seek(offset)
        self.__fh.write(data)
        self.__fh.flush()

    @dec_hint(prefix='OF')
    def flush(self):
        """The OS wants to effect any changes made to the file."""

        _LOGGER.debug("Flushing opened-file.")

        entry = self.__cache.get(self.__entry_id)

        if self.__is_dirty is False:
            _LOGGER.debug("Flush will be skipped for [%s] because there "
                          "are no changes: [%s] IS_LOADED=[%s] "
                          "IS_DIRTY=[%d]", 
                          entry.id, self.file_path, self.__is_loaded, 
                          self.__is_dirty)
            return
        else:
            st = os.stat(self.__temp_filepath)

            _LOGGER.debug("Pushing (%d) bytes for entry with ID from [%s] to "
                          "GD for file-path [%s].",
                          st.st_size, entry.id, self.__temp_filepath)

# TODO: Make sure we sync the mtime to remote.
            gd = get_gdrive()
            entry = gd.update_entry(
                        entry, 
                        filename=entry.title, 
                        data_filepath=self.__temp_filepath, 
                        mime_type=self.mime_type, 
                        parents=entry.parents, 
                        is_hidden=self.__is_hidden)

            self.__is_dirty = False

# TODO(dustin): For now, we don't cleanup the temporary file. We need to 
#               schedule this using LRU-semantics.

            # Immediately update our current cached entry.

            _LOGGER.debug("Update successful. Updating local cache.")

            path_relations = PathRelations.get_instance()
            path_relations.register_entry(entry)

            _LOGGER.info("Update complete on entry with ID [%s].", entry.id)

    @dec_hint(['offset', 'length'], prefix='OF')
    def read(self, offset, length):
        
        _LOGGER.debug("Reading (%d) bytes at offset (%d).", length, offset)

        # We don't care if the cache file is dirty (not on this system, at 
        # least).

        st = os.stat(self.__temp_filepath)

        self.__fh.seek(offset)
        data = self.__fh.read(length)

        len_ = len(data)

        _LOGGER.debug("(%d) bytes retrieved from slice (%d):(%d)/(%d).",
                      len_, offset, length, st.st_size)

        if len_ != length:
            _LOGGER.warning("Read request is only returning (%d) bytes when "
                            "(%d) bytes were requested.", len_, length)

        return data

    @property
    def mime_type(self):
        return self.__mime_type

    @property
    def entry_id(self):
        return self.__entry_id

    @property
    def file_path(self):
        """The GD filepath of the requested file."""

        return build_filepath(self.__path, self.__filename)

def create_for_existing_filepath(filepath):
    """Process the file/path that was requested (potential export-type 
    directive, dot-prefix, etc..), and build an opened-file object using 
    the information.
    """

    _LOGGER.debug("Creating OpenedFile for [%s].", filepath)

    # Process/distill the requested file-path.

    try:
        result = split_path(filepath, path_resolver)
    except GdNotFoundError:
        _LOGGER.exception("Could not process [%s] (create_for_requested).",
                          filepath)

        raise fuse.FuseOSError(ENOENT)

    (parent_clause, path, filename, mime_type, is_hidden) = result
    distilled_filepath = build_filepath(path, filename)

    # Look-up the requested entry.

    path_relations = PathRelations.get_instance()

    try:
        entry_clause = path_relations.get_clause_from_path(
                        distilled_filepath)
    except:
        _LOGGER.exception("Could not try to get clause from path [%s] "
                          "(OpenedFile).", distilled_filepath)

        raise fuse.FuseOSError(EIO)

    if not entry_clause:
        _LOGGER.debug("Path [%s] does not exist for stat().", path)
        raise fuse.FuseOSError(ENOENT)

    entry = entry_clause[CLAUSE_ENTRY]

    # Normalize the mime-type by considering what's available for download. 
    # We're going to let the requests that didn't provide a mime-type fail 
    # right here. It will give us the opportunity to try a few options to 
    # get the file.

    try:
        final_mimetype = entry.normalize_download_mimetype(mime_type)
    except ExportFormatError:
        _LOGGER.exception("There was an export-format error "
                          "(create_for_requested_filesystem).")

        raise fuse.FuseOSError(ENOENT)
    except:
        _LOGGER.exception("Could not normalize mime-type [%s] for entry"
                          "[%s].", mime_type, entry)

        raise fuse.FuseOSError(EIO)

    if final_mimetype != mime_type:
        _LOGGER.info("Entry being opened will be opened as [%s] rather "
                     "than [%s].", final_mimetype, mime_type)

    # Build the object.

    return OpenedFile(
            entry_clause[CLAUSE_ID], 
            path, 
            filename, 
            is_hidden, 
            final_mimetype)

_management_instance = None
def get_om():
    global _management_instance
    if _management_instance is None:
        _management_instance = _OpenedManager()

    return _management_instance
