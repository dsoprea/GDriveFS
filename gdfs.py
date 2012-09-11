#!/usr/bin/python

import stat
import logging
import dateutil.parser
import getpass
import errno
import re
import json
import os
import atexit
import resource
import weakref

from errno          import *
from time           import mktime
from argparse       import ArgumentParser
from fuse           import FUSE, Operations, LoggingMixIn, FuseOSError
from sys            import argv
from threading      import Lock, RLock
from collections    import deque

from gdrivefs.utility import get_utility
from gdrivefs.gdtool import drive_proxy, NormalEntry
from gdrivefs.errors import ExportFormatError
from gdrivefs.change import get_change_manager
from gdrivefs.timer import Timers
from gdrivefs.cache import PathRelations, EntryCache, \
                           CLAUSE_ENTRY, CLAUSE_PARENT, CLAUSE_CHILDREN, \
                           CLAUSE_ID, CLAUSE_CHILDREN_LOADED

app_name = 'GDriveFS Tool'

class _NotFoundError(Exception):
    pass

class _EntryNoLongerCachedError(Exception):
    pass

def _strip_export_type(path, set_mime=True):

    rx = re.compile('(#([a-zA-Z0-9]+))?(\$)?$')
    matched = rx.search(path.encode('ASCII'))

    extension = None
    mime_type = None
    just_info = None

    if matched:
        fragment = matched.group(0)
        extension = matched.group(2)
        just_info = (matched.group(3) == '$')

        if fragment:
            path = path[:-len(fragment)]

        if not extension:
            extension_rx = re.compile('\.([a-zA-Z0-9]+)$')
            matched = extension_rx.search(path.encode('ASCII'))

            if matched:
                extension = matched.group(1)

        if extension:
            logging.info("User wants to export to extension [%s]." % 
                         (extension))

            if set_mime:
                try:
                    mime_type = get_utility().get_first_mime_type_by_extension \
                                    (extension)
                except:
                    logging.warning("Could not render a mime-type for "
                                    "prescribed extension [%s], for read." % 
                                    (extension))

                if mime_type:
                    logging.info("We have been told to export using mime-type "
                                 "[%s]." % (mime_type))

    return (path, extension, just_info, mime_type)

def _split_path(filepath):
    """Completely process and distill the requested file-path. The filename can"
    be padded for adjust what's being requested. This will remove all such 
    information, and return the actual file-path along with the extra meta-
    information.
    """

    # Remove any export-type that this file-path might've been tagged with.

    try:
        (filepath, extension, just_info, mime_type) = _strip_export_type(filepath)
    except:
        logging.exception("Could not process path [%s] for export-type." % 
                          (filepath))
        raise

    # Split the file-path into a path and a filename.

    (path, filename) = os.path.split(filepath)

    if path[0] != '/' or filename == '':
        message = ("Could not create directory with badly-formatted "
                   "file-path [%s]." % (filepath))

        logging.error(message)
        raise ValueError(message)

    # Lookup the file, as it was listed, in our cache.

    path_relations = PathRelations.get_instance()

    try:
        parent_clause = path_relations.get_clause_from_path(path)
    except:
        logging.exception("Could not get clause from path [%s]." % (path))
        raise _NotFoundError()

    if not parent_clause:
        logging.debug("Path [%s] does not exist for split." % (path))
        raise _NotFoundError()

    # Strip a prefixing dot, if present.

    if filename[0] == '.':
        is_hidden = True
        filename = filename[1:]

    else:
        is_hidden = False

    return (parent_clause, path, filename, extension, mime_type, is_hidden, 
            just_info)

class _DisplacedFile(object):
    normalized_entry = None

    file_size = 1000

    def __init__(self, normalized_entry):
        if normalized_entry.__class__ != NormalEntry:
            raise Exception("_DisplacedFile can not wrap a non-NormalEntry object.")

        self.normalized_entry = normalized_entry

    def deposit_file(self, mime_type=None):
        """Write the file to a temporary path, and present a stub (JSON) to the 
        user. This is the only way of getting files that don't have a definite 
        filesize.
        """

        if not mime_type:
            mime_type = self.normalized_entry.normalized_mime_type

        try:
            (temp_file_path, length) = drive_proxy('download_to_local', 
                                     normalized_entry=self.normalized_entry,
                                     mime_type=mime_type)
        except:
            logging.exception("Could not localize displaced file with entry "
                              "having ID [%s]." % (self.normalized_entry.id))
            raise

        try:
            return self.get_stub(mime_type, length, temp_file_path)
        except:
            logging.exception("Could not build stub.")
            raise

    def get_stub(self, mime_type=None, file_size=0, file_path=None):

        if not mime_type:
            mime_type = self.normalized_entry.normalized_mime_type

        stub_data = {
                'EntryId':              self.normalized_entry.id,
                'OriginalMimeType':     self.normalized_entry.mime_type,
                'ExportTypes':          self.normalized_entry.download_links.keys(),
                'Title':                self.normalized_entry.title,
                'Labels':               self.normalized_entry.labels,
                'FinalMimeType':        mime_type,
                'Length':               file_size,
                'Displaceable':         self.normalized_entry.requires_displaceable,
                'ImageMediaMetadata':   self.normalized_entry.image_media_metadata
            }

        if file_path:
            stub_data['FilePath'] = file_path

        try:
            result = json.dumps(stub_data)
            padding = (' ' * (self.file_size - len(result) - 1))

            return ("%s%s\n" % (result, padding))
        except:
            logging.exception("Could not serialize stub-data.")
            raise


class _OpenedManager(object):
    opened = { }
    opened_lock = RLock()
    fh_counter = 1

    @staticmethod
    def get_instance():
        with _OpenedManager.singleton_lock:
            if _OpenedManager.instance == None:
                try:
                    _OpenedManager.instance = _OpenedManager()
                except:
                    logging.exception("Could not create singleton instance of "
                                      "_OpenedManager.")
                    raise

            return _OpenedManager.instance

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
                    return self.fh_counter
                
        message = "Could not allocate new file handle. Safety breach."

        logging.error(message)
        raise Exception(message)

    def add(self, opened_file, fh=None):
        """Registered an _OpenedFile object."""

        if opened_file.__class__.__name__ != '_OpenedFile':
            message = "Can only register an _OpenedFile as an opened-file."

            logging.error(message)
            raise Exception(message)

        with self.opened_lock:
            if not fh:
                try:
                    fh = self.get_new_handle()
                except:
                    logging.exception("Could not acquire handle for "
                                      "_OpenedFile to be registered.")
                    raise

            elif fh in self.opened:
                message = ("Opened-file with file-handle (%d) has already been"
                           " registered." % (opened_file.fh))

                logging.error(message)
                raise Exception(message)

            self.opened[fh] = opened_file

            return fh

    def remove_by_fh(self, fh):
        """Remove an opened-file, by the handle."""

        with self.opened_lock:
            logging.debug("Closing opened-file with handle (%d)." % (fh))

            if fh not in self.opened:
                message = ("Opened-file with file-handle (%d) is not "
                          "registered (remove_by_fh)." % (fh))

                logging.error(message)
                raise Exception(message)

            del self.opened[fh]

    def get_by_fh(self, fh):
        """Retrieve an opened-file, by the handle."""

        with self.opened_lock:
            if fh not in self.opened:
                message = ("Opened-file with file-handle (%d) is not "
                          "registered (get_by_fh)." % (fh))

                logging.error(message)
                raise Exception(message)

            return self.opened[fh]

_OpenedManager.instance = None
_OpenedManager.singleton_lock = Lock()

class _OpenedFile(object):
    """This class describes a single open file, and manages changes. We store 
    a weakref to the entry record so that we can tell if it's uncached/cleaned-
    up.
    """

    entry_id        = None
    path            = None
    filename        = None
    is_hidden       = None
    mime_type       = None

    file_path       = None
    cache           = None
    temp_file_path  = None
    last_file_size  = None
    buffer          = None

    updates         = deque()
    update_lock     = Lock()
    download_lock   = Lock()

    @staticmethod
    def create_for_requested_filepath(filepath):
        """Process the file/path that was requested (potential export-type 
        directive, dot-prefix, etc..), and build an opened-file object using 
        the information.
        """

        logging.debug("Creating _OpenedFile for [%s]." % (filepath))

        # Process/distill the requested file-path.

        try:
            (parent_clause, path, filename, extension, mime_type, is_hidden, \
             just_info) = _split_path(filepath)
        except _NotFoundError:
            logging.exception("Could not process [%s] (create).")
            raise FuseOSError(ENOENT)
        except:
            logging.exception("Could not split path [%s] (create)." % 
                              (filepath))
            raise

        # Look-up the requested entry.

        path_relations = PathRelations.get_instance()

        try:
            entry_clause = path_relations.get_clause_from_path(filepath)
        except:
            logging.exception("Could not try to get clause from path [%s] "
                              "(_OpenedFile)." % (filepath))
            raise FuseOSError(ENOENT)

        if not entry_clause:
            logging.debug("Path [%s] does not exist for stat()." % (path))
            raise FuseOSError(ENOENT)

        # Build the object.

        try:
            return _OpenedFile(entry_clause[CLAUSE_ID], path, filename, is_hidden, mime_type)
        except:
            logging.exception("Could not create _OpenedFile for requested file"
                              " [%s]." % (filepath))
            raise

    def __init__(self, entry_id, path, filename, is_hidden, mime_type):

        logging.info("Opened-file object created for entry-ID [%s] and path "
                     "(%s)." % (entry_id, path))

        self.entry_id   = entry_id
        self.path       = path
        self.filename   = filename
        self.is_hidden  = is_hidden
        self.mime_type  = mime_type
        self.cache      = EntryCache.get_instance().cache

    def __marker(self, name, data=None):
        if data == None:
            data = [ ]

        logging.info("OPEN: %s ========== %s, %s" % 
                     (name, self.entry_id, self.path))

        if data:
            phrases = [ ("%s= [%s]" % (k, v)) for k, v in data.iteritems() ]
            logging.debug("%s" % (', '.join(phrases)))

    def __get_entry_or_raise(self):
        """We can never be sure that the entry will still be known to the 
        system. Grab it and throw an error if it's not available.
        """

        logging.debug("Retrieving entry for opened-file with entry-ID [%s]." % 
                      (self.entry_id))

        try:
            return self.cache.get(self.entry_id)
        except:
            logging.exception("Could not retrieve entry with ID [%s] for the "
                              "opened-file." % (self.entry_id))
            raise 

    def __load_base_from_remote(self):
        """Download the data for the file that we represent."""

        self.__marker('load_base_from_remote')

        logging.debug("Retrieving entry for load_base_from_remote.")

        try:
            entry = self.__get_entry_or_raise()
        except:
            logging.exception("Could not get entry with ID [%s] for "
                              "write-flush." % (self.entry_id))
            raise

        with self.download_lock:
            # Get the current version of the write-cache file, or note that we 
            # don't have it.

            logging.debug("Checking state of current write-cache file.")

            update_cached_file = True
            if not self.buffer:
                try:
                    stat = os.stat(self.temp_file_path)
                except:
                    logging.debug("Write-cache file does not seem to exist.")
                else:
                    # Our buffer always matches the write-cache file, and 
                    # because our "entry" object is a reference to our cache 
                    # and our cache is always going to be up to date because of
                    # our change-management framework, we'll only do an update 
                    # when one is needed, up to within the resolution of our 
                    # change checks.
                    if entry.modified_date == stat.st_mtime:
                        update_cached_file = False

            # We don't yet have a copy of the file, or it has been changed by 
            # someone else.

            if not update_cached_file:
                logging.debug("Write-cache file [%s] is already up-to-date." %
                              (self.temp_file_path))
                return

            logging.info("Updating write-cache file [%s]." % 
                         (self.temp_file_path))

            # The output path is predictable. It shouldn't change.

            try:
                (temp_file_path, length) = \
                    drive_proxy('download_to_local', 
                                    normalized_entry=entry,
                                    mime_type=self.mime_type)
            except (ExportFormatError):
                raise FuseOSError(ENOENT)
            except:
                logging.exception("Could not localize file with entry having "
                                  "ID [%s]." % (self.entry_id))
                raise

            self.temp_file_path = temp_file_path
            self.last_file_size = length

            # Load our buffer.

            logging.debug("Reading write-cache file.")

            with open(self.temp_file_path, 'rb') as f:
                # Read the locally cached file in.

                try:
                    self.buffer = f.read()
                except:
                    logging.exception("Could not read current cached file into buffer.")
                    raise

    def add_update(self, offset, data):
        """Queue an update to this file."""

        self.__marker('add_update', { 'offset': offset, 
                                    'actual_length': len(data) })

        with self.update_lock:
            self.updates.append((offset, data))

        logging.debug("(%d) updates have been queued." % (len(self.updates)))

    def flush(self):
        """The OS wants to effect any changes made to the file."""

        self.__marker('flush', { 'waiting': len(self.updates) })

        logging.debug("Retrieving entry for write-flush.")

        try:
            entry = self.__get_entry_or_raise()
        except:
            logging.exception("Could not get entry with ID [%s] for "
                              "write-flush." % (self.entry_id))
            raise
    
        with self.update_lock:
            if not self.updates:
                logging.debug("Flush will be skipped due to empty write-"
                              "queue.")
                return

            logging.debug("Checking write-cache file (flush).")

            try:
                self.__load_base_from_remote()
            except:
                logging.exception("Could not load write-cache file [%s]." % 
                                  (self.temp_file_path))
                raise

            # Apply updates to the data.

            logging.debug("Applying (%d) updates." % (len(self.updates)))

            i = 0
            while self.updates:
                (offset, data) = self.updates.popleft()
                logging.debug("Applying update (%d) at offset (%d) with data-"
                              "length (%d)." % (i, offset, len(data)))

                right_fragment_start = offset + len(data)
                    
                self.buffer = self.buffer[0:offset] + data + \
                                self.buffer[right_fragment_start:]

                i += 1

            # Push to GD.

            logging.debug("Pushing (%d) bytes for entry with ID from [%s] to "
                          "GD." % (len(self.buffer), entry.id, 
                                   self.temp_file_path))

            try:
                entry = drive_proxy('create_file', filename=self.filename, 
                                    parents=entry.parents,
                                    is_hidden=self.is_hidden, 
                                    update_on_id=entry.id,
                                    data_filepath=self.temp_file_path)
            except:
                logging.exception("Could not localize displaced file with "
                                  "entry having ID [%s]." % 
                                  (self.normalized_entry.id))
                raise

            # Update the write-cache file to the official mtime. We won't 
            # redownload it on the next flush if it wasn't changed, elsewhere.

            logging.debug("Updating local write-cache file to official mtime "
                          "[%s]." % (entry.modified_date))

            try:
                os.utime(self.temp_file_path, 
                         [ entry.modified_date, entry.modified_date ])
            except:
                logging.exception("Could not update mtime of write-cache [%s] "
                                  "for entry with ID [%s], post-flush." % 
                                  (entry.modified_date, entry.id))
                raise

        # Immediately update our current cached entry.

        logging.debug("Update successful. Updating local cache.")

        path_relations = PathRelations.get_instance()

        try:
            path_relations.register_entry(entry)
        except:
            logging.exception("Could not register updated file in cache.")
            raise

        logging.info("Update complete on entry with ID [%s]." % (entry.id))

    def read(self, offset, length):
        
        logging.debug("Checking write-cache file (flush).")

        try:
            self.__load_base_from_remote()
        except:
            logging.exception("Could not load write-cache file [%s]." % 
                              (self.temp_file_path))
            raise

        return self.buffer[offset:length]

# TODO: make sure strip_extension and split_path are used when each are relevant
# TODO: make sure create path reserves a file-handle, uploads the data, and then registers the open-file with the file-handle.
# TODO: make sureto finish the opened-file helper factory.


class _GDriveFS(LoggingMixIn,Operations):
    """The main filesystem class."""

    def __marker(self, name, data=None):
        if data == None:
            data = [ ]

        logging.info("========== %s ==========" % (name))

        if data:
            phrases = [ ("%s= [%s]" % (k, v)) for k, v in data.iteritems() ]
            logging.debug("%s" % (', '.join(phrases)))

    def __register_open_file(self, fh, path, entry_id):

        with self.fh_lock:
            self.open_files[fh] = (entry_id, path)

    def __deregister_open_file(self, fh):

        with self.fh_lock:
            try:
                file_info = self.open_files[fh]
            except:
                logging.exception("Could not deregister invalid file-handle "
                                  "(%d)." % (fh))
                raise

            del self.open_files[fh]
            return file_info

    def __get_open_file(self, fh):

        with self.fh_lock:
            try:
                return self.open_files[fh]
            except:
                logging.exception("Could not retrieve on invalid file-handle "
                                  "(%d)." % (fh))
                raise

    def getattr(self, raw_path, fh=None):
        """Return a stat() structure."""

        self.__marker('getattr', { 'raw_path': raw_path, 'fh': fh })

        try:
            (path, extension, just_info, mime_type) = _strip_export_type \
                                                        (raw_path, True)
        except:
            logging.exception("Could not process export-type directives.")
            raise

        path_relations = PathRelations.get_instance()

        try:
            entry_clause = path_relations.get_clause_from_path(path)
        except:
            logging.exception("Could not try to get clause from path [%s] "
                              "(getattr)." % (path))
            raise FuseOSError(ENOENT)

        if not entry_clause:
            logging.debug("Path [%s] does not exist for stat()." % (path))
            raise FuseOSError(ENOENT)

        effective_permission = 0444
        normalized_entry = entry_clause[0]

        entry = entry_clause[0]

        # If the user has required info, we'll treat folders like files so that 
        # we can return the info.
        is_folder = get_utility().is_directory(entry) and not just_info

        if entry.editable:
            effective_permission |= 0222

        date_obj = dateutil.parser.parse(entry.modified_date)
        mtime_epoch = mktime(date_obj.timetuple())

        stat_result = { "st_mtime": mtime_epoch }
        
        stat_result["st_size"] = _DisplacedFile(entry).file_size \
                                    if (is_folder or \
                                            entry.requires_displaceable) \
                                    else entry.file_size

        if is_folder:
            effective_permission |= 0111
            stat_result["st_mode"] = (stat.S_IFDIR | effective_permission)

            stat_result["st_nlink"] = 2
        else:
            stat_result["st_mode"] = (stat.S_IFREG | effective_permission)
            stat_result["st_nlink"] = 1

        return stat_result

    def readdir(self, path, offset):
        """A generator returning one base filename at a time."""

        self.__marker('readdir', { 'path': path, 'offset': offset })

        # We expect "offset" to always be (0).
        if offset != 0:
            logging.warning("readdir() has been invoked for path [%s] and non-"
                            "zero offset (%d). This is not allowed." % 
                            (path, offset))

# TODO: Once we start working on the cache, make sure we don't make this call, 
#       constantly.

        path_relations = PathRelations.get_instance()

        logging.debug("Listing files.")

        try:
            entry_clause = path_relations.get_clause_from_path(path)
        except:
            logging.exception("Could not get clause from path [%s] "
                              "(readdir)." % (path))
            raise FuseOSError(ENOENT)

        if not entry_clause:
            logging.debug("Path [%s] does not exist for readdir()." % (path))
            raise FuseOSError(ENOENT)

        try:
            filenames = path_relations.get_child_filenames_from_entry_id \
                            (entry_clause[3])
        except:
            logging.exception("Could not render list of filenames under path "
                             "[%s]." % (path))
            raise

        filenames[0:0] = ['.','..']

        for filename in filenames:
            yield filename

    def read(self, raw_path, length, offset, fh):

        self.__marker('read', { 'raw_path': raw_path, 'length': length, 
                                'offset': offset, 'fh': fh })
#
#        # Fetch the file to a local, temporary file.
#
#        if normalized_entry.requires_displaceable or just_info:
#            logging.info("Doing displaced-file download of entry with ID "
#                         "[%s]." % (entry_id))
#
#            try:
#                displaced = _DisplacedFile(normalized_entry)
#            except:
#                logging.exception("Could not wrap entry in _DisplacedFile.")
#                raise
#
#            try:
#                if just_info:
#                    logging.debug("Info for file was requested, rather than "
#                                  "the file itself.")
#                    return displaced.get_stub(mime_type)
#                else:
#                    logging.debug("A displaceable file was requested.")
#                    return displaced.deposit_file(mime_type)
#            except:
#                logging.exception("Could not do displaced-file download.")
#                raise
#
#        else:
#            logging.info("Downloading entry with ID [%s] for path [%s]." % 
#                         (entry_id, path))

        try:
            opened_file = _OpenedManager.get_instance().get_by_fh(fh)
        except:
            logging.exception("Could not retrieve _OpenedFile for handle with "
                              "ID (%d) (read)." % (fh))
            raise

        try:
            return opened_file.read(offset, length)
        except:
            logging.exception("Could not read data.")
            raise

    def mkdir(self, filepath, mode):
        """Create the given directory."""

        self.__marker('mkdir', { 'filepath': filepath, 'mode': oct(mode) })

# TODO: Implement the "mode".

        try:
            (parent_clause, path, filename, extension, mime_type, is_hidden, \
             just_info) = _split_path(filepath)
        except _NotFoundError:
            logging.exception("Could not process [%s] (mkdir).")
            raise FuseOSError(ENOENT)
        except:
            logging.exception("Could not split path [%s] (mkdir)." % 
                              (filepath))
            raise

        logging.debug("Creating directory [%s] under [%s]." % (filename, path))

        try:
            entry = drive_proxy('create_directory', filename=filename, 
                        parents=[parent_clause[0].id], is_hidden=is_hidden)
        except:
            logging.exception("Could not localize displaced file with entry "
                              "having ID [%s]." % (self.normalized_entry.id))
            raise

        logging.info("Directory [%s] created as ID [%s]." % (filepath, 
                     entry.id))

        #parent_clause[4] = False

        path_relations = PathRelations.get_instance()

        try:
            path_relations.register_entry(entry)
        except:
            logging.exception("Could not register new directory in cache.")
            raise

    def create(self, filepath, mode):
        """Create a new file. This always precedes a write."""
# TODO: Implement mode.
        self.__marker('create', { 'filepath': filepath, 'mode': oct(mode) })

        logging.debug("Splitting file-path [%s] for create." % (filepath))

        try:
            (parent_clause, path, filename, extension, mime_type, is_hidden, \
             just_info) = _split_path(filepath)
        except _NotFoundError:
            logging.exception("Could not process [%s] (create).")
            raise FuseOSError(ENOENT)
        except:
            logging.exception("Could not split path [%s] (create)." % 
                              (filepath))
            raise

        logging.debug("Acquiring file-handle.")

        try:
            fh = _OpenedManager.get_instance().get_new_handle()
        except:
            logging.exception("Could not acquire file-handle for create of "
                              "[%s]." % (filepath))
            raise

        logging.debug("Creating empty file [%s] under parent with ID [%s]." % 
                      (filename, parent_clause[3]))

        try:
            entry = drive_proxy('create_file', filename=filename, 
                        parents=[parent_clause[3]], is_hidden=is_hidden)
        except:
            logging.exception("Could not create empty file [%s] under parent "
                              "with ID [%s]." % (filename, parent_clause[3]))
            raise

        logging.debug("Registering created file in cache.")

        path_relations = PathRelations.get_instance()

        try:
            path_relations.register_entry(entry)
        except:
            logging.exception("Could not register created file in cache.")
            raise

        logging.debug("Building _OpenedFile object for created file.")

        try:
            opened_file = _OpenedFile(entry.id, path, filename, is_hidden, mime_type)
        except:
            logging.exception("Could not create _OpenedFile object for "
                              "created file.")
            raise

        logging.debug("Registering _OpenedFile object with handle (%d), path "
                      "[%s], and ID [%s]." % (fh, filepath, entry.id))

        try:
            _OpenedManager.get_instance().add(opened_file, fh=fh)
        except:
            logging.exception("Could not register _OpenedFile for created "
                              "file.")
            raise

        logging.debug("File created, opened, and completely registered.")

        return fh

    def open(self, filepath, flags):

        self.__marker('open', { 'filepath': filepath })

        logging.debug("Building _OpenedFile object for file being opened.")

        try:
            opened_file = _OpenedFile.create_for_requested_filepath(filepath)
        except:
            logging.exception("Could not create _OpenedFile object for "
                              "opened filepath.")
            raise

        logging.debug("_OpenedFile object with path [%s] and ID [%s]." % 
                      (filepath, opened_file.entry_id))

        try:
            fh = _OpenedManager.get_instance().add(opened_file)
        except:
            logging.exception("Could not register _OpenedFile for opened "
                              "file.")
            raise

        logging.debug("File opened.")

        return fh

    def release(self, filepath, fh):
        """Close a file."""

        self.__marker('release', { 'filepath': filepath, 'fh': fh })

        try:
            _OpenedManager.get_instance().remove_by_fh(fh)
        except:
            logging.exception("Could not remove _OpenedFile for handle with ID"
                              "(%d) (release)." % (fh))
            raise

    def write(self, filepath, data, offset, fh):

        self.__marker('write', { 'path': path, '#data': len(data), 
                                 'offset': offset, 'fh': fh })

        try:
            opened_file = _OpenedManager.get_instance().get_by_fh(fh=fh)
        except:
            logging.exception("Could not get _OpenedFile (write).")
            raise

        try:
            opened_file.add_update(offset, data)
        except:
            logging.exception("Could not queue file-update.")
            raise

        return len(data)

    def flush(self, filepath, fh):
        
        self.__marker('flush', { 'fh': fh })

        try:
            opened_file = _OpenedManager.get_instance().get_by_fh(fh=fh)
        except:
            logging.exception("Could not get _OpenedFile (flush).")
            raise

        try:
            opened_file.flush()
        except:
            logging.exception("Could not flush local updates.")
            raise

    def init(self, path):
        """Called on filesystem mount. Path is always /."""

        self.__marker('init', { 'path': path })

        atexit.register(Timers.get_instance().cancel_all)

        get_change_manager().mount_init()

    def destroy(self, path):
        """Called on filesystem destruction. Path is always /."""

        self.__marker('destroy', { 'path': path })

        Timers.get_instance().cancel_all()

        get_change_manager().mount_destroy()

def main():

    if len(argv) != 2:
        print 'usage: %s <mountpoint>' % argv[0]
        exit(1)

    fuse = FUSE(_GDriveFS(), argv[1], foreground=True, nothreads=True)

if __name__ == "__main__":
    main()

