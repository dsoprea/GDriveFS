import stat
import logging
import dateutil.parser
import re
import json
import os
import atexit
import resource

from errno import ENOENT, EIO, ENOTDIR, ENOTEMPTY, EPERM, EEXIST
from fuse import FUSE, Operations, FuseOSError, c_statvfs, fuse_get_context, \
                 LoggingMixIn
from time import mktime, time
from sys import argv, exit, excepthook
from mimetypes import guess_type
from datetime import datetime
from dateutil.tz import tzlocal, tzutc
from os.path import split

from gdrivefs.utility import get_utility
from gdrivefs.change import get_change_manager
from gdrivefs.timer import Timers
from gdrivefs.cache.volume import PathRelations, EntryCache, \
                                  CLAUSE_ENTRY, CLAUSE_PARENT, \
                                  CLAUSE_CHILDREN, CLAUSE_ID, \
                                  CLAUSE_CHILDREN_LOADED
from gdrivefs.conf import Conf
from gdrivefs.gdfs.fsutility import dec_hint
from gdrivefs.gdtool.oauth_authorize import get_auth
from gdrivefs.gdtool.drive import drive_proxy
from gdrivefs.gdtool.account_info import AccountInfo
from gdrivefs.general.buffer_segments import BufferSegments
from gdrivefs.gdfs.opened_file import OpenedManager, OpenedFile
from gdrivefs.gdfs.fsutility import strip_export_type, split_path,\
                                    build_filepath
from gdrivefs.gdfs.displaced_file import DisplacedFile
from gdrivefs.cache.volume import path_resolver
from gdrivefs.errors import GdNotFoundError
from gdrivefs.time_support import get_flat_normal_fs_time_from_epoch

_static_log = logging.getLogger().getChild('(GDFS)')


# TODO: make sure strip_extension and split_path are used when each are relevant
# TODO: make sure create path reserves a file-handle, uploads the data, and then registers the open-file with the file-handle.
# TODO: Make sure that we rely purely on the FH, whenever it is given, 
#       whereever it appears. This will be to accomodate system calls that can work either via file-path or file-handle.

def set_datetime_tz(datetime_obj, tz):
    return datetime_obj.replace(tzinfo=tz)

class GDriveFS(LoggingMixIn,Operations):
    """The main filesystem class."""

    __log = None

    def __init__(self):
        Operations.__init__(self)

        self.__log = logging.getLogger().getChild('GD_VFS')

    def __register_open_file(self, fh, path, entry_id):

        with self.fh_lock:
            self.open_files[fh] = (entry_id, path)

    def __deregister_open_file(self, fh):

        with self.fh_lock:
            try:
                file_info = self.open_files[fh]
            except:
                self.__log.exception("Could not deregister invalid file-handle "
                                  "(%d)." % (fh))
                raise

            del self.open_files[fh]
            return file_info

    def __get_open_file(self, fh):

        with self.fh_lock:
            try:
                return self.open_files[fh]
            except:
                self.__log.exception("Could not retrieve on invalid file-handle "
                                  "(%d)." % (fh))
                raise

    def __get_entry_or_raise(self, raw_path, allow_normal_for_missing=False):
        try:
            result = split_path(raw_path, path_resolver)
            (parent_clause, path, filename, mime_type, is_hidden) = result
        except GdNotFoundError:
            self.__log.exception("Could not retrieve clause for non-existent "
                                 "file-path [%s] (parent does not exist)." % 
                                 (raw_path))

            if allow_normal_for_missing is True:
                raise
            else:
                raise FuseOSError(ENOENT)
        except:
            self.__log.exception("Could not process file-path [%s]." % 
                                 (raw_path))
            raise FuseOSError(EIO)

        filepath = build_filepath(path, filename)
        path_relations = PathRelations.get_instance()

        try:
            entry_clause = path_relations.get_clause_from_path(filepath)
        except GdNotFoundError:
            self.__log.exception("Could not retrieve clause for non-existent "
                                 "file-path [%s] (parent exists)." % 
                                 (filepath))

            if allow_normal_for_missing is True:
                raise
            else:
                raise FuseOSError(ENOENT)
        except:
            self.__log.exception("Could not retrieve clause for path [%s]. " %
                                 (filepath))
            raise FuseOSError(EIO)

        if not entry_clause:
            self.__log.debug("Path [%s] does not exist for stat()." % (filepath))

            if allow_normal_for_missing is True:
                raise GdNotFoundError()
            else:
                raise FuseOSError(ENOENT)

        return (entry_clause[CLAUSE_ENTRY], path, filename)

    @dec_hint(['raw_path', 'fh'])
    def getattr(self, raw_path, fh=None):
        """Return a stat() structure."""
# TODO: Implement handle.

        (entry, path, filename) = self.__get_entry_or_raise(raw_path)
        (uid, gid, pid) = fuse_get_context()

        self.__log.debug("Context: UID= (%d) GID= (%d) PID= (%d)" % (uid, gid, 
                                                                     pid))

        if entry.is_directory:
            effective_permission = int(Conf.get('default_perm_folder'), 8)
        elif entry.editable:
            effective_permission = int(Conf.get('default_perm_file_editable'), 8)
        else:
            effective_permission = int(Conf.get('default_perm_file_noneditable'), 8)

        stat_result = { "st_mtime": entry.modified_date_epoch, # modified time.
                        "st_ctime": entry.modified_date_epoch, # changed time.
                        "st_atime": time(),
                        "st_uid":   uid,
                        "st_gid":   gid }
        
        if entry.is_directory:
            # Per http://sourceforge.net/apps/mediawiki/fuse/index.php?title=SimpleFilesystemHowto, 
            # default size should be 4K.
            stat_result["st_size"] = 1024 * 4
            stat_result["st_mode"] = (stat.S_IFDIR | effective_permission)
            stat_result["st_nlink"] = 2
        else:
            stat_result["st_size"] = DisplacedFile.file_size \
                                        if entry.requires_mimetype \
                                        else entry.file_size

            stat_result["st_mode"] = (stat.S_IFREG | effective_permission)
            stat_result["st_nlink"] = 1

        return stat_result

    @dec_hint(['path', 'offset'])
    def readdir(self, path, offset):
        """A generator returning one base filename at a time."""

        # We expect "offset" to always be (0).
        if offset != 0:
            self.__log.warning("readdir() has been invoked for path [%s] and non-"
                            "zero offset (%d). This is not allowed." % 
                            (path, offset))

# TODO: Once we start working on the cache, make sure we don't make this call, 
#       constantly.

        path_relations = PathRelations.get_instance()

        self.__log.debug("Listing files.")

        try:
            entry_clause = path_relations.get_clause_from_path(path)
        except GdNotFoundError:
            self.__log.exception("Could not process [%s] (readdir).")
            raise FuseOSError(ENOENT)
        except:
            self.__log.exception("Could not get clause from path [%s] "
                              "(readdir)." % (path))
            raise FuseOSError(EIO)

        if not entry_clause:
            self.__log.debug("Path [%s] does not exist for readdir()." % (path))
            raise FuseOSError(ENOENT)

        try:
            entry_tuples = path_relations.get_children_entries_from_entry_id \
                            (entry_clause[CLAUSE_ID])
        except:
            self.__log.exception("Could not render list of filenames under path "
                             "[%s]." % (path))
            raise FuseOSError(EIO)

        yield '.'
        yield '..'

        for (filename, entry) in entry_tuples:

            # Decorate any file that -requires- a mime-type (all files can 
            # merely accept a mime-type)
            if entry.requires_mimetype:
                filename += '#'
        
            yield filename

    @dec_hint(['raw_path', 'length', 'offset', 'fh'])
    def read(self, raw_path, length, offset, fh):

        try:
            opened_file = OpenedManager.get_instance().get_by_fh(fh)
        except:
            self.__log.exception("Could not retrieve OpenedFile for handle "
                                 "with ID (%d) (read)." % (fh))
            raise FuseOSError(EIO)

        try:
            return opened_file.read(offset, length)
        except:
            self.__log.exception("Could not read data.")
            raise FuseOSError(EIO)

    @dec_hint(['filepath', 'mode'])
    def mkdir(self, filepath, mode):
        """Create the given directory."""

# TODO: Implement the "mode".

        try:
            result = split_path(filepath, path_resolver)
            (parent_clause, path, filename, mime_type, is_hidden) = result
        except GdNotFoundError:
            self.__log.exception("Could not process [%s] (mkdir).")
            raise FuseOSError(ENOENT)
        except:
            self.__log.exception("Could not split path [%s] (mkdir)." % 
                              (filepath))
            raise FuseOSError(EIO)

        parent_id = parent_clause[CLAUSE_ID]

        self.__log.debug("Creating directory [%s] under parent [%s] with ID "
                         "[%s]." % (filename, path, parent_id))

        try:
            entry = drive_proxy('create_directory', 
                                filename=filename, 
                                parents=[parent_id], 
                                is_hidden=is_hidden)
        except:
            self.__log.exception("Could not create directory with name [%s] "
                                 "and parent with ID [%s]." % 
                                 (filename, parent_clause[0].id))
            raise FuseOSError(EIO)

        self.__log.info("Directory [%s] created as ID [%s] under parent with "
                        "ID [%s]." % (filepath, entry.id, parent_id))

        #parent_clause[4] = False

        path_relations = PathRelations.get_instance()

        try:
            path_relations.register_entry(entry)
        except:
            self.__log.exception("Could not register new directory in cache.")
            raise FuseOSError(EIO)

# TODO: Find a way to implement or enforce 'mode'.
    def __create(self, filepath, mode=None):
        """Create a new file.
                
        We don't implement "mode" (permissions) because the model doesn't agree 
        with GD.
        """
# TODO: Fail if it already exists.

        self.__log.debug("Splitting file-path [%s] for inner create." % 
                         (filepath))

        try:
            result = split_path(filepath, path_resolver)
            (parent_clause, path, filename, mime_type, is_hidden) = result
        except GdNotFoundError:
            self.__log.exception("Could not process [%s] (i-create).")
            raise FuseOSError(ENOENT)
        except:
            self.__log.exception("Could not split path [%s] (i-create)." % 
                              (filepath))
            raise FuseOSError(EIO)

        distilled_filepath = build_filepath(path, filename)

        self.__log.debug("Acquiring file-handle.")

        # Try to guess at a mime-type, if not otherwise given.
        if mime_type is None:
            (mimetype_guess, _) = guess_type(filename, True)
            
            if mimetype_guess is not None:
                mime_type = mimetype_guess
            else:
                mime_type = Conf.get('default_mimetype')

        self.__log.debug("Creating empty file [%s] under parent with ID "
                         "[%s]." % (filename, parent_clause[3]))

        try:
            entry = drive_proxy('create_file', filename=filename, 
                                data_filepath='/dev/null', 
                                parents=[parent_clause[3]], 
                                mime_type=mime_type,
                                is_hidden=is_hidden)
        except:
            self.__log.exception("Could not create empty file [%s] under "
                                 "parent with ID [%s]." % (filename, 
                                                           parent_clause[3]))
            raise FuseOSError(EIO)

        self.__log.debug("Registering created file in cache.")

        path_relations = PathRelations.get_instance()

        try:
            path_relations.register_entry(entry)
        except:
            self.__log.exception("Could not register created file in cache.")
            raise FuseOSError(EIO)

        self.__log.info("Inner-create of [%s] completed." % 
                        (distilled_filepath))

        return (entry, path, filename, mime_type)

    @dec_hint(['filepath', 'mode'])
    def create(self, raw_filepath, mode):
        """Create a new file. This always precedes a write."""

        self.__log.debug("Acquiring file-handle.")

        try:
            fh = OpenedManager.get_instance().get_new_handle()
        except:
            self.__log.exception("Could not acquire file-handle for create of "
                                 "[%s]." % (raw_filepath))
            raise FuseOSError(EIO)

        (entry, path, filename, mime_type) = self.__create(raw_filepath)

        self.__log.debug("Building OpenedFile object for created file.")

        try:
            opened_file = OpenedFile(entry.id, path, filename, 
                                     not entry.is_visible, mime_type)
        except:
            self.__log.exception("Could not create OpenedFile object for "
                                 "created file.")
            raise FuseOSError(EIO)

        self.__log.debug("Registering OpenedFile object with handle (%d), "
                         "path [%s], and ID [%s]." % 
                         (fh, raw_filepath, entry.id))

        try:
            OpenedManager.get_instance().add(opened_file, fh=fh)
        except:
            self.__log.exception("Could not register OpenedFile for created "
                                 "file.")
            raise FuseOSError(EIO)

        self.__log.debug("File created, opened, and completely registered.")

        return fh

    @dec_hint(['filepath', 'flags'])
    def open(self, filepath, flags):
# TODO: Fail if does not exist and the mode/flags is read only.

        self.__log.debug("Building OpenedFile object for file being opened.")

        try:
            opened_file = OpenedFile.create_for_requested_filepath(filepath)
        except GdNotFoundError:
            self.__log.exception("Could not create handle for requested [%s] "
                                 "(open)." % (filepath))
            raise FuseOSError(ENOENT)
        except:
            self.__log.exception("Could not create OpenedFile object for "
                                 "opened filepath [%s]." % (filepath))
            raise FuseOSError(EIO)

        self.__log.debug("Created OpenedFile object [%s]." % (opened_file))

        try:
            fh = OpenedManager.get_instance().add(opened_file)
        except:
            self.__log.exception("Could not register OpenedFile for opened "
                                 "file.")
            raise FuseOSError(EIO)

        self.__log.debug("File opened.")

        return fh

    @dec_hint(['filepath', 'fh'])
    def release(self, filepath, fh):
        """Close a file."""

        try:
            OpenedManager.get_instance().remove_by_fh(fh)
        except:
            self.__log.exception("Could not remove OpenedFile for handle with "
                                 "ID (%d) (release)." % (fh))
            raise FuseOSError(EIO)

    @dec_hint(['filepath', 'data', 'offset', 'fh'], ['data'])
    def write(self, filepath, data, offset, fh):

        self.__log.debug("Write data length is (%d)." % (len(data)))

        try:
            opened_file = OpenedManager.get_instance().get_by_fh(fh=fh)
        except:
            self.__log.exception("Could not get OpenedFile (write).")
            raise FuseOSError(EIO)

        try:
            opened_file.add_update(offset, data)
        except:
            self.__log.exception("Could not queue file-update.")
            raise FuseOSError(EIO)

        self.__log.debug("Write queued.")

        return len(data)

    @dec_hint(['filepath', 'fh'])
    def flush(self, filepath, fh):
        
        try:
            opened_file = OpenedManager.get_instance().get_by_fh(fh=fh)
        except:
            self.__log.exception("Could not get OpenedFile (flush).")
            raise FuseOSError(EIO)

        try:
            opened_file.flush()
        except:
            self.__log.exception("Could not flush local updates.")
            raise FuseOSError(EIO)

    @dec_hint(['filepath'])
    def rmdir(self, filepath):
        """Remove a directory."""

        path_relations = PathRelations.get_instance()

        self.__log.debug("Removing directory [%s]." % (filepath))

        try:
            entry_clause = path_relations.get_clause_from_path(filepath)
        except GdNotFoundError:
            self.__log.exception("Could not process [%s] (rmdir).")
            raise FuseOSError(ENOENT)
        except:
            self.__log.exception("Could not get clause from file-path [%s] "
                              "(rmdir)." % (filepath))
            raise FuseOSError(EIO)

        if not entry_clause:
            self.__log.error("Path [%s] does not exist for rmdir()." % (filepath))
            raise FuseOSError(ENOENT)

        entry_id = entry_clause[CLAUSE_ID]
        normalized_entry = entry_clause[CLAUSE_ENTRY]

        # Check if not a directory.

        self.__log.debug("Ensuring it is a directory.")

        if not normalized_entry.is_directory:
            self.__log.error("Can not rmdir() non-directory [%s] with ID [%s].", filepath, entry_id)
            raise FuseOSError(ENOTDIR)

        # Ensure the folder is empty.

        self.__log.debug("Checking if empty.")

        try:
            found = drive_proxy('get_children_under_parent_id', 
                                parent_id=entry_id,
                                max_results=1)
        except:
            self.__log.exception("Could not determine if directory to be removed "
                              "has children." % (entry_id))
            raise FuseOSError(EIO)

        if found:
            raise FuseOSError(ENOTEMPTY)

        self.__log.debug("Doing remove of directory [%s] with ID [%s]." % 
                      (filepath, entry_id))

        try:
            drive_proxy('remove_entry', normalized_entry=normalized_entry)
        except (NameError):
            raise FuseOSError(ENOENT)
        except:
            self.__log.exception("Could not remove directory [%s] with ID [%s]." % 
                              (filepath, entry_id))
            raise FuseOSError(EIO)
# TODO: Remove from cache.
        self.__log.debug("Directory removal complete.")

    # Not supported. Google Drive doesn't fit within this model.
    @dec_hint(['filepath', 'mode'])
    def chmod(self, filepath, mode):

        raise FuseOSError(EPERM) # Operation not permitted.

    # Not supported. Google Drive doesn't fit within this model.
    @dec_hint(['filepath', 'uid', 'gid'])
    def chown(self, filepath, uid, gid):

        raise FuseOSError(EPERM) # Operation not permitted.

    # Not supported.
    @dec_hint(['target', 'source'])
    def symlink(self, target, source):

        raise FuseOSError(EPERM)

    # Not supported.
    @dec_hint(['filepath'])
    def readlink(self, filepath):

        raise FuseOSError(EPERM)

    @dec_hint(['filepath'])
    def statfs(self, filepath):
        """Return filesystem status info (for df).

        The given file-path seems to always be '/'.

        REF: http://www.ibm.com/developerworks/linux/library/l-fuse/
        REF: http://stackoverflow.com/questions/4965355/converting-statvfs-to-percentage-free-correctly
        """

        block_size = 512

        try:
            account_info = AccountInfo.get_instance()
            total = account_info.quota_bytes_total / block_size
            used = account_info.quota_bytes_used / block_size
            free = total - used
        except:
            self.__log.exception("Could not get account-info.")
            raise FuseOSError(EIO)

        return {
            # Optimal transfer block size.
            'f_bsize': block_size,

            # Total data blocks in file system.
            'f_blocks': total,

            # Fragment size.
            'f_frsize': block_size,

            # Free blocks in filesystem.
            'f_bfree': free,

            # Free blocks avail to non-superuser.
            'f_bavail': free

            # Total file nodes in filesystem.
#            'f_files': 0,

            # Free file nodes in filesystem.
#            'f_ffree': 0,

            # Free inodes for unprivileged users.
#            'f_favail': 0
        }

    @dec_hint(['filepath_old', 'filepath_new'])
    def rename(self, filepath_old, filepath_new):

        self.__log.debug("Renaming [%s] to [%s]." % 
                         (filepath_old, filepath_new))

        # Make sure the old filepath exists.
        (entry, path, filename_old) = self.__get_entry_or_raise(filepath_old)

        # At this point, decorations, the is-hidden prefix, etc.. haven't been
        # stripped.
        (path, filename_new_raw) = split(filepath_new)

        # Make sure the new filepath doesn't exist.

        try:
            self.__get_entry_or_raise(filepath_new, True)
        except GdNotFoundError:
            pass

        try:
            entry = drive_proxy('rename', normalized_entry=entry, 
                                new_filename=filename_new_raw)
        except:
            self.__log.exception("Could not update entry [%s] for rename." %
                                 (entry))
            raise FuseOSError(EIO)

        # Update our knowledge of the entry.

        path_relations = PathRelations.get_instance()

        try:
            path_relations.register_entry(entry)
        except:
            self.__log.exception("Could not register renamed entry: %s" % 
                                 (entry))
            raise FuseOSError(EIO)

    @dec_hint(['filepath', 'length', 'fh'])
    def truncate(self, filepath, length, fh=None):
        self.__log.debug("Truncating file-path [%s] with FH [%s]." % 
                         (filepath, fh))

        if fh is not None:
            self.__log.debug("Doing truncate by FH (%d)." % (fh))
        
            try:
                opened_file = OpenedManager.get_instance().get_by_fh(fh)
            except:
                self.__log.exception("Could not retrieve OpenedFile for handle "
                                     "with ID (%d) (truncate)." % (fh))
                raise FuseOSError(EIO)

            self.__log.debug("Truncating and clearing FH: %s" % (opened_file))

            opened_file.reset_state()

            entry_id = opened_file.entry_id
            cache = EntryCache.get_instance().cache

            try:
                entry = cache.get(entry_id)
            except:
                self.__log.exception("Could not fetch normalized entry with "
                                     "ID [%s] for truncate with FH." % 
                                     (entry_id))
                raise
        else:
            (entry, path, filename) = self.__get_entry_or_raise(filepath)

        self.__log.debug("Sending truncate request for [%s]." % (entry))

        try:
            entry = drive_proxy('truncate', normalized_entry=entry)
        except:
            self.__log.exception("Could not truncate entry [%s]." % (entry))
            raise FuseOSError(EIO)

        # We don't need to update our internal representation of the file (just 
        # our file-handle and its related buffering).

    @dec_hint(['file_path'])
    def unlink(self, file_path):
        """Remove a file."""
# TODO: Change to simply move to "trash". Have a FUSE option to elect this
# behavior.
        path_relations = PathRelations.get_instance()

        self.__log.debug("Removing file [%s]." % (file_path))

        try:
            entry_clause = path_relations.get_clause_from_path(file_path)
        except GdNotFoundError:
            self.__log.exception("Could not process [%s] (unlink).")
            raise FuseOSError(ENOENT)
        except:
            self.__log.exception("Could not get clause from file-path [%s] "
                                 "(unlink)." % (file_path))
            raise FuseOSError(EIO)

        if not entry_clause:
            self.__log.error("Path [%s] does not exist for unlink()." % 
                             (file_path))
            raise FuseOSError(ENOENT)

        entry_id = entry_clause[CLAUSE_ID]
        normalized_entry = entry_clause[CLAUSE_ENTRY]

        # Check if a directory.

        self.__log.debug("Ensuring it is a file (not a directory).")

        if normalized_entry.is_directory:
            self.__log.error("Can not unlink() directory [%s] with ID [%s]. "
                             "Must be file.", file_path, entry_id)
            raise FuseOSError(errno.EISDIR)

        self.__log.debug("Doing remove of directory [%s] with ID [%s]." % 
                         (file_path, entry_id))

        # Remove online. Complements local removal (if not found locally, a 
        # follow-up request checks online).

        try:
            drive_proxy('remove_entry', normalized_entry=normalized_entry)
        except (NameError):
            raise FuseOSError(ENOENT)
        except:
            self.__log.exception("Could not remove file [%s] with ID [%s]." % 
                                 (file_path, entry_id))
            raise FuseOSError(EIO)

        # Remove from cache. Will no longer be able to be found, locally.

        self.__log.debug("Removing all trace of entry [%s] from cache "
                         "(unlink)." % (normalized_entry))

        try:
            PathRelations.get_instance().remove_entry_all(entry_id)
        except:
            self.__log.exception("There was a problem removing entry [%s] "
                                 "from the caches." % (normalized_entry))
            raise

        # Remove from among opened-files.

        self.__log.debug("Removing all opened-files for [%s]." % (file_path))

        try:
            opened_file = OpenedManager.get_instance().\
                            remove_by_filepath(file_path)
        except:
            self.__log.exception("There was an error while removing all "
                                 "opened-file instances for file [%s] "
                                 "(remove)." % (file_path))
            raise FuseOSError(EIO)

        self.__log.debug("File removal complete.")

    @dec_hint(['raw_path', 'times'])
    def utimens(self, raw_path, times=None):
        """Set the file times."""

        if times is not None:
            (atime, mtime) = times
        else:
            now = time()
            (atime, mtime) = (now, now)

        (entry, path, filename) = self.__get_entry_or_raise(raw_path)

        mtime_phrase = get_flat_normal_fs_time_from_epoch(mtime)
        atime_phrase = get_flat_normal_fs_time_from_epoch(atime)

        self.__log.debug("Updating entry [%s] with m-time [%s] and a-time "
                         "[%s]." % (entry, mtime_phrase, atime_phrase))

        try:
            entry = drive_proxy('update_entry', normalized_entry=entry, 
                                modified_datetime=mtime_phrase,
                                accessed_datetime=atime_phrase)
        except:
            self.__log.exception("Could not update entry [%s] for times." %
                                 (entry))
            raise FuseOSError(EIO)

        self.__log.debug("Entry [%s] mtime is now [%s] and atime is now "
                         "[%s]." % (entry, entry.modified_date, 
                                    entry.atime_byme_date))

        return 0

    @dec_hint(['path'])
    def init(self, path):
        """Called on filesystem mount. Path is always /."""

        get_change_manager().mount_init()

    @dec_hint(['path'])
    def destroy(self, path):
        """Called on filesystem destruction. Path is always /."""

        get_change_manager().mount_destroy()

    @dec_hint(['path'])
    def listxattr(self, raw_path):
        (entry, path, filename) = self.__get_entry_or_raise(raw_path)

        return entry.xattr_data.keys()

    @dec_hint(['path', 'name', 'position'])
    def getxattr(self, raw_path, name, position=0):
        (entry, path, filename) = self.__get_entry_or_raise(raw_path)

        try:
            return entry.xattr_data[name] + "\n"
        except:
            return ''
        
def load_mount_parser_args(parser):
    parser.add_argument('auth_storage_file', help='Authorization storage file')
    parser.add_argument('mountpoint', help='Mount point')
    parser.add_argument('-d', '--debug', help='Debug mode',
                        action='store_true', required=False)
    parser.add_argument('-o', '--opt', help='Mount options',
                        action='store', required=False,
                        nargs=1)

def mount(auth_storage_filepath, mountpoint, debug=None, nothreads=None, 
          option_string=None):

    fuse_opts = { }
    if option_string:
        for opt_parts in [opt.split('=', 1) \
                          for opt \
                          in option_string.split(',') ]:
            k = opt_parts[0]

            # We need to present a bool type for on/off flags. Since all we
            # have are strings, we'll convert anything with a 'True' or 'False'
            # to a bool, or anything with just a key to True.
            if len(opt_parts) == 2:
                v = opt_parts[1]
                v_lower = v.lower()

                if v_lower == 'true':
                    v = True
                elif v_lower == 'false':
                    v = False
            else:
                v = True

            # We have a list of provided options. See which match against our 
            # application options.

            logging.info("Setting option [%s] to [%s]." % (k, v))

            try:
                Conf.set(k, v)
            except (KeyError) as e:
                logging.debug("Forwarding option [%s] with value [%s] to "
                              "FUSE." % (k, v))

                fuse_opts[k] = v
            except:
                logging.exception("Could not set option [%s]. It is probably "
                                  "invalid." % (k))
                raise

    logging.debug("PERMS: F=%s E=%s NE=%s" % 
                  (Conf.get('default_perm_folder'), 
                   Conf.get('default_perm_file_editable'), 
                   Conf.get('default_perm_file_noneditable')))

    # Assume that any option that wasn't an application option is a FUSE 
    # option. The Python-FUSE interface that we're using is beautiful/elegant,
    # but there's no help support. The user is just going to have to know the
    # options.

    set_auth_cache_filepath(auth_storage_filepath)

    # How we'll appear in diskfree, mtab, etc..
    name = ("gdfs(%s)" % (auth_storage_filepath))

    # Don't start any of the scheduled tasks, such as change checking, cache
    # cleaning, etc. It will minimize outside influence of the logs and state
    # to make it easier to debug.

#    atexit.register(Timers.get_instance().cancel_all)
    if debug:
        Timers.get_instance().set_autostart_default(False)

    fuse = FUSE(GDriveFS(), mountpoint, debug=debug, foreground=debug, 
                nothreads=nothreads, fsname=name, **fuse_opts)

def set_auth_cache_filepath(auth_storage_filepath):
    Conf.set('auth_cache_filepath', auth_storage_filepath)


