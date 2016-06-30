import logging
import stat
import dateutil.parser
import re
import json
import os
import atexit
import resource
import pprint
import math

from errno import ENOENT, EIO, ENOTDIR, ENOTEMPTY, EPERM, EEXIST
from fuse import FUSE, Operations, FuseOSError, c_statvfs, fuse_get_context, \
                 LoggingMixIn
from time import mktime, time
from sys import argv, exit, excepthook
from datetime import datetime
from os.path import split

import gdrivefs.gdfs.fsutility
import gdrivefs.gdfs.opened_file
import gdrivefs.config
import gdrivefs.config.changes
import gdrivefs.config.fs

from gdrivefs.utility import utility
from gdrivefs.change import get_change_manager
from gdrivefs.cache.volume import PathRelations, EntryCache, \
                                  CLAUSE_ENTRY, CLAUSE_PARENT, \
                                  CLAUSE_CHILDREN, CLAUSE_ID, \
                                  CLAUSE_CHILDREN_LOADED
from gdrivefs.conf import Conf
from gdrivefs.gdtool.drive import get_gdrive
from gdrivefs.gdtool.account_info import AccountInfo

from gdrivefs.gdfs.fsutility import strip_export_type, split_path,\
                                    build_filepath, dec_hint
from gdrivefs.gdfs.displaced_file import DisplacedFile
from gdrivefs.cache.volume import path_resolver
from gdrivefs.errors import GdNotFoundError
from gdrivefs.time_support import get_flat_normal_fs_time_from_epoch

_logger = logging.getLogger(__name__)

# TODO: make sure strip_extension and split_path are used when each are relevant
# TODO: make sure create path reserves a file-handle, uploads the data, and then registers the open-file with the file-handle.
# TODO: Make sure that we rely purely on the FH, whenever it is given, 
#       whereever it appears. This will be to accomodate system calls that can work either via file-path or file-handle.

def set_datetime_tz(datetime_obj, tz):
    return datetime_obj.replace(tzinfo=tz)

def get_entry_or_raise(raw_path, allow_normal_for_missing=False):
    try:
        result = split_path(raw_path, path_resolver)
        (parent_clause, path, filename, mime_type, is_hidden) = result
    except GdNotFoundError:
        _logger.exception("Could not retrieve clause for non-existent "
                          "file-path [%s] (parent does not exist)." % 
                          (raw_path))

        if allow_normal_for_missing is True:
            raise
        else:
            raise FuseOSError(ENOENT)
    except:
        _logger.exception("Could not process file-path [%s]." % 
                          (raw_path))
        raise FuseOSError(EIO)

    filepath = build_filepath(path, filename)
    path_relations = PathRelations.get_instance()

    try:
        entry_clause = path_relations.get_clause_from_path(filepath)
    except GdNotFoundError:
        _logger.exception("Could not retrieve clause for non-existent "
                          "file-path [%s] (parent exists)." % 
                          (filepath))

        if allow_normal_for_missing is True:
            raise
        else:
            raise FuseOSError(ENOENT)
    except:
        _logger.exception("Could not retrieve clause for path [%s]. " %
                          (filepath))
        raise FuseOSError(EIO)

    if not entry_clause:
        if allow_normal_for_missing is True:
            raise GdNotFoundError()
        else:
            raise FuseOSError(ENOENT)

    return (entry_clause[CLAUSE_ENTRY], path, filename)


class _GdfsMixin(object):
    """The main filesystem class."""

    def __register_open_file(self, fh, path, entry_id):
        with self.fh_lock:
            self.open_files[fh] = (entry_id, path)

    def __deregister_open_file(self, fh):
        with self.fh_lock:
            file_info = self.open_files[fh]

            del self.open_files[fh]
            return file_info

    def __get_open_file(self, fh):
        with self.fh_lock:
            return self.open_files[fh]

    def __build_stat_from_entry(self, entry):
        (uid, gid, pid) = fuse_get_context()

        block_size_b = gdrivefs.config.fs.CALCULATION_BLOCK_SIZE

        if entry.is_directory:
            effective_permission = int(Conf.get('default_perm_folder'), 
                                       8)
        elif entry.editable:
            effective_permission = int(Conf.get('default_perm_file_editable'), 
                                       8)
        else:
            effective_permission = int(Conf.get(
                                            'default_perm_file_noneditable'), 
                                       8)

        stat_result = { "st_mtime": entry.modified_date_epoch, # modified time.
                        "st_ctime": entry.modified_date_epoch, # changed time.
                        "st_atime": time(),
                        "st_uid":   uid,
                        "st_gid":   gid }
        
        if entry.is_directory:
            # Per http://sourceforge.net/apps/mediawiki/fuse/index.php?title=SimpleFilesystemHowto, 
            # default size should be 4K.
# TODO(dustin): Should we just make this (0), since that's what it is?
            stat_result["st_size"] = 1024 * 4
            stat_result["st_mode"] = (stat.S_IFDIR | effective_permission)
            stat_result["st_nlink"] = 2
        else:
            stat_result["st_size"] = DisplacedFile.file_size \
                                        if entry.requires_mimetype \
                                        else entry.file_size

            stat_result["st_mode"] = (stat.S_IFREG | effective_permission)
            stat_result["st_nlink"] = 1

        stat_result["st_blocks"] = \
            int(math.ceil(float(stat_result["st_size"]) / block_size_b))
  
        return stat_result

    @dec_hint(['raw_path', 'fh'])
    def getattr(self, raw_path, fh=None):
        """Return a stat() structure."""
# TODO: Implement handle.

        (entry, path, filename) = get_entry_or_raise(raw_path)
        return self.__build_stat_from_entry(entry)

    @dec_hint(['path', 'offset'])
    def readdir(self, path, offset):
        """A generator returning one base filename at a time."""

        # We expect "offset" to always be (0).
        if offset != 0:
            _logger.warning("readdir() has been invoked for path [%s] and "
                            "non-zero offset (%d). This is not allowed.",
                            path, offset)

# TODO: Once we start working on the cache, make sure we don't make this call, 
#       constantly.

        path_relations = PathRelations.get_instance()

        try:
            entry_clause = path_relations.get_clause_from_path(path)
        except GdNotFoundError:
            _logger.exception("Could not process [%s] (readdir).")
            raise FuseOSError(ENOENT)
        except:
            _logger.exception("Could not get clause from path [%s] "
                              "(readdir)." % (path))
            raise FuseOSError(EIO)

        if not entry_clause:
            raise FuseOSError(ENOENT)

        try:
            entry_tuples = path_relations.get_children_entries_from_entry_id \
                            (entry_clause[CLAUSE_ID])
        except:
            _logger.exception("Could not render list of filenames under path "
                              "[%s].", path)

            raise FuseOSError(EIO)

        yield utility.translate_filename_charset('.')
        yield utility.translate_filename_charset('..')

        for (filename, entry) in entry_tuples:

            # Decorate any file that -requires- a mime-type (all files can 
            # merely accept a mime-type)
            if entry.requires_mimetype:
                filename += utility.translate_filename_charset('#')
        
            yield (filename,
                   self.__build_stat_from_entry(entry),
                   0)

    @dec_hint(['raw_path', 'length', 'offset', 'fh'])
    def read(self, raw_path, length, offset, fh):

        om = gdrivefs.gdfs.opened_file.get_om()

        try:
            opened_file = om.get_by_fh(fh)
        except:
            _logger.exception("Could not retrieve OpenedFile for handle with"
                              "ID (%d) (read).", fh)

            raise FuseOSError(EIO)

        try:
            return opened_file.read(offset, length)
        except:
            _logger.exception("Could not read data.")
            raise FuseOSError(EIO)

    @dec_hint(['filepath', 'mode'])
    def mkdir(self, filepath, mode):
        """Create the given directory."""

# TODO: Implement the "mode".

        try:
            result = split_path(filepath, path_resolver)
            (parent_clause, path, filename, mime_type, is_hidden) = result
        except GdNotFoundError:
            _logger.exception("Could not process [%s] (mkdir).", filepath)
            raise FuseOSError(ENOENT)
        except:
            _logger.exception("Could not split path [%s] (mkdir).", filepath)
            raise FuseOSError(EIO)

        parent_id = parent_clause[CLAUSE_ID]
        gd = get_gdrive()

        try:
            entry = gd.create_directory(
                        filename, 
                        [parent_id], 
                        is_hidden=is_hidden)
        except:
            _logger.exception("Could not create directory with name [%s] "
                              "and parent with ID [%s].",
                              filename, parent_clause[0].id)
            raise FuseOSError(EIO)

        _logger.info("Directory [%s] created as ID [%s] under parent with "
                     "ID [%s].", filepath, entry.id, parent_id)

        #parent_clause[4] = False

        path_relations = PathRelations.get_instance()

        try:
            path_relations.register_entry(entry)
        except:
            _logger.exception("Could not register new directory in cache.")
            raise FuseOSError(EIO)

# TODO: Find a way to implement or enforce 'mode'.
    def __create(self, filepath, mode=None):
        """Create a new file.
                
        We don't implement "mode" (permissions) because the model doesn't agree 
        with GD.
        """

# TODO: Fail if it already exists.

        try:
            result = split_path(filepath, path_resolver)
            (parent_clause, path, filename, mime_type, is_hidden) = result
        except GdNotFoundError:
            _logger.exception("Could not process [%s] (i-create).", filepath)
            raise FuseOSError(ENOENT)
        except:
            _logger.exception("Could not split path [%s] (i-create).",
                              filepath)
            raise FuseOSError(EIO)

        if mime_type is None:
            _, ext = os.path.splitext(filename)
            if ext != '':
                ext = ext[1:]

            mime_type = utility.get_first_mime_type_by_extension(ext)

        distilled_filepath = build_filepath(path, filename)

        gd = get_gdrive()

        try:
            entry = gd.create_file(
                        filename, 
                        [parent_clause[3]], 
                        mime_type,
                        is_hidden=is_hidden)
        except:
            _logger.exception("Could not create empty file [%s] under "
                              "parent with ID [%s].",
                              filename, parent_clause[3])

            raise FuseOSError(EIO)

        path_relations = PathRelations.get_instance()

        try:
            path_relations.register_entry(entry)
        except:
            _logger.exception("Could not register created file in cache.")
            raise FuseOSError(EIO)

        _logger.info("Inner-create of [%s] completed.", distilled_filepath)

        return (entry, path, filename, mime_type)

    @dec_hint(['filepath', 'mode'])
    def create(self, raw_filepath, mode):
        """Create a new file. This always precedes a write."""

        om = gdrivefs.gdfs.opened_file.get_om()

        try:
            fh = om.get_new_handle()
        except:
            _logger.exception("Could not acquire file-handle for create of "
                              "[%s].", raw_filepath)

            raise FuseOSError(EIO)

        (entry, path, filename, mime_type) = self.__create(raw_filepath)

        try:
            opened_file = gdrivefs.gdfs.opened_file.OpenedFile(
                            entry.id, 
                            path, 
                            filename, 
                            not entry.is_visible, 
                            mime_type)
        except:
            _logger.exception("Could not create OpenedFile object for "
                              "created file.")

            raise FuseOSError(EIO)

        _logger.debug("Registering OpenedFile object with handle (%d), "
                      "path [%s], and ID [%s].", fh, raw_filepath, entry.id)

        om = gdrivefs.gdfs.opened_file.get_om()

        try:
            om.add(opened_file, fh=fh)
        except:
            _logger.exception("Could not register OpenedFile for created "
                              "file: [%s]", opened_file)

            raise FuseOSError(EIO)

        _logger.debug("File created, opened, and completely registered.")

        return fh

    @dec_hint(['filepath', 'flags'])
    def open(self, filepath, flags):
# TODO: Fail if does not exist and the mode/flags is read only.

        try:
            opened_file = gdrivefs.gdfs.opened_file.\
                            create_for_existing_filepath(filepath)
        except GdNotFoundError:
            _logger.exception("Could not create handle for requested [%s] "
                              "(open)." % (filepath))
            raise FuseOSError(ENOENT)
        except:
            _logger.exception("Could not create OpenedFile object for "
                                 "opened filepath [%s].", filepath)
            raise FuseOSError(EIO)

        om = gdrivefs.gdfs.opened_file.get_om()

        try:
            fh = om.add(opened_file)
        except:
            _logger.exception("Could not register OpenedFile for opened "
                              "file.")

            raise FuseOSError(EIO)

        _logger.debug("File opened.")

        return fh

    @dec_hint(['filepath', 'fh'])
    def release(self, filepath, fh):
        """Close a file."""

        om = gdrivefs.gdfs.opened_file.get_om()

        try:
            om.remove_by_fh(fh)
        except:
            _logger.exception("Could not remove OpenedFile for handle with "
                              "ID (%d) (release).", fh)

            raise FuseOSError(EIO)

    @dec_hint(['filepath', 'data', 'offset', 'fh'], ['data'])
    def write(self, filepath, data, offset, fh):
        om = gdrivefs.gdfs.opened_file.get_om()

        try:
            opened_file = om.get_by_fh(fh=fh)
        except:
            _logger.exception("Could not get OpenedFile (write).")
            raise FuseOSError(EIO)

        try:
            opened_file.add_update(offset, data)
        except:
            _logger.exception("Could not queue file-update.")
            raise FuseOSError(EIO)

        return len(data)

    @dec_hint(['filepath', 'fh'])
    def flush(self, filepath, fh):
        
        om = gdrivefs.gdfs.opened_file.get_om()

        try:
            opened_file = om.get_by_fh(fh=fh)
        except:
            _logger.exception("Could not get OpenedFile (flush).")
            raise FuseOSError(EIO)

        try:
            opened_file.flush()
        except:
            _logger.exception("Could not flush local updates.")
            raise FuseOSError(EIO)

    @dec_hint(['filepath'])
    def rmdir(self, filepath):
        """Remove a directory."""

        path_relations = PathRelations.get_instance()

        try:
            entry_clause = path_relations.get_clause_from_path(filepath)
        except GdNotFoundError:
            _logger.exception("Could not process [%s] (rmdir).", filepath)
            raise FuseOSError(ENOENT)
        except:
            _logger.exception("Could not get clause from file-path [%s] "
                              "(rmdir).", filepath)
            raise FuseOSError(EIO)

        if not entry_clause:
            _logger.error("Path [%s] does not exist for rmdir().", filepath)
            raise FuseOSError(ENOENT)

        entry_id = entry_clause[CLAUSE_ID]
        normalized_entry = entry_clause[CLAUSE_ENTRY]

        # Check if not a directory.

        if not normalized_entry.is_directory:
            _logger.error("Can not rmdir() non-directory [%s] with ID [%s].", 
                          filepath, entry_id)

            raise FuseOSError(ENOTDIR)

        # Ensure the folder is empty.

        gd = get_gdrive()

        try:
            found = gd.get_children_under_parent_id(
                        entry_id,
                        max_results=1)
        except:
            _logger.exception("Could not determine if directory to be removed "
                              "has children.", entry_id)

            raise FuseOSError(EIO)

        if found:
            raise FuseOSError(ENOTEMPTY)

        try:
            gd.remove_entry(normalized_entry)
        except (NameError):
            raise FuseOSError(ENOENT)
        except:
            _logger.exception("Could not remove directory [%s] with ID [%s].",
                              filepath, entry_id)

            raise FuseOSError(EIO)
# TODO: Remove from cache.

    # Not supported. Google Drive doesn't fit within this model.
    @dec_hint(['filepath', 'mode'])
    def chmod(self, filepath, mode):
        # Return successfully, or rsync might have a problem.
#        raise FuseOSError(EPERM) # Operation not permitted.
        pass

    # Not supported. Google Drive doesn't fit within this model.
    @dec_hint(['filepath', 'uid', 'gid'])
    def chown(self, filepath, uid, gid):
        # Return successfully, or rsync might have a problem.
#        raise FuseOSError(EPERM) # Operation not permitted.
        pass

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

        block_size_b = gdrivefs.config.fs.CALCULATION_BLOCK_SIZE

        try:
            account_info = AccountInfo.get_instance()
            total = account_info.quota_bytes_total / block_size_b
            used = account_info.quota_bytes_used / block_size_b
            free = total - used
        except:
            _logger.exception("Could not get account-info.")
            raise FuseOSError(EIO)

        return {
            # Optimal transfer block size.
            'f_bsize': block_size_b,

            # Total data blocks in file system.
            'f_blocks': total,

            # Fragment size.
            'f_frsize': block_size_b,

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
        # Make sure the old filepath exists.
        (entry, path, filename_old) = get_entry_or_raise(filepath_old)

        # At this point, decorations, the is-hidden prefix, etc.. haven't been
        # stripped.
        (path, filename_new_raw) = split(filepath_new)

        # Make sure the new filepath doesn't exist.

        try:
            get_entry_or_raise(filepath_new, True)
        except GdNotFoundError:
            pass

        gd = get_gdrive()

        try:
            entry = gd.rename(entry, filename_new_raw)
        except:
            _logger.exception("Could not update entry [%s] for rename.", entry)
            raise FuseOSError(EIO)

        # Update our knowledge of the entry.

        path_relations = PathRelations.get_instance()

        try:
            path_relations.register_entry(entry)
        except:
            _logger.exception("Could not register renamed entry: %s", entry)
            raise FuseOSError(EIO)

    @dec_hint(['filepath', 'length', 'fh'])
    def truncate(self, filepath, length, fh=None):
        if fh is not None:
            om = gdrivefs.gdfs.opened_file.get_om()

            try:
                opened_file = om.get_by_fh(fh)
            except:
                _logger.exception("Could not retrieve OpenedFile for handle "
                                  "with ID (%d) (truncate).", fh)

                raise FuseOSError(EIO)

            _logger.debug("Truncating and clearing FH: %s", opened_file)

            opened_file.reset_state()

            entry_id = opened_file.entry_id
            cache = EntryCache.get_instance().cache
            entry = cache.get(entry_id)

            opened_file.truncate(length)
        else:
            (entry, path, filename) = get_entry_or_raise(filepath)

        gd = get_gdrive()

        try:
            entry = gd.truncate(entry)
        except:
            _logger.exception("Could not truncate entry [%s].", entry)
            raise FuseOSError(EIO)

# TODO(dustin): It would be a lot quicker if we truncate our temporary file 
#               here, and make sure its mtime matches.

        # We don't need to update our internal representation of the file (just 
        # our file-handle and its related buffering).

    @dec_hint(['file_path'])
    def unlink(self, file_path):
        """Remove a file."""
# TODO: Change to simply move to "trash". Have a FUSE option to elect this
# behavior.
        path_relations = PathRelations.get_instance()

        try:
            entry_clause = path_relations.get_clause_from_path(file_path)
        except GdNotFoundError:
            _logger.exception("Could not process [%s] (unlink).", file_path)
            raise FuseOSError(ENOENT)
        except:
            _logger.exception("Could not get clause from file-path [%s] "
                              "(unlink).", file_path)

            raise FuseOSError(EIO)

        if not entry_clause:
            _logger.error("Path [%s] does not exist for unlink().",
                          file_path)

            raise FuseOSError(ENOENT)

        entry_id = entry_clause[CLAUSE_ID]
        normalized_entry = entry_clause[CLAUSE_ENTRY]

        # Check if a directory.

        if normalized_entry.is_directory:
            _logger.error("Can not unlink() directory [%s] with ID [%s]. "
                          "Must be file.", file_path, entry_id)

            raise FuseOSError(errno.EISDIR)

        # Remove online. Complements local removal (if not found locally, a 
        # follow-up request checks online).

        gd = get_gdrive()

        try:
            gd.remove_entry(normalized_entry)
        except NameError:
            raise FuseOSError(ENOENT)
        except:
            _logger.exception("Could not remove file [%s] with ID [%s].",
                              file_path, entry_id)

            raise FuseOSError(EIO)

        # Remove from cache. Will no longer be able to be found, locally.
        PathRelations.get_instance().remove_entry_all(entry_id)

        # Remove from among opened-files.

        om = gdrivefs.gdfs.opened_file.get_om()

        try:
            opened_file = om.remove_by_filepath(file_path)
        except:
            _logger.exception("There was an error while removing all "
                                 "opened-file instances for file [%s] "
                                 "(remove).", file_path)
            raise FuseOSError(EIO)

    @dec_hint(['raw_path', 'times'])
    def utimens(self, raw_path, times=None):
        """Set the file times."""

        if times is not None:
            (atime, mtime) = times
        else:
            now = time()
            (atime, mtime) = (now, now)

        (entry, path, filename) = get_entry_or_raise(raw_path)

        mtime_phrase = get_flat_normal_fs_time_from_epoch(mtime)
        atime_phrase = get_flat_normal_fs_time_from_epoch(atime)

        gd = get_gdrive()

        try:
            entry = gd.update_entry(
                        entry, 
                        modified_datetime=mtime_phrase,
                        accessed_datetime=atime_phrase)
        except:
            _logger.exception("Could not update entry [%s] for times.",
                              entry)

            raise FuseOSError(EIO)

        return 0

    @dec_hint(['path'])
    def init(self, path):
        """Called on filesystem mount. Path is always /."""

        if gdrivefs.config.changes.MONITOR_CHANGES is True:
            _logger.info("Activating change-monitor.")
            get_change_manager().mount_init()
        else:
            _logger.warning("We were told not to monitor changes.")

    @dec_hint(['path'])
    def destroy(self, path):
        """Called on filesystem destruction. Path is always /."""

        if gdrivefs.config.changes.MONITOR_CHANGES is True:
            _logger.info("Stopping change-monitor.")
            get_change_manager().mount_destroy()

    @dec_hint(['path'])
    def listxattr(self, raw_path):
        (entry, path, filename) = get_entry_or_raise(raw_path)

        return entry.xattr_data.keys()

    @dec_hint(['path', 'name', 'position'])
    def getxattr(self, raw_path, name, position=0):
        (entry, path, filename) = get_entry_or_raise(raw_path)

        try:
            return entry.xattr_data[name] + "\n"
        except:
            return ''

if gdrivefs.config.DO_LOG_FUSE_MESSAGES is True:
    class GDriveFS(_GdfsMixin, LoggingMixIn, Operations):
        pass
else:
    class GDriveFS(_GdfsMixin, Operations):
        pass

def mount(auth_storage_filepath, mountpoint, debug=None, nothreads=None, 
          option_string=None):

    if os.path.exists(auth_storage_filepath) is False:
        raise ValueError("Credential path is not valid: [%s]" %
                         (auth_storage_filepath,))

    fuse_opts = {}
    
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

            _logger.debug("Setting option [%s] to [%s].", k, v)

            try:
                Conf.set(k, v)
            except KeyError as e:
                _logger.debug("Forwarding option [%s] with value [%s] to "
                              "FUSE.", k, v)

                fuse_opts[k] = v

    if gdrivefs.config.IS_DEBUG is True:
        _logger.debug("FUSE options:\n%s", pprint.pformat(fuse_opts))

    _logger.debug("PERMS: F=%s E=%s NE=%s",
                  Conf.get('default_perm_folder'), 
                  Conf.get('default_perm_file_editable'), 
                  Conf.get('default_perm_file_noneditable'))

    # Assume that any option that wasn't an application option is a FUSE 
    # option. The Python-FUSE interface that we're using is beautiful/elegant,
    # but there's no help support. The user is just going to have to know the
    # options.

    set_auth_cache_filepath(auth_storage_filepath)

    # How we'll appear in diskfree, mtab, etc..
    name = ("gdfs(%s)" % (auth_storage_filepath,))

    # Make sure we can connect.
    gdrivefs.gdtool.account_info.AccountInfo().get_data()

    fuse = FUSE(
            GDriveFS(), 
            mountpoint, 
            debug=debug, 
            foreground=debug, 
            nothreads=nothreads, 
            fsname=name, 
            **fuse_opts)

def set_auth_cache_filepath(auth_storage_filepath):
    auth_storage_filepath = os.path.abspath(auth_storage_filepath)

    Conf.set('auth_cache_filepath', auth_storage_filepath)
