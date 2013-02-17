#!/usr/bin/python

import stat
import logging
import dateutil.parser
import re
import json
import os
import atexit
import resource

from errno import *
from time import mktime, time
from fuse import FUSE, Operations, FuseOSError, c_statvfs, fuse_get_context #, LoggingMixIn
from sys import argv, exit, excepthook

from gdrivefs.utility import get_utility
from gdrivefs.change import get_change_manager
from gdrivefs.timer import Timers
from gdrivefs.cache.volume import PathRelations, EntryCache, \
                                  CLAUSE_ENTRY, CLAUSE_PARENT, \
                                  CLAUSE_CHILDREN, CLAUSE_ID, \
                                  CLAUSE_CHILDREN_LOADED
from gdrivefs.conf import Conf
from gdrivefs.utility import dec_hint
from gdrivefs.gdtool.oauth_authorize import get_auth
from gdrivefs.gdtool.drive import drive_proxy
from gdrivefs.gdtool.account_info import AccountInfo
from gdrivefs.general.buffer_segments import BufferSegments
from gdrivefs.gdfs.displaced_file import DisplacedFile
from gdrivefs.gdfs.opened_file import OpenedManager, OpenedFile
from gdrivefs.gdfs.fsutility import strip_export_type, split_path
from gdrivefs.cache.volume import path_resolver
from gdrivefs.errors import GdNotFoundError


_static_log = logging.getLogger().getChild('(GDFS)')


# TODO: make sure strip_extension and split_path are used when each are relevant
# TODO: make sure create path reserves a file-handle, uploads the data, and then registers the open-file with the file-handle.
# TODO: make sure to finish the opened-file helper factory.


class GDriveFS(Operations):#LoggingMixIn,
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

    @dec_hint(['raw_path', 'fh'])
    def getattr(self, raw_path, fh=None):
        """Return a stat() structure."""
# TODO: Implement handle.

        try:
            (path, extension, just_info, mime_type) = strip_export_type \
                                                        (raw_path, True)
        except:
            self.__log.exception("Could not process export-type directives.")
            raise FuseOSError(EIO)

        path_relations = PathRelations.get_instance()

        try:
            entry_clause = path_relations.get_clause_from_path(path)
        except GdNotFoundError:
            self.__log.exception("Could not process [%s] (getattr).")
            raise FuseOSError(ENOENT)
        except:
            self.__log.exception("Could not try to get clause from path [%s] "
                              "(getattr)." % (path))
            raise FuseOSError(EIO)

        if not entry_clause:
            self.__log.debug("Path [%s] does not exist for stat()." % (path))
            raise FuseOSError(ENOENT)

        effective_permission = 0o444
        normalized_entry = entry_clause[0]

        entry = entry_clause[0]

        # If the user has required info, we'll treat folders like files so that 
        # we can return the info.
        is_folder = get_utility().is_directory(entry) and not just_info

        if entry.editable:
            effective_permission |= 0o222

        stat_result = { "st_mtime": entry.modified_date_epoch }
        
        
        if is_folder or entry.requires_displaceable:
            stat_result["st_size"] = DisplacedFile(entry).file_size
        else:
            stat_result["st_size"] = entry.file_size

        if is_folder:
            effective_permission |= 0o111
            stat_result["st_mode"] = (stat.S_IFDIR | effective_permission)

            stat_result["st_nlink"] = 2
        else:
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
            filenames = path_relations.get_child_filenames_from_entry_id \
                            (entry_clause[3])
        except:
            self.__log.exception("Could not render list of filenames under path "
                             "[%s]." % (path))
            raise FuseOSError(EIO)

        filenames[0:0] = ['.','..']

        for filename in filenames:
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
            (parent_clause, path, filename, extension, mime_type, is_hidden, \
             just_info) = split_path(filepath, path_resolver)
        except GdNotFoundError:
            self.__log.exception("Could not process [%s] (mkdir).")
            raise FuseOSError(ENOENT)
        except:
            self.__log.exception("Could not split path [%s] (mkdir)." % 
                              (filepath))
            raise FuseOSError(EIO)

        self.__log.debug("Creating directory [%s] under [%s]." % (filename, path))

        try:
            entry = drive_proxy('create_directory', filename=filename, 
                        parents=[parent_clause[0].id], is_hidden=is_hidden)
        except:
            self.__log.exception("Could not create directory with name [%s] and "
                              "parent with ID [%s]." % (filename, 
                                                        parent_clause[0].id))
            raise FuseOSError(EIO)

        self.__log.info("Directory [%s] created as ID [%s]." % (filepath, 
                     entry.id))

        #parent_clause[4] = False

        path_relations = PathRelations.get_instance()

        try:
            path_relations.register_entry(entry)
        except:
            self.__log.exception("Could not register new directory in cache.")
            raise FuseOSError(EIO)

    @dec_hint(['filepath', 'mode'])
    def create(self, filepath, mode):
        """Create a new file. This always precedes a write.
        
        We don't implement "mode" (permissions) because the model doesn't agree 
        with GD.
        """
# TODO: Fail if it already exists.

        self.__log.debug("Splitting file-path [%s] for create." % (filepath))

        try:
            (parent_clause, path, filename, extension, mime_type, is_hidden, \
             just_info) = split_path(filepath, path_resolver)
        except GdNotFoundError:
            self.__log.exception("Could not process [%s] (create).")
            raise FuseOSError(ENOENT)
        except:
            self.__log.exception("Could not split path [%s] (create)." % 
                              (filepath))
            raise FuseOSError(EIO)

        self.__log.debug("Acquiring file-handle.")

        try:
            fh = OpenedManager.get_instance().get_new_handle()
        except:
            self.__log.exception("Could not acquire file-handle for create of "
                              "[%s]." % (filepath))
            raise FuseOSError(EIO)

        self.__log.debug("Creating empty file [%s] under parent with ID [%s]." % 
                      (filename, parent_clause[3]))

        try:
            entry = drive_proxy('create_file', filename=filename, 
                                data_filepath='/dev/null', 
                                parents=[parent_clause[3]], 
                                is_hidden=is_hidden)
        except:
            self.__log.exception("Could not create empty file [%s] under parent "
                              "with ID [%s]." % (filename, parent_clause[3]))
            raise FuseOSError(EIO)

        self.__log.debug("Registering created file in cache.")

        path_relations = PathRelations.get_instance()

        try:
            path_relations.register_entry(entry)
        except:
            self.__log.exception("Could not register created file in cache.")
            raise FuseOSError(EIO)

        self.__log.debug("Building OpenedFile object for created file.")

        try:
            opened_file = OpenedFile(entry.id, path, filename, is_hidden, 
                                     mime_type)
        except:
            self.__log.exception("Could not create OpenedFile object for "
                              "created file.")
            raise FuseOSError(EIO)

        self.__log.debug("Registering OpenedFile object with handle (%d), "
                         "path [%s], and ID [%s]." % (fh, filepath, entry.id))

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
# TODO: Fail if does not exist and the mode is read only.

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

        raise FuseOSError(EPERM)

    # Not supported. Google Drive doesn't fit within this model.
    @dec_hint(['filepath', 'uid', 'gid'])
    def chown(self, filepath, uid, gid):

        raise FuseOSError(EPERM)

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
        """Return filesystem metrics.

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
            'f_blocks': used,

            # Fragment size.
#            'f_frsize': block_size,

            # Free blocks in filesystem.
#            'f_bfree': free,

            # Free blocks avail to non-superuser.
            'f_bavail': free

            # Total file nodes in filesystem.
#            'f_files': 0,

            # Free file nodes in filesystem.
#            'f_ffree': 0,

            # Free inodes for unprivileged users.
#            'f_favail': 0
        }

# TODO: !! Finish this.
    @dec_hint(['old', 'new'])
    def rename(self, old, new):
        pass

# TODO: !! Finish this.
    @dec_hint(['path', 'length', 'fh'])
    def truncate(self, path, length, fh=None):
        pass

    @dec_hint(['filepath'])
    def unlink(self, filepath):
        """Remove a file."""

        path_relations = PathRelations.get_instance()

        self.__log.debug("Removing file [%s]." % (filepath))

        try:
            entry_clause = path_relations.get_clause_from_path(filepath)
        except GdNotFoundError:
            self.__log.exception("Could not process [%s] (unlink).")
            raise FuseOSError(ENOENT)
        except:
            self.__log.exception("Could not get clause from file-path [%s] "
                              "(unlink)." % (filepath))
            raise FuseOSError(EIO)

        if not entry_clause:
            self.__log.error("Path [%s] does not exist for unlink()." % (filepath))
            raise FuseOSError(ENOENT)

        entry_id = entry_clause[CLAUSE_ID]
        normalized_entry = entry_clause[CLAUSE_ENTRY]

        # Check if a directory.

        self.__log.debug("Ensuring it is a file (not a directory).")

        if normalized_entry.is_directory:
            self.__log.error("Can not unlink() directory [%s] with ID [%s]. Must be file.", filepath, entry_id)
            raise FuseOSError(errno.EISDIR)

        self.__log.debug("Doing remove of directory [%s] with ID [%s]." % 
                      (filepath, entry_id))

        try:
            drive_proxy('remove_entry', normalized_entry=normalized_entry)
        except (NameError):
            raise FuseOSError(ENOENT)
        except:
            self.__log.exception("Could not remove file [%s] with ID [%s]." % 
                              (filepath, entry_id))
            raise FuseOSError(EIO)

# TODO: Remove from cache.

        self.__log.debug("File removal complete.")

# TODO: Finish this.
    @dec_hint(['path', 'times'])
    def utimens(self, path, times=None):
        """Set the file times."""

        pass
#        now = time()
#        atime, mtime = times if times else (now, now)

    @dec_hint(['path'])
    def init(self, path):
        """Called on filesystem mount. Path is always /."""

        get_change_manager().mount_init()

    @dec_hint(['path'])
    def destroy(self, path):
        """Called on filesystem destruction. Path is always /."""

        get_change_manager().mount_destroy()

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

                if v == 'True':
                    v = True
                elif v == 'False':
                    v = False
            else:
                v = True

            # We have a list of provided options. See which match against our 
            # application options.

            logging.info("Setting option [%s] to [%s]." % (k, v))

            try:
                Conf.set(k, v)
            except (KeyError) as e:
                fuse_opts[k] = v
            except:
                logging.exception("Could not set option [%s]. It is probably "
                                  "invalid." % (k))
                raise

    # Assume that any option that wasn't an application option is a FUSE 
    # option. The Python-FUSE interface that we're using is beautiful/elegant,
    # but there's no help support. The user is just going to have to know the
    # options.

    set_auth_cache_filepath(auth_storage_filepath)

    # How we'll appear in diskfree, mtab, etc..
    name = ("gdfs(%s)" % (auth_storage_filepath))

    fuse = FUSE(GDriveFS(), mountpoint, debug=False, foreground=debug, 
                nothreads=nothreads, fsname=name, **fuse_opts)

def set_auth_cache_filepath(auth_storage_filepath):
    Conf.set('auth_cache_filepath', auth_storage_filepath)

atexit.register(Timers.get_instance().cancel_all)


