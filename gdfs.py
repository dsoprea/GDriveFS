#!/usr/bin/python

import stat
import logging
import dateutil.parser
import getpass
import os
import errno

from time       import mktime
from argparse   import ArgumentParser
from fuse       import FUSE, Operations
from sys        import argv

from utility import get_utility
from gdrivefs.cache import PathRelations
from gdtool import drive_proxy

#if not hasattr(fuse, '__version__'):
#    raise RuntimeError, \
#        "Your fuse-py doesn't know of fuse.__version__, probably it's too old."
#
#fuse.fuse_python_api = (0, 2)

app_name = 'GDriveFS Tool'

#class _GDriveStat(fuse.Stat):
#    """A skeleton stat() structure."""
#
#    def __init__(self):
#        self.st_mode = 0
#        self.st_ino = 0
#        self.st_dev = 0
#        self.st_nlink = 0
#        self.st_uid = 0
#        self.st_gid = 0
#        self.st_size = 0
#        self.st_atime = 0
#        self.st_mtime = 0
#        self.st_ctime = 0

class _GDriveFS(Operations):
    """The main filesystem class."""

    def getattr(self, path, fh=None):
        """Return a stat() structure."""

        logging.info("Stat() on [%s]." % (path))

        path_relations = PathRelations.get_instance()

        try:
            entry_clause = path_relations.get_clause_from_path(path)
        except:
            logging.exception("Could not get clause from path [%s]." % (path))
            return -errno.ENOENT

        effective_permission = 0444

        logging.info("Got clause.")
        is_folder = get_utility().is_directory(entry_clause[0])
        entry = entry_clause[0]

        if entry.user_permission[u'role'] in [ u'owner', u'writer' ]:
            effective_permission |= 0222

        date_obj = dateutil.parser.parse(entry.modified_date)
        mtime_epoch = mktime(date_obj.timetuple())

        stat_result = { "st_mtime": mtime_epoch }

        if is_folder:
            effective_permission |= 0111
            stat_result["st_mode"] = (stat.S_IFDIR | effective_permission)

            stat_result["st_nlink"] = 2
        else:
            stat_result["st_mode"] = (stat.S_IFREG | effective_permission)
            stat_result["st_nlink"] = 1
            stat_result["st_size"] = int(entry.quota_bytes_used)

        return stat_result

    def readdir(self, path, offset):
        """A generator returning one base filename at a time."""

        logging.info("ReadDir(%s,%s) invoked." % (path, type(offset)))

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
            logging.exception("Could not get clause from path [%s]." % (path))
            raise

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

    def read(self, path, size, offset, fh):

        logging.info("Reading file at path [%s] with offset (%d) and count "
                     "(%d)." % (path, offset, size))

        path_relations = PathRelations.get_instance()

        # Figure out what entry represents the path.

        logging.debug("Deriving entry-clause from path.")

        try:
            entry_clause = path_relations.get_clause_from_path(path)
        except:
            logging.exception("Could not get clause from path [%s]." % (path))
            return -errno.ENOENT

        normalized_entry = entry_clause[0]
        entry_id = entry_clause[3]

        # TODO: mime_type needs to be derived, still.
        mime_type = mime_type

        # Fetch the file to a local, temporary file.

        logging.info("Downloading entry with ID [%s] for path [%s]." % 
                     (entry_id, path))

        try:
            temp_file_path = drive_proxy('download_to_local', 
                                     normalized_entry=normalized_entry,
                                     mime_type=mime_type)
        except:
            logging.exception("Could not localize file with entry having ID "
                              "[%s]." % (entry_id))
            raise

        # Retrieve the data.

        try:
            with open(temp_file_path, 'rb') as f:
                f.seek(offset)
                return f.read(size)
        except:
            logging.exception("Could not produce data from the temporary file-"
                              "path [%s]." % (temp_file_path))
            raise

    def destroy(self, path):
        """Called on filesystem destruction. Path is always /"""

        pass

def dump_changes(overview):
    (largest_change_id, next_page_token, changes) = overview

    for change_id, change in changes.iteritems():
        (file_id, was_deleted, entry) = change
        print("%s> %s" % (change_id, entry[u'title']))

#    print(changes)

def main():

    if len(argv) != 2:
        print 'usage: %s <mountpoint>' % argv[0]
        exit(1)

    fuse = FUSE(_GDriveFS(), argv[1], foreground=True, nothreads=True)

if __name__ == "__main__":
    main()

