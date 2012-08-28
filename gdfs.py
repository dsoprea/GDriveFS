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

        logging.info("Got clause.")
        is_folder = get_utility().is_directory(entry_clause[0])
        entry = entry_clause[0]

#        st = _GDriveStat()

#        return dict(st_mode=(S_IFDIR | 0755), st_ctime=now,
#            st_mtime=now, st_atime=now, st_nlink=2)


        if is_folder:
            return {
                    "st_mode": (stat.S_IFDIR | 0755),
                    "st_nlink": 2
                }
        else:
            date_obj = dateutil.parser.parse(entry.modified_date)
            mtime_epoch = mktime(date_obj.timetuple())

            return {
                    "st_mode": (stat.S_IFREG | 0444),
                    "st_nlink": 1,
                    "st_size": int(entry.quota_bytes_used),
                    "st_mtime": mtime_epoch
                }

    def readdir(self, path, fh):
        """A generator returning one base filename at a time."""

        logging.info("ReadDir(%s,%s) invoked." % (path, type(fh)))

        # We expect "offset" to always be (0).
#        if offset != 0:
#            logging.warning("readdir() has been invoked for path [%s] and non-"
#                            "zero offset (%d). This is not allowed." % 
#                            (path, offset))

# TODO: Return -ENOENT if not found?
# TODO: Once we start working on the cache, make sure we don't make this call, 
#       constantly.

        path_relations = PathRelations.get_instance()

        logging.debug("Listing files.")

        try:
            entry_clause = path_relations.get_clause_from_path(path)
        except:
            logger.exception("Could not get clause from path [%s]." % (path))
            raise

        try:
            filenames = path_relations.get_child_filenames_from_entry_id \
                            (entry_clause[3])
        except:
            logger.exception("Could not render list of filenames under path "
                             "[%s]." % (path))
            raise

        filenames[0:0] = ['.','..']
        logging.info(filenames)
        for filename in filenames:
            yield filename

#    def fsinit(*args): 
#        import syslog
#        syslog.openlog('myfs') 
#        syslog.syslog("INIT") 
#        syslog.closelog() 
#
#    def __del__(self, *args):
#        with open('/tmp/destroy', 'w') as f:
#            f.write("Content.")
#
#    def fsdestroy(self, *args):
#        with open('/tmp/destroy', 'w') as f:
#            f.write("Content.")
    def destroy(self, path):
        """Called on filesystem destruction. Path is always /"""
        logging.info("FS destroyed.")

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

