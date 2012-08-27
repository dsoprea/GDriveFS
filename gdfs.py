#!/usr/bin/python

import fuse
import stat
import logging
import dateutil.parser
import getpass
import os

from time       import mktime
from argparse   import ArgumentParser

from utility import get_utility
from gdrivefs.cache import PathRelations

if not hasattr(fuse, '__version__'):
    raise RuntimeError, \
        "Your fuse-py doesn't know of fuse.__version__, probably it's too old."

fuse.fuse_python_api = (0, 2)

app_name = 'GDriveFS Tool'

class _GDriveStat(fuse.Stat):
    """A skeleton stat() structure."""

    def __init__(self):
        self.st_mode = 0
        self.st_ino = 0
        self.st_dev = 0
        self.st_nlink = 0
        self.st_uid = 0
        self.st_gid = 0
        self.st_size = 0
        self.st_atime = 0
        self.st_mtime = 0
        self.st_ctime = 0

class _GDriveFS(fuse.Fuse):
    """The main filesystem class."""

    def getattr(self, path):
        """Return a stat() structure."""

        logging.info("Stat() on [%s]." % (path))

        path_relations = PathRelations.get_instance()

        try:
            entry_clause = path_relations.get_clause_from_path(path)
        except:
            logger.exception("Could not get clause from path [%s]." % (path))
            return -errno.ENOENT
        logging.info("Got clause.")
        is_folder = get_utility().is_directory(entry_clause[0])
        entry = entry_clause[0]

        st = _GDriveStat()

        if is_folder:
            st.st_mode = stat.S_IFDIR | 0755
            st.st_nlink = 2

        else:
            st.st_mode = stat.S_IFREG | 0444
            st.st_nlink = 1
            st.st_size = int(entry.quota_bytes_used)

            date_obj = dateutil.parser.parse(entry.modified_date)
            mtime_epoch = mktime(date_obj.timetuple())
            st.st_mtime = mtime_epoch

#        else:
#            return -errno.ENOENT

        return st

    def readdir(self, path, offset):
        """A generator returning one base filename at a time."""

        logging.info("ReadDir(%s,%d) invoked." % (path, offset))

        # We expect "offset" to always be (0).
        if offset != 0:
            logging.warning("readdir() has been invoked for path [%s] and non-"
                            "zero offset (%d). This is not allowed." % 
                            (path, offset))

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
            yield fuse.Direntry(get_utility().translate_filename_charset(filename))

#    def fsinit(*args): 
#        import syslog
#        syslog.openlog('myfs') 
#        syslog.syslog("INIT") 
#        syslog.closelog() 
#
#    def destroy(self, path):
#        with open('/tmp/destroy', 'w') as f:
#            f.write("Content.")
#
#        import syslog
#        syslog.openlog('myfs2') 
#        syslog.syslog("DESTROY (2)") 
#        syslog.closelog()

def dump_changes(overview):
    (largest_change_id, next_page_token, changes) = overview

    for change_id, change in changes.iteritems():
        (file_id, was_deleted, entry) = change
        print("%s> %s" % (change_id, entry[u'title']))

#    print(changes)

def main():
    #change_overview = drive_proxy('list_changes')
    #dump_changes(change_overview)
    #files = drive_proxy('list_files')

    #return
    usage="""GDriveFS Fuser\n\n""" + fuse.Fuse.fusage
    server = _GDriveFS(version="%prog " + fuse.__version__,
                      usage=usage,
                      dash_s_do='setsingle')

#    server.parser.add_option(mountopt='ac', metavar="FILEPATH",
#                             help="API Credentials JSON file-path.")
#    server.parser.add_option('-a', '--ac', metavar="FILEPATH", 
#                             help="API Credentials JSON file-path.")

    fuse_args = server.parse(errex=1)

    server.main()

if __name__ == "__main__":
    main()

