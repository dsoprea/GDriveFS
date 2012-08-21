#!/usr/bin/python

from gdtool import AuthorizationError, AuthorizationFailureError, AuthorizationFaultError
from gdtool import drive_proxy, get_auth
from gdtool import get_cache

import fuse
import os
import stat
import errno
import sys
import logging
import dateutil.parser
import time

from fuse import Fuse
from argparse import ArgumentParser

logging.basicConfig(
        level       = logging.DEBUG, 
        format      = '%(asctime)s  %(levelname)s %(message)s',
        filename    = '/tmp/gdrivefs.log'
    )

app_name = 'GDriveFS'

if not hasattr(fuse, '__version__'):
    raise RuntimeError, \
        "Your fuse-py doesn't know of fuse.__version__, probably it's too old."

fuse.fuse_python_api = (0, 2)

# The path of the example file, relative to the mount directory. First 
# character is a slash.
hello_path = '/hello'
hello_str = 'Hello World!\n'

class GDriveStat(fuse.Stat):
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

class GDriveFS(Fuse):
    def getattr(self, path):
        """Return a stat structure."""

        st = GDriveStat()

        if path == '/':
            st.st_mode = stat.S_IFDIR | 0755
            st.st_nlink = 2

        else:#elif path == hello_path:
            entry = get_cache().get_entry_byfilepath(path)
# TODO: We need to access the wrapper instead of the cache so that it can get the information if it's not cached.

            date_obj = dateutil.parser.parse(entry[u'modifiedDate'])
            mtime_epoch = time.mktime(date_obj.timetuple())

            st.st_mode = stat.S_IFREG | 0444
            st.st_nlink = 1

            if 'quotaBytesUsed' in entry:
                st.st_size = int(entry['quotaBytesUsed'])
            else:
                st.st_size = 0
            
            st.st_mtime = mtime_epoch

#        else:
#            return -errno.ENOENT

        return st

    def readdir(self, path, offset):
        """A generator returning one base filename at a time."""

        try:
            files = drive_proxy('list_files')
        except:
            logging.exception("Could not get list of files.")
            raise

        filenames = ['.','..']
        #filenames.extend((entry_tuple[1] for entry_typle in files))
        #filenames.extend([entry_tuple[1] for entry_typle in files])
        for entry_tuple in files:
            filenames.append(entry_tuple[1])

        for filename in filenames:
            yield fuse.Direntry(filename)

def main():
    usage="""GDriveFS Fuser\n\n""" + Fuse.fusage
    server = GDriveFS(version="%prog " + fuse.__version__,
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

