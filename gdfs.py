#!/usr/bin/python

import stat
import logging
import dateutil.parser
import getpass
import errno
import re

from errno      import *
from time       import mktime
from argparse   import ArgumentParser
from fuse       import FUSE, Operations, LoggingMixIn, FuseOSError
from sys        import argv
from os         import getenv

from utility import get_utility
from gdrivefs.cache import PathRelations
from gdtool import drive_proxy
from errors import ExportFormatError


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

class _GDriveFS(LoggingMixIn,Operations):
    """The main filesystem class."""

    def getattr(self, raw_path, fh=None):
        """Return a stat() structure."""

        logging.info("Stat() on [%s]." % (raw_path))

        try:
            (path, mime_type, extension) = self.__strip_export_type(raw_path)
        except:
            logging.exception("Could not process export-type directives.")
            raise

        path_relations = PathRelations.get_instance()

        try:
            entry_clause = path_relations.get_clause_from_path(path)
        except:
            logging.exception("Could not get clause from path [%s] (getattr)." % (path))
            raise FuseOSError(ENOENT)

        effective_permission = 0444

        is_folder = get_utility().is_directory(entry_clause[0])
        entry = entry_clause[0]

        if entry.editable:
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
            stat_result["st_size"] = int(entry.file_size)

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
            logging.exception("Could not get clause from path [%s] (readdir)." % (path))
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

    def __strip_export_type(self, path):

        rx = re.compile('#([a-zA-Z0-9]+)$')
        matched = rx.search(path.encode('ASCII'))

        extension = None
        mime_type = None

        if matched:
            fragment = matched.group(0)
            extension = matched.group(1)

            logging.info("User wants to export to extension [%s]." % 
                         (extension))

            try:
                mime_type = get_utility().get_first_mime_type_by_extension \
                                (extension)
            except:
                logging.warning("Could not render a mime-type for prescribed"
                                "extension [%s], for read." % (extension))

            if mime_type:
                logging.info("We have been told to export using mime-type "
                             "[%s]." % (mime_type))

                path = path[:-len(fragment)]

        return (path, mime_type, extension)

    def read(self, raw_path, size, offset, fh):

        logging.info("Reading file at path [%s] with offset (%d) and count "
                     "(%d)." % (raw_path, offset, size))

        try:
            (path, mime_type, extension) = self.__strip_export_type(raw_path)
        except:
            logging.exception("Could not process export-type directives.")
            raise

        path_relations = PathRelations.get_instance()

        # Figure out what entry represents the path.

        logging.debug("Deriving entry-clause from path.")

        try:
            entry_clause = path_relations.get_clause_from_path(path)
        except:
            logging.exception("Could not get clause from path [%s] (read)." % (path))
            raise FuseOSError(ENOENT)

        normalized_entry = entry_clause[0]
        entry_id = entry_clause[3]

        if not mime_type:
            try:
                mime_type = get_utility().get_normalized_mime_type \
                                (normalized_entry)
            except:
                logging.exception("Could not render a mime-type for entry with"
                                  " ID [%s], for read." % (entry.id))
                raise

        # Fetch the file to a local, temporary file.

        logging.info("Downloading entry with ID [%s] for path [%s]." % 
                     (entry_id, path))

        try:
            temp_file_path = drive_proxy('download_to_local', 
                                     normalized_entry=normalized_entry,
                                     mime_type=mime_type)
        except (ExportFormatError):
            raise FuseOSError(ENOENT)
        except:
            logging.exception("Could not localize file with entry having ID "
                              "[%s]." % (entry_id))
            raise

        # Retrieve the data.

        try:
            with open(temp_file_path, 'rb') as f:
                f.seek(offset)
                buffer = f.read(size)
                
                logging.debug("(%d) bytes are being returned." % (len(buffer)))
                return buffer
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

