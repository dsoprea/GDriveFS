#!/usr/bin/python

import stat
import logging
import dateutil.parser
import getpass
import errno
import re
import json

from errno      import *
from time       import mktime
from argparse   import ArgumentParser
from fuse       import FUSE, Operations, LoggingMixIn, FuseOSError
from sys        import argv
from os         import getenv

from gdrivefs.utility import get_utility
from gdrivefs.cache import PathRelations, EntryCache
from gdrivefs.gdtool import drive_proxy, NormalEntry
from gdrivefs.errors import ExportFormatError

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

class _DisplacedFile(object):
    normalized_entry = None

    def __init__(self, normalized_entry):
        if normalized_entry.__class__ != NormalEntry:
            raise Exception("_DisplacedFile can not wrap a non-NormalEntry object.")

        self.normalized_entry = normalized_entry

    def get_listed_file_size(self):
        return 1000

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
                'EntryId':          self.normalized_entry.id,
                'OriginalMimeType': self.normalized_entry.mime_type,
                'ExportTypes':      self.normalized_entry.download_links.keys(),
                'Title':            self.normalized_entry.title,
                'Labels':           self.normalized_entry.labels,
                'FinalMimeType':    mime_type,
                'Length':           file_size,
                'Displaceable':     self.normalized_entry.requires_displaceable
            }

        if file_path:
            stub_data['FilePath'] = file_path

        try:
            result = json.dumps(stub_data)
            padding = (' ' * (self.get_listed_file_size() - len(result) - 1))

            return ("%s%s\n" % (result, padding))
        except:
            logging.exception("Could not serialize stub-data.")
            raise

class _GDriveFS(LoggingMixIn,Operations):
    """The main filesystem class."""

    def getattr(self, raw_path, fh=None):
        """Return a stat() structure."""

        logging.info("Stat() on [%s]." % (raw_path))

        try:
            (path, mime_type, extension, just_info) = self.__strip_export_type \
                                                        (raw_path)
        except:
            logging.exception("Could not process export-type directives.")
            raise

        path_relations = PathRelations.get_instance()

        try:
            entry_clause = path_relations.get_clause_from_path(path)
        except:
            logging.exception("Could not get clause from path [%s] "
                              "(getattr)." % (path))
            raise FuseOSError(ENOENT)

        effective_permission = 0444
        normalized_entry = entry_clause[0]

        entry = entry_clause[0]
        is_folder = get_utility().is_directory(entry)

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

            if entry.requires_displaceable:
                try:
                    displaced = _DisplacedFile(entry)
                except:
                    logging.exception("Could not wrap entry in _DisplacedFile.")
                    raise

                stat_result["st_size"] = displaced.get_listed_file_size()
            else:
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
            logging.exception("Could not get clause from path [%s] "
                              "(readdir)." % (path))
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

        rx = re.compile('(#([a-zA-Z0-9]+))?(\$)?$')
        matched = rx.search(path.encode('ASCII'))

        extension = None
        mime_type = None
        just_info = None

        if matched:
            fragment = matched.group(0)
            extension = matched.group(2)
            just_info = (matched.group(3) == '$')

            if extension:
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

            if fragment:
                path = path[:-len(fragment)]

        return (path, mime_type, extension, just_info)

    def read(self, raw_path, size, offset, fh):

        logging.info("Reading file at path [%s] with offset (%d) and count "
                     "(%d)." % (raw_path, offset, size))

        try:
            (path, mime_type, extension, just_info) = self.__strip_export_type \
                                                        (raw_path)
        except:
            logging.exception("Could not process export-type directives.")
            raise

        path_relations = PathRelations.get_instance()

        # Figure out what entry represents the path.

        logging.debug("Deriving entry-clause from path.")

        try:
            entry_clause = path_relations.get_clause_from_path(path)
        except:
            logging.exception("Could not get clause from path [%s] (read)." % 
                              (path))
            raise FuseOSError(ENOENT)

        normalized_entry = entry_clause[0]
        entry_id = entry_clause[3]

        if not mime_type:
            mime_type = normalized_entry.normalized_mime_type

        # Fetch the file to a local, temporary file.

        if normalized_entry.requires_displaceable or just_info:
            logging.info("Doing displaced-file download of entry with ID "
                         "[%s]." % (entry_id))

            try:
                displaced = _DisplacedFile(normalized_entry)
            except:
                logging.exception("Could not wrap entry in _DisplacedFile.")
                raise

            try:
                if just_info:
                    logging.debug("Info for file was requested, rather than "
                                  "the file itself.")
                    return displaced.get_stub(mime_type)
                else:
                    logging.debug("A displaceable file was requested.")
                    return displaced.deposit_file(mime_type)
            except:
                logging.exception("Could not do displaced-file download.")
                raise

        else:
            logging.info("Downloading entry with ID [%s] for path [%s]." % 
                         (entry_id, path))

            try:
                (temp_file_path, length) = \
                    drive_proxy('download_to_local', 
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

