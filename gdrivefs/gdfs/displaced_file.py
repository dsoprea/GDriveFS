import logging
import json

from os import makedirs
from os.path import isdir

from gdrivefs.gdtool.drive import drive_proxy
from gdrivefs.gdtool.normal_entry import NormalEntry
from gdrivefs.conf import Conf

temp_path = ("%s/displaced" % (Conf.get('file_download_temp_path')))
if isdir(temp_path) is False:
    makedirs(temp_path)


class DisplacedFile(object):
    __log = None
    normalized_entry = None
    file_size = 1000

    def __init__(self, normalized_entry):
        self.__log = logging.getLogger().getChild('DisFile')
    
        if normalized_entry.__class__ != NormalEntry:
            raise Exception("_DisplacedFile can not wrap a non-NormalEntry "
                            "object.")

        self.__normalized_entry = normalized_entry

    def deposit_file(self, mime_type):
        """Write the file to a temporary path, and present a stub (JSON) to the 
        user. This is the only way of getting files that don't have a 
        well-defined filesize without providing a type, ahead of time.
        """

        temp_path = Conf.get('file_download_temp_path')
        file_path = ("%s/displaced/%s.%s" % (temp_path, 
                                             self.__normalized_entry.title, 
                                             mime_type.replace('/', '+')))

        try:
            result = drive_proxy('download_to_local', 
                                 output_file_path=file_path, 
                                 normalized_entry=self.__normalized_entry,
                                 mime_type=mime_type)
            (length, cache_fault) = result
        except:
            self.__log.exception("Could not localize displaced file with "
                                 "entry having ID [%s]." % 
                                 (self.__normalized_entry.id))
            raise

        self.__log.debug("Displaced entry [%s] deposited to [%s] with length "
                         "(%d)." % 
                         (self.__normalized_entry, file_path, length)) 

        try:
            return self.get_stub(mime_type, length, file_path)
        except:
            self.__log.exception("Could not build stub for [%s]." % 
                                 (self.__normalized_entry))
            raise

    def get_stub(self, mime_type, file_size=0, file_path=None):
        """Return the content for an info ("stub") file."""

        if file_size == 0 and \
           self.__normalized_entry.requires_displaceable is False:
            file_size = self.__normalized_entry.file_size

        stub_data = {
                'EntryId':              self.__normalized_entry.id,
                'OriginalMimeType':     self.__normalized_entry.mime_type,
                'ExportTypes':          self.__normalized_entry.download_types,
                'Title':                self.__normalized_entry.title,
                'Labels':               self.__normalized_entry.labels,
                'FinalMimeType':        mime_type,
                'Length':               file_size,
                'RequiresMimeType':     self.__normalized_entry.requires_mimetype,
                'ImageMediaMetadata':   self.__normalized_entry.image_media_metadata
            }

        if file_path:
            stub_data['FilePath'] = file_path

        try:
            result = json.dumps(stub_data)
            padding = (' ' * (self.file_size - len(result) - 1))

            return ("%s%s\n" % (result, padding))
        except:
            self.__log.exception("Could not serialize stub-data.")
            raise

