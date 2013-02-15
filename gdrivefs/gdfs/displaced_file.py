import logging
import json

from gdrivefs.gdfs.fsutility import get_temp_filepath
from gdrivefs.gdtool.drive import drive_proxy
from gdrivefs.gdtool.normal_entry import NormalEntry

class DisplacedFile(object):
    __log = None
    normalized_entry = None
    file_size = 1000

    def __init__(self, normalized_entry):
        self.__log = logging.getLogger().getChild('DisFile')
    
        if normalized_entry.__class__ != NormalEntry:
            raise Exception("_DisplacedFile can not wrap a non-NormalEntry object.")

        self.normalized_entry = normalized_entry

    def deposit_file(self, mime_type=None):
        """Write the file to a temporary path, and present a stub (JSON) to the 
        user. This is the only way of getting files that don't have a 
        well-defined filesize without providing a type, ahead of time.
        """

        if not mime_type:
            mime_type = self.normalized_entry.normalized_mime_type

        file_path = get_temp_filepath(self.normalized_entry, True, mime_type)

        try:
            length = drive_proxy('download_to_local', 
                                 output_file_path=file_path, 
                                 normalized_entry=self.normalized_entry,
                                 mime_type=mime_type)
        except:
            self.__log.exception("Could not localize displaced file with entry "
                              "having ID [%s]." % (self.normalized_entry.id))
            raise

        try:
            return self.get_stub(mime_type, length, file_path)
        except:
            self.__log.exception("Could not build stub for [%s]." % 
                                 (self.normalized_entry))
            raise

    def get_stub(self, mime_type=None, file_size=0, file_path=None):
        """Return the content for an info ("stub") file."""

        if file_size == 0 and \
           self.normalized_entry.requires_displaceable is False:
            file_size = self.normalized_entry.file_size

        if not mime_type:
            mime_type = self.normalized_entry.normalized_mime_type

        stub_data = {
                'EntryId':              self.normalized_entry.id,
                'OriginalMimeType':     self.normalized_entry.mime_type,
                'ExportTypes':          self.normalized_entry.download_links.keys(),
                'Title':                self.normalized_entry.title,
                'Labels':               self.normalized_entry.labels,
                'FinalMimeType':        mime_type,
                'Length':               file_size,
                'Displaceable':         self.normalized_entry.requires_displaceable,
                'ImageMediaMetadata':   self.normalized_entry.image_media_metadata
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

