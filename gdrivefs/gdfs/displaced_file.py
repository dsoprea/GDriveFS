import logging
import json
import tempfile
import os

from os import makedirs
from os.path import isdir

from gdrivefs.gdtool.drive import get_gdrive
from gdrivefs.gdtool.normal_entry import NormalEntry
from gdrivefs.conf import Conf

_logger = logging.getLogger(__name__)


class DisplacedFile(object):
    normalized_entry = None
    file_size = 1000

    def __init__(self, normalized_entry):
        assert issubclass(normalized_entry.__class__, NormalEntry) is True, \
               "DisplacedFile can not wrap a non-NormalEntry object."

        self.__normalized_entry = normalized_entry
        self.__filepath = tempfile.NamedTemporaryFile(delete=False)

    def __del__(self):
        os.unlink(self.__filepath)

    def deposit_file(self, mime_type):
        """Write the file to a temporary path, and present a stub (JSON) to the 
        user. This is the only way of getting files that don't have a 
        well-defined filesize without providing a type, ahead of time.
        """

        gd = get_gdrive()

        result = gd.download_to_local(
                    self.__filepath, 
                    self.__normalized_entry,
                    mime_type)

        (length, cache_fault) = result

        _logger.debug("Displaced entry [%s] deposited to [%s] with length "
                      "(%d).", self.__normalized_entry, self.__filepath, length)

        return self.get_stub(mime_type, length, self.__filepath)

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

            result = json.dumps(stub_data)
            padding = (' ' * (self.file_size - len(result) - 1))

            return ("%s%s\n" % (result, padding))
