import json
import logging

from mimetypes import guess_extension

from conf import Conf

class _DriveUtility(object):
    """General utility functions loosely related to GD."""

    # Mime-types to translate to, if they appear within the "exportLinks" list.
    gd_to_normal_mime_mappings = {
            u'application/vnd.google-apps.document':        u'text/plain',
            u'application/vnd.google-apps.spreadsheet':     u'application/vnd.ms-excel',
            u'application/vnd.google-apps.presentation':    u'application/vnd.ms-powerpoint',
            u'application/vnd.google-apps.drawing':         u'application/pdf',
            u'application/vnd.google-apps.audio':           u'audio/mpeg',
            u'application/vnd.google-apps.photo':           u'image/png',
            u'application/vnd.google-apps.video':           u'video/x-flv'
        }

    # Default extensions for mime-types.
    default_extensions = { 
            u'text/plain':                      u'txt',
            u'application/vnd.ms-excel':        u'xls',
            u'application/vnd.ms-powerpoint':   u'ppt',
            u'application/pdf':                 u'pdf',
            u'audio/mpeg':                      u'mp3',
            u'image/png':                       u'png',
            u'video/x-flv':                     u'flv'
        }

    mimetype_folder = u"application/vnd.google-apps.folder"

    def __init__(self):
        self.__load_mappings()

    def __load_mappings(self):
        # Allow someone to override our default mappings of the GD types.

        gd_to_normal_mapping_filepath = \
            Conf.get('gd_to_normal_mapping_filepath')

        try:
            with open(gd_to_normal_mapping_filepath, 'r') as f:
                self.gd_to_normal_mime_mappings.extend(json.load(f))
        except:
            logging.info("No mime-mapping was found.")

        # Allow someone to set file-extensions for mime-types, and not rely on 
        # Python's educated guesses.

        extension_mapping_filepath = Conf.get('extension_mapping_filepath')

        try:
            with open(extension_mapping_filepath, 'r') as f:
                self.default_extensions.extend(json.load(f))
        except:
            logging.info("No extension-mapping was found.")

    def is_folder(self, entry):
        return (entry[u'mimeType'] == self.mimetype_folder)

    def get_extension(self, entry):
        """Return the filename extension that should be associated with this 
        file.
        """

        # A front-line defense against receiving the wrong kind of data.
        if u'id' not in entry:
            raise Exception("Entry is not a dictionary with a key named "
                            "'id'.")

        logging.debug("Deriving extension for extension with ID [%s]." % 
                      (entry[u'id']))

        if self.is_folder(entry):
            message = ("Could not derive extension for folder.  ENTRY_ID= "
                       "[%s]" % (entry[u'id']))
            
            logging.error(message)
            raise Exception(message)

        # Since we're loading from files and also juggling mime-types coming 
        # from Google, we're just going to normalize all of the character-sets 
        # to ASCII. This is reasonable since they're supposed to be standards-
        # based, anyway.
        mime_type = entry[u'mimeType']
        normal_mime_type = None

        # If there's a standard type on the entry, there won't be a list of
        # export options.
        if u'exportLinks' not in entry or not entry[u'exportLinks']:
            normal_mime_type = mime_type

        # If we have a local mapping of the mime-type on the entry to another 
        # mime-type, only use it if that mime-type is listed among the export-
        # types.
        elif mime_type in self.gd_to_normal_mime_mappings:
            normal_mime_type_candidate = self.gd_to_normal_mime_mappings[mime_type]
            if normal_mime_type_candidate in entry[u'exportLinks']:
                normal_mime_type = normal_mime_type_candidate

        # If we still haven't been able to normalize the mime-type, use the 
        # first export-link
        if normal_mime_type == None:
            normal_mime_type = None

            # If there is one or more mime-type-specific download links.
            for temp_mime_type in entry[u'exportLinks'].iterkeys():
                normal_mime_type = temp_mime_type
                break

        logging.debug("GD MIME [%s] normalized to [%s]." % (mime_type, 
                                                           normal_mime_type))

        # We have an actionable mime-type for the entry, now.

        if normal_mime_type in self.default_extensions:
            file_extension = self.default_extensions[normal_mime_type]
            logging.debug("We had a mapping for mime-type [%s] to extension "
                          "[%s]." % (normal_mime_type, file_extension))

        else:
            try:
                file_extension = guess_extension(normal_mime_type)
            except:
                logging.exception("Could not attempt to derive a file-extension "
                                  "for mime-type [%s]." % (normal_mime_type))
                raise

            file_extension = file_extension[1:]

            logging.debug("Guessed extension [%s] for mime-type [%s]." % 
                          (file_extension, normal_mime_type))

        return file_extension

def get_utility():
    if get_utility.instance == None:
        try:
            get_utility.instance = _DriveUtility()
        except:
            logging.exception("Could not manufacture DriveUtility instance.")
            raise

    return get_utility.instance

get_utility.instance = None

