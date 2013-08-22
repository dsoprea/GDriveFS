import json
import logging

from sys import getfilesystemencoding

from gdrivefs.conf import Conf


class _DriveUtility(object):
    """General utility functions loosely related to GD."""

    # Mime-types to translate to, if they appear within the "exportLinks" list.
    gd_to_normal_mime_mappings = {
            'application/vnd.google-apps.document':        
                'text/plain',
            'application/vnd.google-apps.spreadsheet':     
                'application/vnd.ms-excel',
            'application/vnd.google-apps.presentation':    
                'application/vnd.ms-powerpoint',
            'application/vnd.google-apps.drawing':         
                'application/pdf',
            'application/vnd.google-apps.audio':           
                'audio/mpeg',
            'application/vnd.google-apps.photo':           
                'image/png',
            'application/vnd.google-apps.video':           
                'video/x-flv'
        }

    # Default extensions for mime-types.
    default_extensions = { 
            'text/plain':                       'txt',
            'application/vnd.ms-excel':         'xls',
            'application/vnd.ms-powerpoint':    'ppt',
            'application/pdf':                  'pdf',
            'audio/mpeg':                       'mp3',
            'image/png':                        'png',
            'video/x-flv':                      'flv'
        }

    local_character_set = getfilesystemencoding()

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

    def get_first_mime_type_by_extension(self, extension):

        found = [ mime_type 
                    for mime_type, temp_extension 
                    in self.default_extensions.iteritems()
                    if temp_extension == extension ]

        if not found:
            return None

        return found[0]

    def translate_filename_charset(self, original_filename):
        """Convert the given filename to the correct character set."""
        
        return original_filename.encode(self.local_character_set)

def get_utility():
    if get_utility.__instance == None:
        try:
            get_utility.__instance = _DriveUtility()
        except:
            logging.exception("Could not manufacture DriveUtility instance.")
            raise

    return get_utility.__instance

get_utility.__instance = None

