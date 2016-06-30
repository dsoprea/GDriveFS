import logging
import json
import re
import sys

import gdrivefs.conf

_logger = logging.getLogger(__name__)

# TODO(dustin): Make these individual functions.


class _DriveUtility(object):
    """General utility functions loosely related to GD."""

#    # Mime-types to translate to, if they appear within the "exportLinks" list.
#    gd_to_normal_mime_mappings = {
#            'application/vnd.google-apps.document':        
#                'text/plain',
#            'application/vnd.google-apps.spreadsheet':     
#                'application/vnd.ms-excel',
#            'application/vnd.google-apps.presentation':    
#/gd_to_normal_mime_mappings
#                'application/vnd.ms-powerpoint',
#            'application/vnd.google-apps.drawing':         
#                'application/pdf',
#            'application/vnd.google-apps.audio':           
#                'audio/mpeg',
#            'application/vnd.google-apps.photo':           
#                'image/png',
#            'application/vnd.google-apps.video':           
#                'video/x-flv'
#        }

    # Default extensions for mime-types.
# TODO(dustin): !! Move this to the config directory.
    default_extensions = { 
            'text/plain':                       'txt',
            'application/vnd.ms-excel':         'xls',
            'application/vnd.ms-powerpoint':    'ppt',
            'application/pdf':                  'pdf',
            'audio/mpeg':                       'mp3',
            'image/png':                        'png',
            'video/x-flv':                      'flv'
        }

    local_character_set = sys.getfilesystemencoding()

    def __init__(self):
        self.__load_mappings()

    def __load_mappings(self):
        # Allow someone to override our default mappings of the GD types.

# TODO(dustin): Isn't actually used, so commenting.
#        gd_to_normal_mapping_filepath = \
#            gdrivefs.conf.Conf.get('gd_to_normal_mapping_filepath')
#
#        try:
#            with open(gd_to_normal_mapping_filepath, 'r') as f:
#                self.gd_to_normal_mime_mappings.extend(json.load(f))
#        except IOError:
#            _logger.info("No mime-mapping was found.")

        # Allow someone to set file-extensions for mime-types, and not rely on 
        # Python's educated guesses.

        extension_mapping_filepath = \
            gdrivefs.conf.Conf.get('extension_mapping_filepath')

        try:
            with open(extension_mapping_filepath, 'r') as f:
                self.default_extensions.extend(json.load(f))
        except IOError:
            _logger.info("No extension-mapping was found.")

    def get_first_mime_type_by_extension(self, extension):

        found = [ 
            mime_type 
            for mime_type, temp_extension 
            in self.default_extensions.iteritems()
            if temp_extension == extension
        ]

        if not found:
            return None

        return found[0]

    def translate_filename_charset(self, original_filename):
        """Convert the given filename to the correct character set."""

        # fusepy doesn't support the Python 2.x Unicode type. Expect a native
        # string (anything but a byte string).
        return original_filename
       
#        # If we're in an older version of Python that still defines the Unicode
#        # class and the filename isn't unicode, translate it.
#
#        try:
#            sys.modules['__builtin__'].unicode
#        except AttributeError:
#            pass
#        else:
#            if issubclass(original_filename.__class__, unicode) is False:
#                return unicode(original_filename)#original_filename.decode(self.local_character_set)
#
#        # It's already unicode. Don't do anything.
#        return original_filename

    def make_safe_for_filename(self, text):
        """Remove any filename-invalid characters."""
    
        return re.sub('[^a-z0-9\-_\.]+', '', text)

utility = _DriveUtility()
