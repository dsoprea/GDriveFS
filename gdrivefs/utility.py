import json
import logging

from mimetypes import guess_extension
from sys import getfilesystemencoding
from fuse import FuseOSError

from gdrivefs.conf import Conf


def dec_hint(argument_names=[], excluded=[], prefix='', otherdata_cb=None):
    """A decorator for the calling of functions to be emphasized in the 
    logging. Displays prefix and suffix information in the logs.
    """

    try:
        log = dec_hint.log
    except:
        log = logging.getLogger().getChild('VfsAction')
        dec_hint.log = log

    # We use a serial-number so that we can eyeball corresponding pairs of
    # beginning and ending statements in the logs.
    sn = getattr(dec_hint, 'sn', 0) + 1
    dec_hint.sn = sn

    prefix = ("%s: " % (prefix)) if prefix else ''

    def real_decorator(f):
        def wrapper(*args, **kwargs):
        
            try:
                pid = fuse_get_context()[2]
            except:
                # Just in case.
                pid = 0
        
            log.info("%s>>>>>>>>>> %s(%d) >>>>>>>>>> (%d)" % 
                     (prefix, f.__name__, sn, pid))
        
            if args or kwargs:
                condensed = {}
                for i in xrange(len(args)):
                    # Skip the 'self' argument.
                    if i == 0:
                        continue
                
                    if i - 1 >= len(argument_names):
                        break

                    condensed[argument_names[i - 1]] = args[i]

                for k, v in kwargs.iteritems():
                    condensed[k] = v

                values_nice = [("%s= [%s]" % (k, v)) for k, v \
                                                     in condensed.iteritems() \
                                                     if k not in excluded]
                
                if otherdata_cb:
                    data = otherdata_cb(*args, **kwargs)
                    for k, v in data.iteritems():
                        values_nice[k] = v
                
                if values_nice:
                    values_string = '  '.join(values_nice)
                    log.debug("DATA: %s" % (values_string))

            suffix = ''

            try:
                result = f(*args, **kwargs)
            except FuseOSError as e:
                log.info("FUSE error [%s] (%d) will be forwarded back to GDFS: "
                             "%s" % (e.__class__.__name__, e.errno, e))
                raise
            except Exception as e:
                log.exception("There was an exception.")
                suffix = (' (E(%s): "%s")' % (e.__class__.__name__, str(e)))
                raise
            finally:
                log.info("%s<<<<<<<<<< %s(%d) (%d)%s" % 
                         (prefix, f.__name__, sn, pid, suffix))
            
            return result
        return wrapper
    return real_decorator


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

    _mimetype_directory = u'application/vnd.google-apps.folder'
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

    def is_directory(self, entry):
        logging.info("is_directory(%s)" % (entry))
        return (entry.mime_type == self._mimetype_directory)

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

    @property
    def mimetype_directory(self):
        return self._mimetype_directory

def get_utility():
    if get_utility.__instance == None:
        try:
            get_utility.__instance = _DriveUtility()
        except:
            logging.exception("Could not manufacture DriveUtility instance.")
            raise

    return get_utility.__instance

get_utility.__instance = None

