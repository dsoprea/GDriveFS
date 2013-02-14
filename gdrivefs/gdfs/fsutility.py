import re

from gdrivefs.conf import Conf

def get_temp_filepath(normalized_entry, just_info, mime_type):
    if mime_type is None:
        mime_type = normalized_entry.normalized_mime_type

    temp_filename = ("%s.%s" % (normalized_entry.id, mime_type)).\
                    encode('ascii')
    temp_filename = re.sub('[^0-9a-zA-Z_\.]+', '', temp_filename)

    temp_path = Conf.get('file_download_temp_path')
    suffix = '_temp' if just_info else ''
    return ("%s/%s" % (temp_path, temp_filename, suffix))

