import logging
import re

from os.path import split

from gdrivefs.conf import Conf
from gdrivefs.utility import get_utility

def get_temp_filepath(normalized_entry, just_info, mime_type):
    if mime_type is None:
        mime_type = normalized_entry.normalized_mime_type

    temp_filename = ("%s.%s" % (normalized_entry.id, mime_type)).\
                    encode('ascii')
    temp_filename = re.sub('[^0-9a-zA-Z_\.]+', '', temp_filename)

    temp_path = Conf.get('file_download_temp_path')
    suffix = '_temp' if just_info else ''
    return ("%s/%s%s" % (temp_path, temp_filename, suffix))

def strip_export_type(path, set_mime=True):

    rx = re.compile('(#([a-zA-Z0-9]+))?(\$)?$')
    matched = rx.search(path.encode('ASCII'))

    extension = None
    mime_type = None
    just_info = None

    if matched:
        fragment = matched.group(0)
        extension = matched.group(2)
        just_info = (matched.group(3) == '$')

        if fragment:
            path = path[:-len(fragment)]

        if not extension:
            extension_rx = re.compile('\.([a-zA-Z0-9]+)$')
            matched = extension_rx.search(path.encode('ASCII'))

            if matched:
                extension = matched.group(1)

        if extension:
            logging.info("User wants to export to extension [%s]." % 
                         (extension))

            if set_mime:
                try:
                    mime_type = get_utility().get_first_mime_type_by_extension \
                                    (extension)
                except:
                    logging.warning("Could not render a mime-type for "
                                    "prescribed extension [%s], for read." % 
                                    (extension))

                if mime_type:
                    logging.info("We have been told to export using mime-type "
                                 "[%s]." % (mime_type))

    return (path, extension, just_info, mime_type)

def split_path(filepath, pathresolver_cb):
    """Completely process and distill the requested file-path. The filename can"
    be padded to adjust what's being requested. This will remove all such 
    information, and return the actual file-path along with the extra meta-
    information. pathresolver_cb should expect a single parameter of a path,
    nd return a NormalEntry object.
    """

    # Remove any export-type that this file-path might've been tagged with.

    try:
        _initial_split_results = strip_export_type(filepath)
        (filepath, extension, just_info, mime_type) = _initial_split_results
    except:
        logging.exception("Could not process path [%s] for export-type." % 
                          (filepath))
        raise

    # Split the file-path into a path and a filename.

    (path, filename) = split(filepath)

    if path[0] != '/' or filename == '':
        message = ("Could not create directory with badly-formatted "
                   "file-path [%s]." % (filepath))

        logging.error(message)
        raise ValueError(message)

    # Lookup the file, as it was listed, in our cache.

    try:
        path_resolution = pathresolver_cb(path)
    except:
        logger.exception("Exception while getting entry from path [%s]." % 
                         (path))
        raise GdNotFoundError()

    if not path_resolution:
        logging.debug("Path [%s] does not exist for split." % (path))
        raise GdNotFoundError()

    (parent_entry, parent_clause) = path_resolution

    # Strip a prefixing dot, if present.

    if filename[0] == '.':
        is_hidden = True
#        filename = filename[1:]

    else:
        is_hidden = False

    logging.debug("File-path [%s] dereferenced to parent with ID [%s], path "
                  "[%s], filename [%s], extension [%s], mime-type [%s], "
                  "is_hidden [%s], and just-info [%s]." % 
                  (filepath, parent_entry.id, path, filename, extension, 
                   mime_type, is_hidden, just_info))

    return (parent_clause, path, filename, extension, mime_type, is_hidden, 
            just_info)

