import logging
import re

from os.path import split

def strip_export_type(path):

    matched = re.search('#([a-zA-Z0-9\-]+\\+[a-zA-Z0-9\-]+)$', 
                       path.encode('ASCII'))

    mime_type = None

    if matched:
        fragment = matched.group(0)
        mime_type = matched.group(1).replace('+', '/')

        path = path[:-len(fragment)]

    return (path, mime_type)

def split_path(filepath_original, pathresolver_cb):
    """Completely process and distill the requested file-path. The filename can"
    be padded to adjust what's being requested. This will remove all such 
    information, and return the actual file-path along with the extra meta-
    information. pathresolver_cb should expect a single parameter of a path,
    and return a NormalEntry object. This can be used for both directories and 
    files.
    """

    # Remove any export-type that this file-path might've been tagged with.

    try:
        (filepath, mime_type) = strip_export_type(filepath_original)
    except:
        logging.exception("Could not process path [%s] for export-type." % 
                          (original_filepath))
        raise

    logging.debug("File-path [%s] split into filepath [%s] and mime_type "
                  "[%s]." % (filepath_original, filepath, mime_type))

    # Split the file-path into a path and a filename.

    (path, filename) = split(filepath)

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

    is_hidden = (filename[0] == '.') if filename else False

    logging.debug("File-path [%s] split into parent with ID [%s], path [%s], "
                  "unverified filename [%s], mime-type [%s], and is_hidden "
                  "[%s]." % 
                  (filepath_original, parent_entry.id, path, filename, 
                   mime_type, is_hidden))

    return (parent_clause, path, filename, mime_type, is_hidden)

