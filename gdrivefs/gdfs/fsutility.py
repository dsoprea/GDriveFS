import logging
import re

from os.path import split
from fuse import FuseOSError, fuse_get_context

from gdrivefs.errors import GdNotFoundError

log = logging.getLogger('FsUtility')

def dec_hint(argument_names=[], excluded=[], prefix='', otherdata_cb=None):
    """A decorator for the calling of functions to be emphasized in the 
    logging. Displays prefix and suffix information in the logs.
    """

#    try:
#        log = dec_hint.log
#    except:
#        log = log.getLogger().getChild('VfsAction')
#        dec_hint.log = log
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
        
            if not prefix:
                log.info('--------------------------------------------------')

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
                log.info("FUSE error [%s] (%s) will be forwarded back to "
                         "GDFS: %s" % (e.__class__.__name__, e.errno, str(e)))
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

def strip_export_type(path):

    matched = re.search('#([a-zA-Z0-9\-]+\\+[a-zA-Z0-9\-]+)?$', 
                       path.encode('ASCII'))

    mime_type = None

    if matched:
        fragment = matched.group(0)
        mime_type = matched.group(1)
        
        if mime_type is not None:
            mime_type = mime_type.replace('+', '/')

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
        log.exception("Could not process path [%s] for export-type." % 
                      (filepath_original))
        raise

    log.debug("File-path [%s] split into filepath [%s] and mime_type "
              "[%s]." % (filepath_original, filepath, mime_type))

    # Split the file-path into a path and a filename.

    (path, filename) = split(filepath)

    # Lookup the file, as it was listed, in our cache.

    try:
        path_resolution = pathresolver_cb(path)
    except:
        log.exception("Exception while getting entry from path [%s]." % (path))
        raise GdNotFoundError()

    if not path_resolution:
        log.debug("Path [%s] does not exist for split." % (path))
        raise GdNotFoundError()

    (parent_entry, parent_clause) = path_resolution

    is_hidden = (filename[0] == '.') if filename else False

    log.debug("File-path [%s] split into parent with ID [%s], path [%s], "
              "unverified filename [%s], mime-type [%s], and is_hidden [%s]." % 
              (filepath_original, parent_entry.id, path, filename, 
               mime_type, is_hidden))

    return (parent_clause, path, filename, mime_type, is_hidden)

def split_path_nolookups(filepath_original):
    """This allows us to get the is-hidden flag, mimetype info, path, and 
    filename, without doing the [time consuming] lookup if unnecessary.
    """

    # Remove any export-type that this file-path might've been tagged with.

    try:
        (filepath, mime_type) = strip_export_type(filepath_original)
    except:
        log.exception("Could not process path [%s] for export-type." % 
                      (filepath_original))
        raise

    # Split the file-path into a path and a filename.

    (path, filename) = split(filepath)

    # We don't remove the period, if we will mark it as hidden, as appropriate.
    is_hidden = (filename[0] == '.') if filename else False

    return (path, filename, mime_type, is_hidden)

def build_filepath(path, filename):
    separator = '/' if path != '/' else ''

    return ('%s%s%s' % (path, separator, filename))

def escape_filename_for_query(filename):
    return filename.replace("\\", "\\\\").replace("'", "\\'")

