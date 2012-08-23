#!/usr/bin/python

from apiclient.discovery    import build
from oauth2client.client    import flow_from_clientsecrets
from oauth2client.client    import OOB_CALLBACK_URN

from datetime       import datetime
from argparse       import ArgumentParser
from httplib2       import Http
from sys            import exit, getfilesystemencoding
from collections    import OrderedDict
from threading      import Thread, Event, Lock
from mimetypes      import guess_extension

import logging
import os
import getpass
import pickle
import json

from errors import AuthorizationError, AuthorizationFailureError
from errors import AuthorizationFaultError, MustIgnoreFileError
from errors import FilenameQuantityError

current_user = getpass.getuser()

if current_user == 'root':
    log_filepath = '/var/log/gdrivefs.log'

else:
    log_filepath = 'gdrivefs.log'

logging.basicConfig(
        level       = logging.DEBUG, 
        format      = '%(asctime)s  %(levelname)s %(message)s',
        filename    = log_filepath
    )

app_name = 'GDriveFS Tool'
change_monitor_thread = None

class Conf(object):
    """Manages options."""

    auth_temp_path                  = '/var/cache/gdfs'
    auth_cache_filename             = 'credcache'
    auth_secrets_filepath           = '/etc/gdfs/client_secrets.json'
    gd_to_normal_mapping_filepath   = '/etc/gdfs/mime_mapping.json'
    extension_mapping_filepath      = '/etc/gdfs/extension_mapping.json'
    change_check_interval_s         = .5

    @staticmethod
    def get(key):
        try:
            return Conf.__dict__[key]
        except:
            logging.exception("Could not retrieve config value with key "
                              "[%s]." % (key))
            raise Exception

    @staticmethod
    def set(key, value):
        setattr(Conf, key, value)

class _OauthAuthorize(object):
    """Manages authorization process."""

    flow            = None
    credentials     = None
    cache_filepath  = None
    
    def __init__(self):
        creds_filepath  = Conf.get('auth_secrets_filepath')
        temp_path       = Conf.get('auth_temp_path')
        cache_filename  = Conf.get('auth_cache_filename')

        if not os.path.exists(temp_path):
            try:
                os.makedirs(temp_path)
            except:
                logging.exception("Could not create temporary path [%s]." % 
                                  (temp_path))
                raise

        self.cache_filepath = ("%s/%s" % (temp_path, cache_filename))

        self.flow = flow_from_clientsecrets(creds_filepath, scope='')
        self.flow.scope = self.__get_scopes()
        self.flow.redirect_uri = OOB_CALLBACK_URN

    def __get_scopes(self):
        scopes = "https://www.googleapis.com/auth/drive https://www.googleapis.com/auth/drive.file"
               #'https://www.googleapis.com/auth/userinfo.email '
               #'https://www.googleapis.com/auth/userinfo.profile')
        return scopes

    def step1_get_auth_url(self):
        return self.flow.step1_get_authorize_url()

    def __clear_cache(self):
        try:
            os.remove(self.cache_filepath)
        except:
            pass
    
    def __refresh_credentials(self):
        logging.info("Doing credentials refresh.")

        http = Http()

        try:
            self.credentials.refresh(http)
        except (Exception) as e:
            logging.exception("Could not refresh credentials.")
            raise AuthorizationFailureError

        try:
            self.__update_cache(self.credentials)
        except:
            logging.exception("Could not update cache. We've nullified the "
                              "in-memory credentials.")
            raise
            
        logging.info("Credentials have been refreshed.")
            
    def __step2_check_auth_cache(self):
        # Attempt to read cached credentials.
        
        if self.credentials != None:
            return self.credentials
        
        logging.info("Checking for cached credentials.")

        try:
            with open(self.cache_filepath, 'r') as cache:
                credentials_serialized = cache.read()
        except:
            return None

        # If we're here, we have serialized credentials information.

        logging.info("Raw credentials retrieved from cache.")
        
        try:
            credentials = pickle.loads(credentials_serialized)
        except:
            # We couldn't decode the credentials. Kill the cache.
            self.__clear_cache()

            logging.exception("Could not deserialize credentials. Ignoring.")
            return None

        self.credentials = credentials
            
        # Credentials restored. Check expiration date.
            
        logging.info("Cached credentials found with expire-date [%s]." % 
                     (credentials.token_expiry.strftime('%Y%m%d-%H%M%S')))
        
        self.credentials = credentials

        self.check_credential_state()
        
        return self.credentials

    def check_credential_state(self):
        """Do all of the regular checks necessary to keep our access going, 
        such as refreshing when we expire.
        """
        if(datetime.today() >= self.credentials.token_expiry):
            logging.info("Credentials have expired. Attempting to refresh "
                         "them.")
            
            self.__refresh_credentials()
            return self.credentials
            
    def get_credentials(self):
        try:
            self.credentials = self.__step2_check_auth_cache()
        except:
            logging.exception("Could not check cache for credentials.")
            raise AuthorizationFailureError
    
        if self.credentials == None:
            raise AuthorizationFaultError

        return self.credentials
    
    def __update_cache(self, credentials):
        # Serialized credentials.

        logging.info("Serializing credentials for cache.")
        
        credentials_serialized = None
        
        try:
            credentials_serialized = pickle.dumps(credentials)
        except:
            logging.exception("Could not serialize credentials.")
            raise

        # Write cache file.

        logging.info("Writing credentials to cache.")
        
        try:
            with open(self.cache_filepath, 'w') as cache:
                cache.write(credentials_serialized)
        except:
            logging.exception("Could not write credentials to cache.")
            raise
    
    def step2_doexchange(self, auth_code):
        # Do exchange.

        logging.info("Doing exchange.")
        
        try:
            credentials = self.flow.step2_exchange(auth_code)
        except:
            logging.exception("Could not do auth exchange.")
            raise AuthorizationFailureError
        
        logging.info("Credentials established.")

        try:
            self.__update_cache(credentials)
        except:
            logging.exception("Could not update cache. Process cancelled.")
            raise
        
        self.credentials = credentials
        
def get_auth():
    if get_auth.instance == None:
        get_auth.instance = _OauthAuthorize()
    
    return get_auth.instance

get_auth.instance = None

class Drive_Utility(object):
    """General utility functions loosely related to GD."""

    # Mime-types to translate to, if they appear within the "exportLinks" list.
    gd_to_normal_mime_mappings = {
            'application/vnd.google-apps.document':     'text/plain',
            'application/vnd.google-apps.spreadsheet':  'application/vnd.ms-excel',
            'application/vnd.google-apps.presentation': 'application/vnd.ms-powerpoint',
            'application/vnd.google-apps.drawing':      'application/pdf',
            'application/vnd.google-apps.audio':        'audio/mpeg',
            'application/vnd.google-apps.photo':        'image/png',
            'application/vnd.google-apps.video':        'video/x-flv'
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

    mimetype_folder = u"application/vnd.google-apps.folder"

    def __init__(self):
        self.__load_mappings()

    @staticmethod
    def get_instance():
        try:
            return Drive_Utility.instance
        except:
            Drive_Utility.instance = Drive_Utility()
            return Drive_Utility.instance

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
        mime_type = entry[u'mimeType'].encode('ASCII')

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

class _FileCache(object):
    """An in-memory buffer of the files that we're aware of."""

    entry_cache         = { }
    cleanup_index       = OrderedDict()
#    name_index          = { }
#    name_index_r        = { }
    filepath_index      = { }
    filepath_index_r    = { }
# TODO: The following includes duplicates of the above.
    paths           = { }
    paths_by_name   = { }
    root_entries    = [ ]
    entry_ll        = { }

    locker = Lock()
    latest_change_id = None
    local_character_set = getfilesystemencoding()

    def get_cached_entries(self):
        return self.entry_cache

    def cleanup_by_id(self, id):
        with self.locker:
            try:
                del self.cleanup_index[id]

            except:
                pass

#            try:
#                parent_id = self.name_index_r[id]
#                del self.name_index_r[id]
#                del self.name_index[parent_id][id]
#
#            except:
#                pass

            try:
                filepath = self.filepath_index_r[id]
                del self.filepath_index_r[id]
                del self.filepath_index[filepath]

            except:
                pass

            try:
                del self.entry_cache[id]

            except:
                pass

    def register_entry(self, parent_id, entry, filepath):
        """Register file in the cache. We assume that the file-path is unique 
        (no duplicates).
        """

        entry_id = entry[u'id']

        self.cleanup_by_id(entry_id)

        with self.locker:
            # Store the entry.

            # Keep a forward and reverse index for the file-paths so that we 
            # can allow look-up and clean-up based on IDs while also allowing 
            # us to efficiently manage naming duplicity.

            if filepath in self.filepath_index:
                raise Exception("File-path [%s] is already recorded in the "
                                "cache with a different ID [%s]." % (filepath, 
                                                                    entry_id))

            self.filepath_index[filepath] = entry_id
            self.filepath_index_r[entry_id] = filepath

            # An ordered-dict to keep track of the tracked files by add order.
            self.entry_cache[entry_id] = entry
#            logging.info("ParentID: %s" % (parent_id))
#            # A hash for the heirarchical structure.
#            if parent_id not in self.name_index:
#                self.name_index[parent_id] = { entry_id: entry }
#
#            else:
#                self.name_index[parent_id][entry_id] = entry
#
#            self.name_index_r[entry_id] = parent_id

            # Delete it from the clean-up index.

            try:
                del self.cleanup_index[entry_id]
            except:
                pass

            # Now, add it to the end of the clean-up index.
            self.cleanup_index[entry_id] = entry

    def get_entry_by_filepath(self, filepath):
        logging.info("Retrieving entry for file-path [%s]." % (filepath))

        with self.locker:
            try:
                entry_id = self.filepath_index[filepath]
                entry = self.entry_cache[entry_id]
            except:
                return None

            return entry

    def get_entry_by_id(self, id):
        with self.locker:
            if id in self.entry_cache:
                return entry_cache[id]

            else:
                return None

    def get_latest_change_id(self):
        return self.latest_change_id

    def apply_changes(self, changes):
        # Sort by change-ID (integer) in ascending order.

        logging.debug("Sorting changes to be applied.")

        sorted_changes = sorted(changes.items(), key=lambda t: t[0])
        updates = 0

        with self.locker:
            for change_id, change in sorted_changes:
                logging.debug("Applying change with ID (%d)." % (change_id))

                # If we've already processed updates, skip everything we've already 
                # processed.
                if self.latest_change_id != None and \
                        self.latest_change_id >= change_id:
                    logging.debug("The current change-ID (%d) is less than the"
                                  " last recorded change-ID (%d)." % 
                                  (change_id, self.latest_change_id))
                    continue

                (entry_id, was_deleted, entry) = change

                # Determine if we're already up-to-date.

                if entry_id in self.entry_cache:
                    logging.debug("We received a change item for entry-ID [%s]"
                                  " in our cache." % (entry_id))

                    local_entry = self.entry_cache['entry_id']

                    local_mtime = local_entry[u'modifiedDate']
                    date_obj = dateutil.parser.parse(local_mtime)
                    local_mtime_epoch = time.mktime(date_obj.timetuple())

                    remote_mtime = entry[u'modifiedDate']
                    date_obj = dateutil.parser.parse(remote_mtime)
                    remote_mtime_epoch = time.mktime(date_obj.timetuple())

                    # The local version is newer or equal-to this change.
                    if remote_mtime_epoch <= local_mtime_epoch:
                        logging.info("Change will be ignored because its mtime"
                                     " is [%s] and the one we have is [%s]." % 
                                     (remote_mtime, local_mtime))
                        continue

                # If we're here, our data for this file is old or non-existent.

                updates += 1

                if was_deleted:
                    logging.info("File [%s] will be deleted." % (entry_id))

                    try:
                        self.cleanup_by_id(entry_id)
                    except:
                        logging.exception("Could not cleanup deleted file with"
                                          " ID [%s]." % (entry_id))
                        raise

                else:
                    logging.info("File [%s] will be inserted/updated." % 
                                 (entry_id))

#                    try:
#                        self.register_entry(None, None, entry)
#                    except:
#                        logging.exception("Could not register changed file "
#                                          "with ID [%s].  WAS_DELETED= (%s)" % 
#                                          (entry_id, was_deleted))
#                        raise
        
                logging.info("Update successful.")

                # Update our tracker for which changes have been applied.
                self.latest_change_id = change_id

            logging.info("(%d) updates were performed." % (updates))

    def _is_invisible(self, entry):
        labels = entry[u'labels']
        if labels[u'hidden'] or labels[u'trashed']:
            return True

        return False

    def _build_ll(self, entry_list):
        """Build a linked list of directory-entries. We need it to determine 
        the heirarchy, as well as to calculate the full pathnames of the 
        constituents.
        """

        filtered_list = [ ]
        entry_ll = { }
        for entry in entry_list:
            # At this point, we'll filter any files that we want to hide.
            if self._is_invisible(entry):
                continue

            filtered_list.append(entry)

            entry_id = entry[u'id']
            entry_ll[entry_id] = [entry, None, []]

        root_entries = [ ]
        for entry in filtered_list:
            entry_id = entry[u'id']
            entry_record = entry_ll[entry_id]

            in_root = False
            for parent in entry[u'parents']:
                parent_id = parent[u'id']

                if parent[u'isRoot']:
                    in_root = True

                # If we're not in the root, link to the parent, and vice-versa. 
                # Only do this if the parent has a record, which won't happen 
                # if we've filtered it (above).
                elif parent_id in entry_ll:
                    parent_record = entry_ll[parent_id]

                    entry_record[1] = parent_record
                    parent_record[2].append(entry_record)

            if in_root:
                root_entries.append(entry_record)

        return (root_entries, entry_ll)

    def _translate_filename_charset(self, original_filename):
        """Make sure we're in the right character set."""
        
        return original_filename.encode(self.local_character_set)

    def _build_heirarchy(self, entry_list_raw):
        """Build a heirarchical model of the filesystem."""

        logging.info("Building file heirarchies.")

        # Build a list of relations (as a linked-list).

        try:
            (root_entries, entry_ll) = self._build_ll(entry_list_raw)
        except:
            logging.exception("Could not build heirarchy from files.")
            raise

        path_cache = { }
        def get_path(linked_entry, depth = 1):
            """A recursive path-name finder."""

            if depth > 8:
                raise Exception("Could not calculate paths for folder heirarchy"
                                " that's too deep.")

            if not linked_entry:
                return ''

            entry = linked_entry[0]
            entry_id = entry[u'id']

            if entry_id in path_cache:
                return path_cache[entry_id]

            parent_path = get_path(linked_entry[1], depth + 1)
            path = ("%s/%s" % (parent_path, entry[u'title']))

            # If it's not a folder, try to find an extension to attach to it.

            utility = Drive_Utility.get_instance()

            if not utility.is_folder(entry):
                try:
                    extension = utility.get_extension(entry)
                except:
                    logging.exception("Could not attempt to derive an extension "
                                      "for entry with ID [%s] and mime-type "
                                      "[%s]." % (entry_id, entry[u'mimeType']))
                    raise

                if extension != None:
                    path = ("%s.%s" % (path, extension))

            path = self._translate_filename_charset(path)
            path_cache[entry_id] = path

            return path

        # Produce a dictionary of entry-IDs and unique file-paths.

        paths = { }
        paths_by_name = { }
        for entry_id, linked_entry in entry_ll.iteritems():
            path = get_path(linked_entry)
            
            current_variation = path
            elected_variation = None
            i = 1
            while i < 256:
                if current_variation not in paths_by_name:
                    elected_variation = current_variation
                    break

                i += 1
                current_variation = self._translate_filename_charset("%s (%d)" % (path, i))
            
            if elected_variation == None:
                logging.error("There were too many duplicates of filename [%s]."
                              " We will have to hide all excess entries." % 
                              (base))
                continue

            paths[entry_id] = elected_variation
            paths_by_name[elected_variation] = entry_id

        return (paths, paths_by_name, root_entries, entry_ll)

    def get_children_by_path(self, path):
        if path == '/':
            entries = [ ]
            for linked_entry in self.root_entries:
                entry_id = linked_entry[0][u'id']
                entries.append(entry_id)

            return entries

        elif path not in self.paths_by_name:
            message = "Path [%s] not found in cache."

            logging.error(message)
            raise Exception(message)

        else:
            entry_id = self.paths_by_name[path]
            return [child[0][u'id'] for child in self.entry_ll[entry_id][2]]

    def get_filepaths_for_entries(self, entry_id_list):

        filepaths = { }
        for entry_id in entry_id_list:
            filepaths[entry_id] = self.filepath_index_r[entry_id]

        return filepaths

    def init_heirarchy(self, entry_list_raw):

        logging.info("Initializing file heirarchies.")

        try:
            heirarchy = self._build_heirarchy(entry_list_raw)
        except:
            logging.exception("Could not build heirarchy.")
            raise

        (paths, paths_by_name, root_entries, entry_ll) = heirarchy

        self.paths          = paths
        self.paths_by_name  = paths_by_name
        self.root_entries   = root_entries
        self.entry_ll       = entry_ll

        logging.info("Registering entries in cache.")

        for entry_id, linked_entry in self.entry_ll.iteritems():
            entry = linked_entry[0]
            parent = linked_entry[1]

            if parent:
                parent_id = parent[0][u'id']
            else:
                parent_id = None

            try:
                self.register_entry(parent_id, entry, self.paths[entry_id])
            except:
                logging.exception("Could not register entry with ID [%s] with "
                                  "the cache." % (entry_id))
                raise

        logging.info("All entries registered.")

        return self.paths

def get_cache():
    if get_cache.file_cache == None:
        get_cache.file_cache = _FileCache()

    return get_cache.file_cache

get_cache.file_cache = None

# TODO: Start a cache clean-up thread to make sure that all old items at the 
# beginning of the cleanup_index are constantly pruned.

class _GdriveManager(object):
    """Handles all basic communication with Google Drive. All methods should
    try to invoke only one call, or make sure they handle authentication 
    refreshing when necessary.
    """

    authorize   = None
    credentials = None
    client      = None
    file_cache  = None

    conf_service_name       = 'drive'
    conf_service_version    = 'v2'
    
    def __init__(self):
        self.file_cache = get_cache()
        self.authorize = get_auth()

        self.check_authorization()

    def check_authorization(self):
        self.credentials = self.authorize.get_credentials()

    def get_client(self):
        self.check_authorization()
    
        if self.client != None:
            return self.client

        logging.info("Getting authorized HTTP tunnel.")
            
        http = Http()

        try:
            self.credentials.authorize(http)
        except:
            logging.exception("Could not get authorized HTTP client for Google"
                              " Drive client.")
            raise
    
        logging.info("Building authorized client.")
    
        # Build a client from the passed discovery document path
        client = build(self.conf_service_name, self.conf_service_version, 
                        http=http)

        self.client = client
        return self.client

    def get_entry_info(self, id, allow_cached = True):
        entry_info = self.file_cache.get_entry(self, id)

        if entry_info != None:
            return entry_info
# TODO: Do a look-up, here.
        raise Exception("no entry_index for entry with ID [%s]." % (id))

    def list_changes(self, page_token=None):
        """Get a list of the most recent changes from GD. This only returns one
        page at a time.
        """

        try:
            client = self.get_client()
        except:
            logging.exception("There was an error while acquiring the Google "
                              "Drive client (list_files).")
            raise

        try:
            response = client.changes().list(pageToken=page_token).execute()
        except:
            logging.exception("Problem while listing changes.")
            raise

        largest_change_id   = response[u'largestChangeId']
        items               = response[u'items']

        if u'nextPageToken' in response:
            next_page_token = response[u'nextPageToken']
        else:
            next_page_token = None

        changes = { }
        for item in items:
            change_id   = item[u'id']
            entry_id    = item[u'fileId']
            was_deleted = item[u'deleted']

            if was_deleted:
                entry = None
            else:
                entry = item[u'file']

            changes[int(change_id)] = (entry_id, was_deleted, entry)

        return (largest_change_id, next_page_token, changes)

    def list_files(self, query=None, parentId=None):
        
        try:
            client = self.get_client()
        except:
            logging.exception("There was an error while acquiring the Google "
                              "Drive client (list_files).")
            raise

        try:
            response = client.files().list(q=query).execute()
        except:
            logging.exception("Problem while listing files.")
            raise

        logging.info("File listing received. Sorting.")

        entry_list_raw = response[u"items"]

        try:
            final_list = self.file_cache.init_heirarchy(entry_list_raw)
        except:
            logging.exception("Could not initialize the heirarchical representation.")
            raise

#        try:
#            final_list = []
#            for entry in entry_list_raw:
#                try:
#                    # Register the file in our internal cache. The filename might
#                    # be modified to ensure uniqueness, so if you have to use a 
#                    # filename, use the one that was returned.
#                    final_filename = self.file_cache.register_entry(None, None, 
#                                                                    entry)
#                    final_list.append((entry, final_filename))
#                except (FilenameQuantityError) as e:
#                    logging.exception("We were told to exclude file [%s] from "
#                                      "the listing." % (str(e)))
#                except:
#                    logging.exception("There was an entry-registration "
#                                      "problem.")
#                    raise
#        except:
#            logging.exception("Could not register listed files in cache.")
#            raise
#
        return final_list

    def get_file_info(self, entry_id):
        
        try:
            client = self.get_client()
        except:
            logging.exception("There was an error while acquiring the Google "
                              "Drive client (get_file_info).")
            raise

        try:
            file_info = client.files().get(fileId=entry_id).execute()
        except:
            logging.exception("Could not get the file with ID [%s]." % 
                              (entry_id))
            raise
            
        return file_info

class _GoogleProxy(object):
    """A proxy class that invokes the specified Google Drive call. It will 
    automatically refresh our authorization credentials when the need arises. 
    Nothing inside the Google Drive wrapper class should call this. In general, 
    only external logic should invoke us.
    """
    
    authorize       = None
    gdrive_wrapper  = None
    
    def __init__(self):
        self.authorize      = get_auth()
        self.gdrive_wrapper = _GdriveManager()

    def __getattr__(self, action):
        logging.info("Proxied action [%s] requested." % (action))
    
        try:
            method = getattr(self.gdrive_wrapper, action)
        except (AttributeError):
            logging.exception("Action [%s] can not be proxied to Drive. "
                              "Action is not valid." % (action))
            raise

        def proxied_method(auto_refresh = True, **kwargs):
            # Now, try to invoke the mechanism. If we succeed, return 
            # immediately. If we get an authorization-fault (a resolvable 
            # authorization problem), fall through and attempt to fix it. Allow 
            # any other error to bubble up.
            
            logging.debug("Attempting to invoke method for action [%s]." % 
                          (action))
                
            try:
                return method(**kwargs)
            except (AuthorizationFaultError):
                if not auto_refresh:
                    logging.exception("There was an authorization fault under "
                                      "proxied action [%s], and we were told "
                                      "to NOT auto-refresh." % (action))
                    raise
            except:
                logging.exception("There was an unhandled exception during the"
                                  " execution of the Drive logic for action "
                                  "[%s]." % (action))
                raise
                
            # We had a resolvable authorization problem.

            logging.info("There was an authorization fault under action [%s]. "
                         "Attempting refresh." % (action))
            
            try:
                authorize = get_auth()
                authorize.check_credential_state()
            except:
                logging.exception("There was an error while trying to fix an "
                                  "authorization fault.")
                raise

            # Re-attempt the action.

            logging.info("Refresh seemed successful. Reattempting action "
                         "[%s]." % (action))
            
            try:
                return method(**kwargs)
            except:
                logging.exception("There was an unhandled exception during "
                                  "the execution of the Drive logic for action"
                                  " [%s], and refreshing either didn't help it"
                                  " or wasn't sufficient." % (action))
                raise
        
        return proxied_method
                
def drive_proxy(action, auto_refresh = True, **kwargs):
    if drive_proxy.gp == None:
        try:
            drive_proxy.gp = _GoogleProxy()
        except (Exception) as e:
            logging.exception("There was an exception while creating the proxy"
                              " singleton.")
            raise

    try:    
        method = getattr(drive_proxy.gp, action)
        return method(auto_refresh, **kwargs)
    except (Exception) as e:
        logging.exception("There was an exception while invoking proxy action.")
        raise
    
drive_proxy.gp = None

def apply_changes():
    """Go and get a list of recent changes, and then apply them. This is a 
    separate mechanism because it is too complex an action to put into 
    _GdriveManager, and it can't be put into _FileCache because it would create 
    a cyclical relationship with _GdriveManager."""

    # Get cache object.

    try:
        file_cache = get_cache()
    except:
        logging.exception("Could not acquire cache.")
        raise

    # Get latest change-ID to use as a marker.

    try:
        local_latest_change_id = file_cache.get_latest_change_id(self)
    except:
        logging.exception("Could not get latest change-ID.")
        raise

    # Move through the changes.

    page_token = None
    page_num = 0
    all_changes = []
    while(1):
        logging.debug("Retrieving first page of changes using page-token [%s]." 
                      % (page_token))

        # Get page.

        try:
            change_tuple = drive_proxy('list_changes', page_token=page_token)
            (largest_change_id, next_page_token, changes) = change_tuple
        except:
            logging.exception("Could not get changes for page_token [%s] on "
                              "page (%d)." % (page_token, page_num))
            raise

        logging.info("We have retrieved (%d) recent changes." % (len(changes)))

        # Determine whether we're getting changes added since last time. This 
        # is only really relevant just the first time, as the same value is
        # returned in all subsequent pages.

        if local_latest_change_id != None and largest_change_id <= local_latest_change_id:
            if largest_change_id < local_latest_change_id:
                logging.warning("For some reason, the remote change-ID (%d) is"
                                " -less- than our local change-ID (%d)." % 
                                (largest_change_id, local_largest_change_id))
                return

        # If we're here, this is either the first time, or there have actually 
        # been changes. Collect all of the change information.

        for change_id, change in changes.iteritems():
            all_changes[change_id] = change

        if next_page_token == None:
            break

        page_num += 1 

    # We now have a list of all changes.

    if not changes:
        logging.info("No changes were reported.")

    else:
        logging.info("We will now apply (%d) changes." % (len(changes)))

        try:
            file_cache.apply_changes(changes)
        except:
            logging.exception("An error occured while applying changes.")
            raise

        logging.info("Changes were applied successfully.")

class ChangeMonitor(Thread):
    """The change-management thread."""

    def __init__(self):
        super(self.__class__, self).__init__()
        self.stop_event = Event()

    def run(self):
        while(1):
            if self.stop_event.isSet():
                logging.info("ChangeMonitor is terminating.")
                break
        
            try:
                new_random = random.randint(1, 10)
                q.put(new_random, False)
                log_me("Child put (%d)." % (new_random), True)

            except Full:
                log_me("Can not add new item. Full.")
            
            time.sleep(Conf.get('change_check_interval_s'))

#change_monitor_thread = ChangeMonitor()
#change_monitor_thread.start()

## TODO: Add documentation annotations. Complete comments.
## TODO: Rename properties to be private

#drive_proxy('print_files')
    
#wrapper = _GdriveManager(authorize)
#wrapper.print_files("title contains 'agenda'")
#wrapper.print_files()#(parentId="0B5Ft2OXeDBqSRzlxM0xXdDFDX0E")

#exit()
#f = service.files().get(fileId=file_id).execute()
#downloadUrl = f.get('downloadUrl')
#print(downloadUrl)
#if downloadUrl:
#  resp, f['content'] = service._http.request(downloadUrl)

def main():
    parser = ArgumentParser(prog=app_name)

    group = parser.add_mutually_exclusive_group()
    group.add_argument('-u', '--url', help='Get an authorization URL.', 
                       action='store_true')
    group.add_argument('-a', '--auth', metavar=('authcode'), 
                       help='Register an authorization-code from Google '
                       'Drive.')

    args = parser.parse_args()

    if args.url:
        try:
            authorize = get_auth()
            url = authorize.step1_get_auth_url()
        except:
            logging.exception("Could not produce auth-URL.")
            exit()

        print("To authorize %s to use your Google Drive account, visit the "
              "following URL to produce an authorization code:\n\n%s\n" % 
              (app_name, url))

    if args.auth:
        try:
            authorize = get_auth()
            authorize.step2_doexchange(args.auth)

        except:
            logging.exception("Exchange failed.")
            exit()

        print("Exchange okay.")

    exit()

if __name__ == "__main__":
    main()

