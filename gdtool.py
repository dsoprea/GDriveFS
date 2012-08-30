#!/usr/bin/python

import logging
import os
import pickle
import json
import re
import dateutil.parser

from apiclient.discovery    import build
from oauth2client.client    import flow_from_clientsecrets
from oauth2client.client    import OOB_CALLBACK_URN

from time           import mktime, time
from datetime       import datetime
from argparse       import ArgumentParser
from httplib2       import Http
from sys            import exit
from threading      import Thread, Event

from errors import AuthorizationError, AuthorizationFailureError
from errors import AuthorizationFaultError, MustIgnoreFileError
from errors import FilenameQuantityError, ExportFormatError
from conf import Conf
from utility import get_utility

app_name = 'GDriveFS Tool'
change_monitor_thread = None

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
    if get_auth.__instance == None:
        try:
            get_auth.__instance = _OauthAuthorize()
        except:
            logging.exception("Could not manufacture OauthAuthorize instance.")
            raise
    
    return get_auth.__instance

get_auth.__instance = None

class NormalEntry(object):

    def __init__(self, gd_resource_type, raw_data):
        # LESSONLEARNED: We had these set as properties, but CPython was 
        #                reusing the reference between objects.
        self.info = { }
        self.parents = [ ]

        try:
            self.info['mime_type']                  = raw_data[u'mimeType']
            self.info['labels']                     = raw_data[u'labels']
            self.info['id']                         = raw_data[u'id']
            self.info['title']                      = raw_data[u'title']
            self.info['last_modifying_user_name']   = raw_data[u'lastModifyingUserName']
            self.info['writers_can_share']          = raw_data[u'writersCanShare']
            self.info['owner_names']                = raw_data[u'ownerNames']
            self.info['editable']                   = raw_data[u'editable']
            self.info['user_permission']            = raw_data[u'userPermission']
            self.info['modified_date']              = raw_data[u'modifiedDate']
            self.info['created_date']               = raw_data[u'createdDate']

            self.info['download_links']         = raw_data[u'exportLinks'] if u'exportLinks' in raw_data else { }
            self.info['link']                   = raw_data[u'embedLink'] if u'embedLink' in raw_data else None
            self.info['modified_by_me_date']    = raw_data[u'modifiedByMeDate'] if u'modifiedByMeDate' in raw_data else None
            self.info['last_viewed_by_me_date'] = raw_data[u'lastViewedByMeDate'] if u'lastViewedByMeDate' in raw_data else None
            self.info['quota_bytes_used']       = int(raw_data[u'quotaBytesUsed']) if u'quotaBytesUsed' in raw_data else 0

            # This is encoded for displaying locally.
            self.info['title_fs'] = get_utility().translate_filename_charset(raw_data[u'title'])

            for parent in raw_data[u'parents']:
                self.parents.append(parent[u'id'])

        except (KeyError) as e:
            logging.exception("Could not normalize entry on raw key [%s]. Does not exist in source." % (str(e)))
            raise

    def __getattr__(self, key):
        if key not in self.info:
            return None

        return self.info[key]

    def __str__(self):
        return ("<Normalized entry object with ID [%s]: %s>" % (self.id, self.title))

    @property
    def is_directory(self):
        return get_utility().is_directory(self)

class LiveReader(object):
    """A base object for data that can be retrieved on demand."""

    data = None

    def __getitem__(self, key):
        child_name = self.__class__.__name__

        logging.debug("Key [%s] requested on LiveReader type [%s]." % (key, child_name))

        try:
            return self.data[key]
        except:
            pass

        try:
            self.data = self.get_data(key)
        except:
            logging.exception("Could not retrieve data for live-updater wrapping [%s]." % (child_name))
            raise

        try:
            return self.data[key]
        except:
            logging.exception("We just updated live-updater wrapping [%s], but"
                              " we must've not been able to find entry [%s]." % 
                              (child_name, key))
            raise

    def get_data(self, key):
        raise NotImplementedError("get_data() method must be implemented in the LiveReader child.")

    @classmethod
    def get_instance(cls):
        """A helper method to dispense a singleton of whomever is inheriting "
        from us.
        """

        class_name = cls.__name__

        try:
            LiveReader.__instances
        except:
            LiveReader.__instances = { }

        try:
            return LiveReader.__instances[class_name]
        except:
            LiveReader.__instances[class_name] = cls()
            return LiveReader.__instances[class_name]

class AccountInfo(LiveReader):
    """Encapsulates our account info."""

    def get_data(self, key):
        try:
            return drive_proxy('get_about_info')
        except:
            logging.exception("get_about_info() call failed.")
            raise

    @property
    def root_id(self):
        return self[u'rootFolderId']

class _GdriveManager(object):
    """Handles all basic communication with Google Drive. All methods should
    try to invoke only one call, or make sure they handle authentication 
    refreshing when necessary.
    """

    authorize   = None
    credentials = None
    client      = None

    conf_service_name       = 'drive'
    conf_service_version    = 'v2'
    
    def __init__(self):
        self.authorize = get_auth()

        self.check_authorization()

    def check_authorization(self):
        self.credentials = self.authorize.get_credentials()

    def get_authed_http(self):

        self.check_authorization()
    
        logging.info("Getting authorized HTTP tunnel.")
            
        http = Http()

        try:
            self.credentials.authorize(http)
        except:
            logging.exception("Could not get authorized HTTP client for Google"
                              " Drive client.")
            raise

        return http

    def get_client(self):

        if self.client != None:
            return self.client

        try:
            authed_http = self.get_authed_http()
        except:
            logging.exception("Could not get authed Http instance.")
            raise

        logging.info("Building authorized client from Http.  TYPE= [%s]" % (type(authed_http)))
    
        # Build a client from the passed discovery document path
        client = build(self.conf_service_name, self.conf_service_version, 
                        http=authed_http)

        self.client = client
        return self.client

    def get_about_info(self):
        """Return the 'about' information for the drive."""

        try:
            client = self.get_client()
        except:
            logging.exception("There was an error while acquiring the Google "
                              "Drive client (get_about).")
            raise

        try:
            response = client.about().get().execute()
        except:
            logging.exception("Problem while getting 'about' information.")
            raise
        
        return response

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

    def get_parents_containing_id(self, child_id, max_results=None):
        
        logging.info("Getting client for parent-listing.")

        try:
            client = self.get_client()
        except:
            logging.exception("There was an error while acquiring the Google "
                              "Drive client (get_parents_containing_id).")
            raise

        logging.info("Listing entries over child with ID [%s]." % (child_id))

        try:
            response = client.parents().list(fileId=child_id).execute()
        except:
            logging.exception("Problem while listing files.")
            raise

        return [ entry[u'id'] for entry in response[u'items'] ]

    def get_children_under_parent_id(self, parent_id, query_contains_string=None, query_is_string=None):

        logging.info("Getting client for child-listing.")

        try:
            client = self.get_client()
        except:
            logging.exception("There was an error while acquiring the Google "
                              "Drive client (get_children_under_parent_id).")
            raise

        if query_contains_string and query_is_string:
            logging.exception("The query_contains_string and query_is_string "
                              "parameters are mutually exclusive.")
            raise

        if query_is_string:
            query = ("title='%s'" % (query_is_string.replace("'", "\\'")))
        elif query_contains_string:
            query = ("title contains '%s'" % (query_contains_string.replace("'", "\\'")))
        else:
            query = None

        logging.info("Listing entries under parent with ID [%s].  QUERY= [%s]" % (parent_id, query))

        try:
            response = client.children().list(q=query,folderId=parent_id).execute()
        except:
            logging.exception("Problem while listing files.")
            raise

        return [ entry[u'id'] for entry in response[u'items'] ]

    def get_entries(self, entry_ids):

        retrieved = { }
        for entry_id in entry_ids:
            try:
                entry = drive_proxy('get_entry', entry_id=entry_id)
            except:
                logging.exception("Could not retrieve entry with ID [%s]." % 
                                  (entry_id))
                raise

            retrieved[entry_id] = entry

        logging.debug("(%d) entries were retrieved." % (len(retrieved)))

        return retrieved

    def get_entry(self, entry_id):
        
        try:
            client = self.get_client()
        except:
            logging.exception("There was an error while acquiring the Google "
                              "Drive client (get_entry).")
            raise

        try:
            entry_raw = client.files().get(fileId=entry_id).execute()
        except:
            logging.exception("Could not get the file with ID [%s]." % 
                              (entry_id))
            raise

        try:
            entry = NormalEntry('direct_read', entry_raw)
        except:
            logging.exception("Could not normalize raw-data for entry with ID [%s]." % (entry_id))
            raise

        return entry

    def list_files(self):
        
        logging.info("Listing all files.")

        try:
            client = self.get_client()
        except:
            logging.exception("There was an error while acquiring the Google "
                              "Drive client (list_files).")
            raise

        try:
            result = client.files().list().execute()
        except:
            logging.exception("Could not get the list of files.")
            raise

        entries = []
        for entry_raw in result[u'items']:
            try:
                entry = NormalEntry('files_list', entry_raw)
            except:
                logging.exception("Could not normalize raw-data for entry with"
                                  " ID [%s]." % (entry_raw[u'id']))
                raise

            entries.append(entry)

        return entries

    def download_to_local(self, normalized_entry, mime_type):
        """Download the given file. If we've cached a previous download and the 
        mtime hasn't changed, re-use.
        """

        logging.info("Downloading entry with ID [%s] and mime-type [%s]." % 
                     (normalized_entry.id, mime_type))

        if mime_type not in normalized_entry.download_links:
            message = ("Entry with ID [%s] can not be exported to type [%s]. The available types are: %s" % 
                       (normalized_entry.id, mime_type, ', '.join(normalized_entry.download_links.keys())))

            logging.warning(message)
            raise ExportFormatError(message)

        temp_path = Conf.get('file_download_temp_path')

        if not os.path.isdir(temp_path):
            try:
                os.makedirs(temp_path)
            except:
                logging.exception("Could not create temporary download path "
                                  "[%s]." % (temp_path))
                raise

        # Produce a file-path of a temporary file that we can store the data 
        # to. More often than not, we'll be called when the OS wants to read 
        # the file, and we'll need the data at hand in order to page through 
        # it.

        temp_filename = ("%s.%s" % (normalized_entry.id, mime_type)). \
                            encode('ascii')
        temp_filename = re.sub('[^0-9a-zA-Z_\.]+', '', temp_filename)
        temp_filepath = ("%s/%s" % (temp_path, temp_filename))

        gd_date_obj = dateutil.parser.parse(normalized_entry.modified_date)
        gd_mtime_epoch = mktime(gd_date_obj.timetuple())

        logging.info("File will be downloaded to [%s]." % (temp_filepath))

        use_cache = False
        if os.path.isfile(temp_filepath):
            # Determine if a local copy already exists that we can use.
            try:
                stat = os.stat(temp_filepath)
            except:
                logging.exception("Could not retrieve stat() information for "
                                  "temp download file [%s]." % (temp_filepath))
                raise

            if gd_mtime_epoch == stat.st_mtime:
                use_cache = True

        if use_cache:
            # Use the cache. It's fine.

            logging.info("File retrieved from the previously downloaded, still-current file.")
            return temp_filepath

        # Go and get the file.

        try:
            authed_http = self.get_authed_http()
        except:
            logging.exception("Could not get authed Http instance for download.")
            raise

        url = normalized_entry.download_links[mime_type]

        logging.debug("Downloading file from [%s]." % (url))

        try:
            data_tuple = authed_http.request(url)
        except:
            logging.exception("Could not download entry with ID [%s], type "
                              "[%s], and URL [%s]." % (normalized_entry.id, mime_type, url))
            raise

        (response_headers, data) = data_tuple

        # Throw a log-item if we see any "Range" response-headers. If GD ever
        # starts supporting "Range" headers, we'll be able to write smarter 
        # download mechanics (resume, etc..).

        r = re.compile('Range')
        range_found = [("%s: %s" % (k, v)) for k, v in response_headers.iteritems() if r.match(k)]
        if range_found:
            logger.info("GD has returned Range-related headers: %s" % (", ".join(found)))

        logging.info("Downloaded file is (%d) bytes. Writing to [%s]." % (len(data), temp_filepath))

        try:
            with open(temp_filepath, 'wb') as f:
                f.write(data)
        except:
            logging.exception("Could not cached downloaded file. Skipped.")

        else:
            logging.info("File written to cache successfully.")

        try:
            os.utime(temp_filepath, (time(), gd_mtime_epoch))
        except:
            logging.exception("Could not set time on [%s]." % (temp_filepath))
            raise

        return temp_filepath

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

