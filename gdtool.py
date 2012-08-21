#!/usr/bin/python

#sys.path.insert(0, 'lib')

from apiclient.discovery import build
from apiclient.http import MediaUpload

from oauth2client.client import flow_from_clientsecrets
from oauth2client.client import FlowExchangeError
from oauth2client.client import AccessTokenRefreshError
from oauth2client.client import OOB_CALLBACK_URN
from oauth2client.client import OAuth2Credentials
from oauth2client.appengine import CredentialsProperty
from oauth2client.appengine import StorageByKeyName
from oauth2client.appengine import simplejson as json

from datetime import datetime
from argparse import ArgumentParser

import pickle
import os
import httplib2
import logging
import sys
import collections
import threading

logging.basicConfig(
        level       = logging.DEBUG, 
        format      = '%(asctime)s  %(levelname)s %(message)s',
        filename    = '/tmp/gdrivefs.log'
    )

app_name = 'GDriveFS Tool'

change_monitor_thread = None

class AuthorizationError(Exception):
    pass

class AuthorizationFailureError(AuthorizationError):
    """There was a general authorization failure."""
    pass
        
class AuthorizationFaultError(AuthorizationError):
    """Our authorization is not available or has expired."""
    pass

class MustIgnoreFileError(Exception):
    """An error requiring us to ignore the file."""
    pass

class FilenameQuantityError(MustIgnoreFileError):
    """Too many filenames share the same name in a single directory."""
    pass

class Conf(object):
    """Manages track of changeable parameters."""

    auth_temp_path          = '/var/cache/gdfs'
    auth_cache_filename     = 'credcache'
    auth_secrets_filepath   = '/etc/gdfs/client_secrets.json'
    change_check_interval_s = .5

    @staticmethod
    def get(key):
        try:
            return Conf.__dict__[key]
        except:
            logging.exception("Could not retrieve config value with key "
                              "[%s]." % (key))
            raise

    @staticmethod
    def set(key, value):
        setattr(Conf, key, value)

class _OauthAuthorize(object):
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

        http = httplib2.Http()

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

        logging.info("Raw credentials retrieved from cache.");
        
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

class _FileCache(object):
    entry_cache         = { }
    cleanup_index       = collections.OrderedDict()
    name_index          = { }
    name_index_r        = { }
    filepath_index      = { }
    filepath_index_r    = { }

    locker = threading.Lock()
    latest_change_id = None

    def cleanup_byid(self, id):
        with self.locker:
            try:
                del self.cleanup_index[id]

#                logging.debug("Removing clean-up index for ID [%s]." % (id))
            except:
                pass

            try:
                parent_id = self.name_index_r[id]
                del self.name_index_r[id]
                del self.name_index[parent_id][id]

#                logging.debug("Removing name-index for ID [%s]." % (id))
            except:
                pass

            try:
                filepath = self.filepath_index_r[id]
                del self.filepath_index_r[id]
                del self.filepath_index[filepath]

#                logging.debug("Removing file-path index for ID [%s]." % (id))
            except:
                pass

            try:
                del self.entry_cache[id]

#                logging.debug("Removing primary entry-cache item for ID [%s]." 
#                               % (id))
            except:
                pass

    def register_entry(self, parent_id, parent_path, entry):
        entry_id = entry[u'id'].encode('ascii')
        filename = entry[u'title'].encode('ascii')

        self.cleanup_byid(entry_id)

        with self.locker:
            # Store the entry.

            # TODO: We translate the file-name information coming back into 
            # ASCII. We're not sure how this affects other locales, but it 
            # won't work in ours if we don't.

            if parent_path == None:
                parent_path = ''

            # Keep a forward and reverse index for the file-paths so that we 
            # can allow look-up and clean-up based on IDs while also allowing 
            # us to efficiently manage naming duplicity.
            #
            # Here, we'll also determine if we need to modify the name slightly 
            # in the presence of duplicates.

            i = 1
            base_name = filename
            current_variation = filename
            elected_variation = None
            max_duplicates = 255
            while i < max_duplicates:
                current_filepath = ('%s/%s' % (parent_path, current_variation))
                if current_filepath not in self.filepath_index:
                    elected_variation = current_variation
                    break

                i += 1
                current_variation = ("%s (%d)" % (base_name, i))

            # There are too many files with the same location and name. Raise 
            # an error prior to the cache being affected.
            if elected_variation == None:
                raise FilenameQuantityError(base_name)

            filepath = current_filepath

            self.filepath_index[filepath] = entry_id
            self.filepath_index_r[entry_id] = filepath

            # An ordered-dict to keep track of the tracked files by add order.
            self.entry_cache[entry_id] = entry

            # A hash for the heirarchical structure.
            if parent_id not in self.name_index:
                self.name_index[parent_id] = { entry_id: entry }

            else:
                self.name_index[parent_id][entry_id] = entry

            self.name_index_r[entry_id] = parent_id

            # Delete it from the clean-up index.

            try:
                del self.cleanup_index[entry_id]
            except:
                pass

            # Now, add it to the end of the clean-up index.
            self.cleanup_index[entry_id] = entry

            # Return the registered name of the file, which may not match the 
            # original name.
            return elected_variation

    def get_entry_byfilepath(self, filepath):
        with self.locker:
            try:
                entry_id = self.filepath_index[filepath]
                entry = self.entry_cache[entry_id]
            except:
                return None

            return entry

    def get_entry_byid(self, id):
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
                if self.latest_change_id != None and self.latest_change_id >= change_id:
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
                        self.cleanup_byid(entry_id)
                    except:
                        logging.exception("Could not cleanup deleted file with"
                                          " ID [%s]." % (entry_id))
                        raise

                else:
                    logging.info("File [%s] will be inserted/updated." % (entry_id))

                    try:
                        self.register_entry(None, None, entry)
                    except:
                        logging.exception("Could not register changed file with ID [%s].  WAS_DELETED= (%s)" % (entry_id, was_deleted))
                        raise
        
                logging.info("Update successful.")

                # Update our tracker for which changes have been applied.
                self.latest_change_id = change_id

            logging.info("(%d) updates were performed." % (updates))

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
    conf_mimetype_folder    = ["application/vnd.google-apps.folder"]
    
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
            
        http = httplib2.Http()

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

            print("Files-List etag: [%s]" % (response[u'etag']))

            files = response["items"]
            final_list = []
            for entry in files:
                try:
                    # Register the file in our internal cache. The filename might
                    # be modified to ensure uniqueness, so if you have to use a 
                    # filename, use the one that was returned.
                    final_filename = self.file_cache.register_entry(None, None, 
                                                                    entry)
                    final_list.append((entry, final_filename))
                except (FilenameQuantityError) as e:
                    logging.exception("We were told to exclude file [%s] from "
                                      "the listing." % (str(e)))
                except:
                    logging.exception("There was an entry-registration "
                                      "problem.")
                    raise
        except:
            logging.exception("Problem while listing files.")
            raise

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
    only more external logic should invoke us.
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
    page_num = 0;
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
            logging.exception("An error occured while applying changes.");
            raise

        logging.info("Changes were applied successfully.")

class ChangeMonitor(threading.Thread):
    def __init__(self):
        super(self.__class__, self).__init__();
        self.stop_event = threading.Event();

    def run(self):
        while(1):
            if self.stop_event.isSet():
                logging.info("ChangeMonitor is terminating.");
                break;
        
            try:
                new_random = random.randint(1, 10);
                q.put(new_random, False);
                log_me("Child put (%d)." % (new_random), True);

            except Full:
                log_me("Can not add new item. Full.");
            
            time.sleep(Conf.get('change_check_interval_s'));

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
            sys.exit()

        print("To authorize %s to use your Google Drive account, visit the "
              "following URL to produce an authorization code:\n\n%s\n" % 
              (app_name, url))

    if args.auth:
        authorize = get_auth()

        try:
            authorize.step2_doexchange(args.auth)

        except:
            logging.exception("Exchange failed.")
            sys.exit()

        print("Exchange okay.")

    sys.exit()

if __name__ == "__main__":
    main()

