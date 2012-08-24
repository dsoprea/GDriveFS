#!/usr/bin/python

from apiclient.discovery    import build
from oauth2client.client    import flow_from_clientsecrets
from oauth2client.client    import OOB_CALLBACK_URN

from datetime       import datetime
from argparse       import ArgumentParser
from httplib2       import Http
from sys            import exit
from threading      import Thread, Event

import logging
import os
import pickle
import json

from errors import AuthorizationError, AuthorizationFailureError
from errors import AuthorizationFaultError, MustIgnoreFileError
from errors import FilenameQuantityError
from cache import get_cache
from conf import Conf

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
    if get_auth.instance == None:
        try:
            get_auth.instance = _OauthAuthorize()
        except:
            logging.exception("Could not manufacture OauthAuthorize instance.")
            raise
    
    return get_auth.instance

get_auth.instance = None

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

    def get_children_under_parent_id(self, parent_id, query=None):

        logging.info("Getting client for child-listing.")

        try:
            client = self.get_client()
        except:
            logging.exception("There was an error while acquiring the Google "
                              "Drive client (list_files_by_parent_id).")
            raise

        logging.info("Listing entries under parent with ID [%s]." % (parent_id))

        try:
            response = client.children().list(q=query,folderId=parent_id).execute()
        except:
            logging.exception("Problem while listing files.")
            raise

        return [ entry[u'id'] for entry in response[u'items'] ]

    def list_files(self, query=None):
        
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

