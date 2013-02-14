import logging
import pickle
import json

from oauth2client.client import flow_from_clientsecrets
from oauth2client.client import OOB_CALLBACK_URN

from datetime import datetime
from httplib2 import Http
from tempfile import NamedTemporaryFile
from os import remove

from gdrivefs.errors import AuthorizationFailureError, AuthorizationFaultError
from gdrivefs.conf import Conf


class _OauthAuthorize(object):
    """Manages authorization process."""

    __log = None

    flow            = None
    credentials     = None
    cache_filepath  = None
    
    def __init__(self):
        self.__log = logging.getLogger().getChild('OauthAuth')

        cache_filepath  = Conf.get('auth_cache_filepath')
        api_credentials = Conf.get('api_credentials')

        self.cache_filepath = cache_filepath

        with NamedTemporaryFile() as f:
            json.dump(api_credentials, f)
            f.flush()

            self.flow = flow_from_clientsecrets(f.name, 
                                                scope=self.__get_scopes(), 
                                                redirect_uri=OOB_CALLBACK_URN)
        
        #self.flow.scope = self.__get_scopes()
        #self.flow.redirect_uri = OOB_CALLBACK_URN

    def __get_scopes(self):
        scopes = "https://www.googleapis.com/auth/drive https://www.googleapis.com/auth/drive.file"
               #'https://www.googleapis.com/auth/userinfo.email '
               #'https://www.googleapis.com/auth/userinfo.profile')
        return scopes

    def step1_get_auth_url(self):
        try:
            return self.flow.step1_get_authorize_url()
        except (Exception) as e:
            self.__log.exception("Could not get authorization URL: %s" % (e))
            raise        

    def __clear_cache(self):
        try:
            remove(self.cache_filepath)
        except:
            pass
    
    def __refresh_credentials(self):
        self.__log.info("Doing credentials refresh.")

        http = Http()

        try:
            self.credentials.refresh(http)
        except (Exception) as e:
            message = "Could not refresh credentials."

            self.__log.exception(message)
            raise AuthorizationFailureError(message)

        try:
            self.__update_cache(self.credentials)
        except:
            self.__log.exception("Could not update cache. We've nullified the "
                              "in-memory credentials.")
            raise
            
        self.__log.info("Credentials have been refreshed.")
            
    def __step2_check_auth_cache(self):
        # Attempt to read cached credentials.
        
        if self.credentials != None:
            return self.credentials
        
        self.__log.info("Checking for cached credentials.")

        try:
            with open(self.cache_filepath, 'r') as cache:
                credentials_serialized = cache.read()
        except:
            return None

        # If we're here, we have serialized credentials information.

        self.__log.info("Raw credentials retrieved from cache.")
        
        try:
            credentials = pickle.loads(credentials_serialized)
        except:
            # We couldn't decode the credentials. Kill the cache.
            self.__clear_cache()

            self.__log.exception("Could not deserialize credentials. Ignoring.")
            return None

        self.credentials = credentials
            
        # Credentials restored. Check expiration date.
            
        self.__log.info("Cached credentials found with expire-date [%s]." % 
                     (credentials.token_expiry.strftime('%Y%m%d-%H%M%S')))
        
        self.credentials = credentials

        self.check_credential_state()
        
        return self.credentials

    def check_credential_state(self):
        """Do all of the regular checks necessary to keep our access going, 
        such as refreshing when we expire.
        """
        if(datetime.today() >= self.credentials.token_expiry):
            self.__log.info("Credentials have expired. Attempting to refresh "
                         "them.")
            
            self.__refresh_credentials()
            return self.credentials

    def get_credentials(self):
        try:
            self.credentials = self.__step2_check_auth_cache()
        except:
            message = "Could not check cache for credentials."

            self.__log.exception(message)
            raise AuthorizationFailureError(message)
    
        if self.credentials == None:
            message = "No credentials were established from the cache."

            self.__log.exception(message)
            raise AuthorizationFaultError(message)

        return self.credentials
    
    def __update_cache(self, credentials):

        # Serialize credentials.

        self.__log.info("Serializing credentials for cache.")

        credentials_serialized = None
        
        try:
            credentials_serialized = pickle.dumps(credentials)
        except:
            self.__log.exception("Could not serialize credentials.")
            raise

        # Write cache file.

        self.__log.info("Writing credentials to cache.")

        try:
            with open(self.cache_filepath, 'w') as cache:
                cache.write(credentials_serialized)
        except:
            self.__log.exception("Could not write credentials to cache.")
            raise

    def step2_doexchange(self, auth_code):
        # Do exchange.

        self.__log.info("Doing exchange.")
        
        try:
            credentials = self.flow.step2_exchange(auth_code)
        except:
            message = "Could not do auth-exchange (this was either a legitimate" \
                      " error, or the auth-exchange was attempted when not " \
                      "necessary)."

            self.__log.exception(message)
            raise AuthorizationFailureError(message)
        
        self.__log.info("Credentials established.")

        try:
            self.__update_cache(credentials)
        except:
            self.__log.exception("Could not update cache. Process cancelled.")
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

