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

_logger = logging.getLogger(__name__)
_logger.setLevel(logging.INFO)


class _OauthAuthorize(object):
    """Manages authorization process."""

    def __init__(self):
        cache_filepath  = Conf.get('auth_cache_filepath')
        api_credentials = Conf.get('api_credentials')

        self.cache_filepath = cache_filepath
        self.credentials = None

        with NamedTemporaryFile() as f:
            json.dump(api_credentials, f)
            f.flush()

            self.flow = flow_from_clientsecrets(f.name, 
                                                scope=self.__get_scopes(), 
                                                redirect_uri=OOB_CALLBACK_URN)
        
    def __get_scopes(self):
        scopes = "https://www.googleapis.com/auth/drive "\
                 "https://www.googleapis.com/auth/drive.file"
        return scopes

    def step1_get_auth_url(self):
        return self.flow.step1_get_authorize_url()

    def __clear_cache(self):
        if self.cache_filepath is not None:
            try:
                remove(self.cache_filepath)
            except:
                pass
    
    def __refresh_credentials(self):
        _logger.info("Doing credentials refresh.")

        http = Http()

        try:
            self.credentials.refresh(http)
        except:
            raise AuthorizationFailureError("Could not refresh credentials.")

        self.__update_cache(self.credentials)
            
        _logger.debug("Credentials have been refreshed.")
            
    def __step2_check_auth_cache(self):
        # Attempt to read cached credentials.

        if self.cache_filepath is None:
            raise ValueError("Credentials file-path is not set.")

        if self.credentials is None:
            _logger.debug("Checking for cached credentials: %s",
                          self.cache_filepath)

            with open(self.cache_filepath) as cache:
                credentials_serialized = cache.read()

            # If we're here, we have serialized credentials information.
            
            try:
                credentials = pickle.loads(credentials_serialized)
            except:
                # We couldn't decode the credentials. Kill the cache.
                self.__clear_cache()
                raise

            self.credentials = credentials
                
            # Credentials restored. Check expiration date.

            expiry_phrase = self.credentials.token_expiry.strftime(
                                '%Y%m%d-%H%M%S')
                
            _logger.debug("Cached credentials found with expire-date [%s].",
                          expiry_phrase)
            
            self.check_credential_state()

        return self.credentials

    def check_credential_state(self):
        """Do all of the regular checks necessary to keep our access going, 
        such as refreshing when we expire.
        """
        if(datetime.today() >= self.credentials.token_expiry):
            _logger.info("Credentials have expired. Attempting to refresh "
                         "them.")
            
            self.__refresh_credentials()
            return self.credentials

    def get_credentials(self):
        return self.__step2_check_auth_cache()
    
    def __update_cache(self, credentials):
        if self.cache_filepath is None:
            raise ValueError("Credentials file-path is not set.")

        # Serialize credentials.

        credentials_serialized = pickle.dumps(credentials)

        # Write cache file.

        with open(self.cache_filepath, 'w') as cache:
            cache.write(credentials_serialized)

    def step2_doexchange(self, auth_code):
        # Do exchange.

        _logger.debug("Doing exchange.")
        
        try:
            credentials = self.flow.step2_exchange(auth_code)
        except Exception as e:
            message = \
                "Could not do auth-exchange (this was either a legitimate "\
                "error, or the auth-exchange was attempted when not "\
                "necessary): {}".format(e)

            raise AuthorizationFailureError(message)
        
        _logger.debug("Credentials established.")

        self.__update_cache(credentials)
        self.credentials = credentials

oauth = None
def get_auth():
    global oauth
    if oauth is None:
        _logger.debug("Creating OauthAuthorize.")
        oauth = _OauthAuthorize()
    
    return oauth

