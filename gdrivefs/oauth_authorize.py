import os
import logging
import pickle
import json
import datetime
import tempfile

import oauth2client.client

import httplib2

import gdrivefs.conf
import gdrivefs.errors

_LOGGER = logging.getLogger(__name__)
_LOGGER.setLevel(logging.INFO)


class OauthAuthorize(object):
    """Manages authorization process."""

    def __init__(
            self, redirect_uri=oauth2client.client.OOB_CALLBACK_URN):
        creds_filepath  = gdrivefs.conf.Conf.get('auth_cache_filepath')

        assert \
            creds_filepath is not None, \
            "Credentials file-path not set."

        creds_path = os.path.dirname(creds_filepath)
        if creds_path != '' and \
           os.path.exists(creds_path) is False:
            os.makedirs(creds_path)

        self.__creds_filepath = creds_filepath
        self.__credentials = None

        api_credentials = gdrivefs.conf.Conf.get('api_credentials')

        with tempfile.NamedTemporaryFile(mode='w+') as f:
            json.dump(api_credentials, f)
            f.flush()

            self.flow = \
                oauth2client.client.flow_from_clientsecrets(
                    f.name,
                    scope=self.__get_scopes(),
                    redirect_uri=redirect_uri)

    def __get_scopes(self):
        scopes = [
            "https://www.googleapis.com/auth/drive",
            "https://www.googleapis.com/auth/drive.file",
        ]

        return ' '.join(scopes)

    def step1_get_auth_url(self):
        return self.flow.step1_get_authorize_url()

    def __clear_cache(self):
        if self.__creds_filepath is not None:
            try:
                os.remove(self.__creds_filepath)
            except:
                pass

    def __refresh_credentials(self):
        _LOGGER.debug("Doing credentials refresh.")

        http = httplib2.Http()

        try:
            self.__credentials.refresh(http)
        except:
            raise gdrivefs.errors.AuthorizationFailureError("Could not refresh credentials.")

        self.__update_cache(self.__credentials)

        _LOGGER.debug("Credentials have been refreshed.")

    def __step2_check_auth_cache(self):
        # Attempt to read cached credentials.

        if self.__creds_filepath is None:
            raise ValueError("Credentials file-path is not set.")

        if self.__credentials is None:
            _LOGGER.debug("Checking for cached credentials: %s",
                          self.__creds_filepath)

            # If we're here, we have serialized credentials information.

            try:
                with open(self.__creds_filepath, 'rb') as f:
                    credentials = pickle.load(f)
            except:
                # We couldn't decode the credentials. Kill the cache.
                self.__clear_cache()
                raise

            self.__credentials = credentials

            # Credentials restored. Check expiration date.

            expiry_phrase = self.__credentials.token_expiry.strftime(
                                '%Y%m%d-%H%M%S')

            _LOGGER.debug("Cached credentials found with expire-date [%s].",
                          expiry_phrase)

            self.check_credential_state()

        return self.__credentials

    def check_credential_state(self):
        """Do all of the regular checks necessary to keep our access going,
        such as refreshing when we expire.
        """
        if(datetime.datetime.today() >= self.__credentials.token_expiry):
            _LOGGER.debug("Credentials have expired. Attempting to refresh "
                          "them.")

            self.__refresh_credentials()
            return self.__credentials

    def get_credentials(self):
        return self.__step2_check_auth_cache()

    def __update_cache(self, credentials):
        if self.__creds_filepath is None:
            raise ValueError("Credentials file-path is not set.")

        with open(self.__creds_filepath, 'wb') as f:
            pickle.dump(credentials, f)

        os.chmod(self.__creds_filepath, 0o600)

    def step2_doexchange(self, auth_code):
        # Do exchange.

        _LOGGER.debug("Doing exchange.")

        credentials = self.flow.step2_exchange(auth_code)

        _LOGGER.debug("Credentials established.")

        self.__update_cache(credentials)
        self.__credentials = credentials

# A singleton, for general use.

oauth = None
def get_auth():
    global oauth
    if oauth is None:
        _LOGGER.debug("Creating OauthAuthorize.")
        oauth = OauthAuthorize()

    return oauth
