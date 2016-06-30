import logging
import re
import dateutil.parser
import random
import json
import time
import httplib
import ssl
import tempfile
import pprint
import functools
import threading
import os

import httplib2

import apiclient.discovery
import apiclient.http
import apiclient.errors

import gdrivefs.constants
import gdrivefs.config
import gdrivefs.conf
import gdrivefs.gdtool.chunked_download
import gdrivefs.errors
import gdrivefs.gdtool.oauth_authorize
import gdrivefs.gdtool.normal_entry
import gdrivefs.time_support
import gdrivefs.gdfs.fsutility

_CONF_SERVICE_NAME = 'drive'
_CONF_SERVICE_VERSION = 'v2'

_MAX_EMPTY_CHUNKS = 3
_DEFAULT_UPLOAD_CHUNK_SIZE_B = 1024 * 1024

logging.getLogger('apiclient.discovery').setLevel(logging.WARNING)

_logger = logging.getLogger(__name__)

def _marshall(f):
    """A method wrapper that will reauth and/or reattempt where reasonable.
    """

    auto_refresh = True

    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        # Now, try to invoke the mechanism. If we succeed, return 
        # immediately. If we get an authorization-fault (a resolvable 
        # authorization problem), fall through and attempt to fix it. Allow 
        # any other error to bubble up.
        
        for n in range(0, 5):
            try:
                return f(*args, **kwargs)
            except (ssl.SSLError, httplib.BadStatusLine) as e:
                # These happen sporadically. Use backoff.
                _logger.exception("There was a transient connection "
                                  "error (%s). Trying again [%s]: %s",
                                  e.__class__.__name__, str(e), n)

                time.sleep((2 ** n) + random.randint(0, 1000) / 1000)
            except apiclient.errors.HttpError as e:
                if e.content == '':
                    raise

                try:
                    error = json.loads(e.content)
                except ValueError:
                    _logger.error("Non-JSON error while doing chunked "
                                  "download: [%s]", e.content) 
                    raise e

                if error.get('code') == 403 and \
                   error.get('errors')[0].get('reason') \
                        in ['rateLimitExceeded', 'userRateLimitExceeded']:
                    # Apply exponential backoff.
                    _logger.exception("There was a transient HTTP "
                                      "error (%s). Trying again (%d): "
                                      "%s",
                                      e.__class__.__name__, str(e), n)

                    time.sleep((2 ** n) + random.randint(0, 1000) / 1000)
                else:
                    # Other error, re-raise.
                    raise
            except gdrivefs.errors.AuthorizationFaultError:
                # If we're not allowed to refresh the token, or we've
                # already done it in the last attempt.
                if not auto_refresh or n == 1:
                    raise

                # We had a resolvable authorization problem.

                _logger.info("There was an authorization fault under "
                             "action [%s]. Attempting refresh.", action)
                
                authorize = gdrivefs.gdtool.oauth_authorize.get_auth()
                authorize.check_credential_state()

                # Re-attempt the action.

                _logger.info("Refresh seemed successful. Reattempting "
                             "action [%s].", action)

    return wrapper


class GdriveAuth(object):
    def __init__(self):
        self.__client = None
        self.__authorize = gdrivefs.gdtool.oauth_authorize.get_auth()
        self.__check_authorization()
        self.__http = None

    def __check_authorization(self):
        self.__credentials = self.__authorize.get_credentials()

    def get_authed_http(self):
        if self.__http is None:
            self.__check_authorization()
            _logger.debug("Getting authorized HTTP tunnel.")
                
            http = httplib2.Http()
            self.__credentials.authorize(http)

            _logger.debug("Got authorized tunnel.")

            self.__http = http

        return self.__http

    def get_client(self):
        if self.__client is None:
            authed_http = self.get_authed_http()
        
            # Build a client from the passed discovery document path
            
            discoveryUrl = \
                gdrivefs.conf.Conf.get('google_discovery_service_url')
# TODO: We should cache this, since we have, so often, had a problem 
#       retrieving it. If there's no other way, grab it directly, and then pass
#       via a file:// URI.
        
            try:
                client = \
                    apiclient.discovery.build(
                        _CONF_SERVICE_NAME, 
                        _CONF_SERVICE_VERSION, 
                        http=authed_http, 
                        discoveryServiceUrl=discoveryUrl)
            except apiclient.errors.HttpError as e:
                # We've seen situations where the discovery URL's server is down,
                # with an alternate one to be used.
                #
                # An error here shouldn't leave GDFS in an unstable state (the 
                # current command should just fail). Hoepfully, the failure is 
                # momentary, and the next command succeeds.

                _logger.exception("There was an HTTP response-code of (%d) while "
                                  "building the client with discovery URL [%s].",
                                  e.resp.status, discoveryUrl)
                raise

            self.__client = client

        return self.__client


class _GdriveManager(object):
    """Handles all basic communication with Google Drive. All methods should
    try to invoke only one call, or make sure they handle authentication 
    refreshing when necessary.
    """

    def __init__(self):
        self.__auth = GdriveAuth()

    def __assert_response_kind(self, response, expected_kind):
        actual_kind = response[u'kind']
        if actual_kind != unicode(expected_kind):
            raise ValueError("Received response of type [%s] instead of "
                             "[%s]." % (actual_kind, expected_kind))

    @_marshall
    def get_about_info(self):
        """Return the 'about' information for the drive."""

        client = self.__auth.get_client()
        response = client.about().get().execute()
        self.__assert_response_kind(response, 'drive#about')

        return response

    @_marshall
    def list_changes(self, start_change_id=None, page_token=None):
        """Get a list of the most recent changes from GD, with the earliest 
        changes first. This only returns one page at a time. start_change_id 
        doesn't have to be valid.. It's just the lower limit to what you want 
        back. Change-IDs are integers, but are not necessarily sequential.
        """

        client = self.__auth.get_client()

        response = client.changes().list(
                    pageToken=page_token, 
                    startChangeId=start_change_id).execute()

        self.__assert_response_kind(response, 'drive#changeList')

        items = response[u'items']

        if items:
            _logger.debug("We received (%d) changes to apply.", len(items))

        largest_change_id = int(response[u'largestChangeId'])
        next_page_token = response.get(u'nextPageToken')

        changes = []
        last_change_id = None
        for item in items:
            change_id = int(item[u'id'])
            entry_id = item[u'fileId']

            if item[u'deleted']:
                was_deleted = True
                entry = None

                _logger.debug("CHANGE: [%s] (DELETED)", entry_id)
            else:
                was_deleted = False
                entry = item[u'file']

                _logger.debug("CHANGE: [%s] [%s] (UPDATED)", 
                              entry_id, entry[u'title'])

            if was_deleted:
                normalized_entry = None
            else:
                normalized_entry = \
                    gdrivefs.gdtool.normal_entry.NormalEntry(
                        'list_changes', 
                        entry)

            changes.append((change_id, (entry_id, was_deleted, normalized_entry)))
            last_change_id = change_id

        return (largest_change_id, next_page_token, changes)

    @_marshall
    def get_parents_containing_id(self, child_id, max_results=None):
        
        _logger.info("Getting client for parent-listing.")

        client = self.__auth.get_client()

        _logger.info("Listing entries over child with ID [%s].", child_id)

        response = client.parents().list(fileId=child_id).execute()
        self.__assert_response_kind(response, 'drive#parentList')

        return [ entry[u'id'] for entry in response[u'items'] ]

    @_marshall
    def get_children_under_parent_id(self,
                                     parent_id,
                                     query_contains_string=None,
                                     query_is_string=None,
                                     max_results=None):

        _logger.info("Getting client for child-listing.")

        client = self.__auth.get_client()

        assert \
            (query_contains_string is not None and \
             query_is_string is not None) is False, \
            "The query_contains_string and query_is_string parameters are "\
            "mutually exclusive."

        if query_is_string:
            query = ("title='%s'" % 
                     (gdrivefs.gdfs.fsutility.escape_filename_for_query(query_is_string)))
        elif query_contains_string:
            query = ("title contains '%s'" % 
                     (gdrivefs.gdfs.fsutility.escape_filename_for_query(query_contains_string)))
        else:
            query = None

        _logger.info("Listing entries under parent with ID [%s].  QUERY= "
                     "[%s]", parent_id, query)

        response = client.children().list(
                    q=query, 
                    folderId=parent_id,
                    maxResults=max_results).execute()

        self.__assert_response_kind(response, 'drive#childList')

        return [ entry[u'id'] for entry in response[u'items'] ]

    @_marshall
    def get_entries(self, entry_ids):
        retrieved = { }
        for entry_id in entry_ids:
            retrieved[entry_id] = self.get_entry(entry_id)

        _logger.debug("(%d) entries were retrieved.", len(retrieved))

        return retrieved

    @_marshall
    def get_entry(self, entry_id):
        client = self.__auth.get_client()

        response = client.files().get(fileId=entry_id).execute()
        self.__assert_response_kind(response, 'drive#file')

        return \
            gdrivefs.gdtool.normal_entry.NormalEntry('direct_read', response)

    @_marshall
    def list_files(self, query_contains_string=None, query_is_string=None, 
                   parent_id=None):
        
        _logger.info("Listing all files. CONTAINS=[%s] IS=[%s] "
                     "PARENT_ID=[%s]",
                     query_contains_string 
                        if query_contains_string is not None 
                        else '<none>', 
                     query_is_string 
                        if query_is_string is not None 
                        else '<none>', 
                     parent_id 
                        if parent_id is not None 
                        else '<none>')

        client = self.__auth.get_client()

        query_components = []

        if parent_id:
            query_components.append("'%s' in parents" % (parent_id))

        if query_is_string:
            query_components.append("title='%s'" % 
                                    (gdrivefs.gdfs.fsutility.escape_filename_for_query(query_is_string)))
        elif query_contains_string:
            query_components.append("title contains '%s'" % 
                                    (gdrivefs.gdfs.fsutility.escape_filename_for_query(query_contains_string)))

        # Make sure that we don't get any entries that we would have to ignore.

        hidden_flags = gdrivefs.conf.Conf.get('hidden_flags_list_remote')
        if hidden_flags:
            for hidden_flag in hidden_flags:
                query_components.append("%s = false" % (hidden_flag))

        query = ' and '.join(query_components) if query_components else None

        page_token = None
        page_num = 0
        entries = []
        while 1:
            _logger.debug("Doing request for listing of files with page-"
                          "token [%s] and page-number (%d): %s",
                          page_token, page_num, query)

            result = client.files().list(q=query, pageToken=page_token).\
                        execute()

            self.__assert_response_kind(result, 'drive#fileList')

            _logger.debug("(%d) entries were presented for page-number "
                          "(%d).", len(result[u'items']), page_num)

            for entry_raw in result[u'items']:
                entry = \
                    gdrivefs.gdtool.normal_entry.NormalEntry(
                        'list_files', 
                        entry_raw)

                entries.append(entry)

            if u'nextPageToken' not in result:
                _logger.debug("No more pages in file listing.")
                break

            _logger.debug("Next page-token in file-listing is [%s].", 
                          result[u'nextPageToken'])

            page_token = result[u'nextPageToken']
            page_num += 1

        return entries

    @_marshall
    def download_to_local(self, output_file_path, normalized_entry, 
                          mime_type=None, allow_cache=True):
        """Download the given file. If we've cached a previous download and the 
        mtime hasn't changed, re-use. The third item returned reflects whether 
        the data has changed since any prior attempts.
        """

        _logger.info("Downloading entry with ID [%s] and mime-type [%s] to "
                     "[%s].", normalized_entry.id, mime_type, output_file_path)

        if mime_type is None:
            if normalized_entry.mime_type in normalized_entry.download_links:
                mime_type = normalized_entry.mime_type

                _logger.debug("Electing file mime-type for download: [%s]", 
                              normalized_entry.mime_type)
            elif gdrivefs.constants.OCTET_STREAM_MIMETYPE \
                    in normalized_entry.download_links:
                mime_type = gdrivefs.constants.OCTET_STREAM_MIMETYPE

                _logger.debug("Electing octet-stream for download.")
            else:
                raise ValueError("Could not determine what to fallback to for "
                                 "the mimetype: {}".format(
                                 normalized_entry.mime_type))

        if mime_type != normalized_entry.mime_type and \
                mime_type not in normalized_entry.download_links:
            message = ("Entry with ID [%s] can not be exported to type [%s]. "
                       "The available types are: %s" % 
                       (normalized_entry.id, mime_type, 
                        ', '.join(normalized_entry.download_links.keys())))

            _logger.warning(message)
            raise gdrivefs.errors.ExportFormatError(message)

        gd_mtime_epoch = time.mktime(
                            normalized_entry.modified_date.timetuple())

        _logger.info("File will be downloaded to [%s].", output_file_path)

        use_cache = False
        if allow_cache and os.path.isfile(output_file_path):
            # Determine if a local copy already exists that we can use.
            stat_info = os.stat(output_file_path)

            if gd_mtime_epoch == stat_info.st_mtime:
                use_cache = True

        if use_cache:
            # Use the cache. It's fine.

            _logger.info("File retrieved from the previously downloaded, "
                         "still-current file.")

            return (stat_info.st_size, False)

        # Go and get the file.

        authed_http = self.__auth.get_authed_http()

        url = normalized_entry.download_links[mime_type]

        with open(output_file_path, 'wb') as f:
            downloader = gdrivefs.gdtool.chunked_download.ChunkedDownload(
                            f, 
                            authed_http, 
                            url)

            progresses = []

            while 1:
                status, done, total_size = downloader.next_chunk()
                assert status.total_size is not None, \
                       "total_size is None"

                _logger.debug("Read chunk: STATUS=[%s] DONE=[%s] "
                              "TOTAL_SIZE=[%s]", status, done, total_size)

                if status.total_size > 0:
                    percent = status.progress()
                else:
                    percent = 100.0

                _logger.debug("Chunk: PROGRESS=[%s] TOTAL-SIZE=[%s] "
                              "RESUMABLE-PROGRESS=[%s]",
                              percent, status.total_size, 
                              status.resumable_progress)

# TODO(dustin): This just places an arbitrary limit on the number of empty 
#               chunks we can receive. Can we drop this to 1?
                if len(progresses) >= _MAX_EMPTY_CHUNKS:
                    assert percent > progresses[0], \
                           "Too many empty chunks have been received."

                progresses.append(percent)

                # Constrain how many percentages we keep.
                if len(progresses) > _MAX_EMPTY_CHUNKS:
                    del progresses[0]

                if done is True:
                    break

            _logger.debug("Download complete. Offset is: (%d)", f.tell())

        os.utime(output_file_path, (time.time(), gd_mtime_epoch))

        return (total_size, True)

    @_marshall
    def create_directory(self, filename, parents, **kwargs):

        mimetype_directory = gdrivefs.conf.Conf.get('directory_mimetype')
        return self.__insert_entry(
                False,
                filename, 
                parents,
                mimetype_directory, 
                **kwargs)

    @_marshall
    def create_file(self, filename, parents, mime_type, data_filepath=None, 
                    **kwargs):
# TODO: It doesn't seem as if the created file is being registered.
        # Even though we're supposed to provide an extension, we can get away 
        # without having one. We don't want to impose this when acting like a 
        # normal FS.

        return self.__insert_entry(
                True,
                filename,
                parents,
                mime_type,
                data_filepath=data_filepath,
                **kwargs)

    @_marshall
    def __insert_entry(self, is_file, filename, parents, mime_type, 
                       data_filepath=None, modified_datetime=None, 
                       accessed_datetime=None, is_hidden=False, 
                       description=None):

        if parents is None:
            parents = []

        now_phrase = gdrivefs.time_support.get_flat_normal_fs_time_from_dt()

        if modified_datetime is None:
            modified_datetime = now_phrase 
    
        if accessed_datetime is None:
            accessed_datetime = now_phrase 

        _logger.info("Creating entry with filename [%s] under parent(s) "
                     "[%s] with mime-type [%s]. MTIME=[%s] ATIME=[%s] "
                     "DATA_FILEPATH=[%s]",
                     filename, ', '.join(parents), mime_type, 
                     modified_datetime, accessed_datetime, data_filepath)

        client = self.__auth.get_client()

        ## Create request-body.

        body = { 
                'title': filename, 
                'parents': [dict(id=parent) for parent in parents], 
                'labels': { "hidden": is_hidden }, 
                'mimeType': mime_type,
            }

        if description is not None:
            body['description'] = description

        if modified_datetime is not None:
            body['modifiedDate'] = modified_datetime

        if accessed_datetime is not None:
            body['lastViewedByMeDate'] = accessed_datetime

        ## Create request-arguments.

        args = {
            'body': body,
        }

        if data_filepath:
            args.update({
                'media_body': 
                    apiclient.http.MediaFileUpload(
                        data_filepath, 
                        mimetype=mime_type, 
                        resumable=True,
                        chunksize=_DEFAULT_UPLOAD_CHUNK_SIZE_B),
# TODO(dustin): Documented, but does not exist.
#                'uploadType': 'resumable',
            })

        if gdrivefs.config.IS_DEBUG is True:
            _logger.debug("Doing file-insert with:\n%s", 
                          pprint.pformat(args))

        request = client.files().insert(**args)

        response = self.__finish_upload(
                    filename,
                    request,
                    data_filepath is not None)

        self.__assert_response_kind(response, 'drive#file')

        normalized_entry = \
            gdrivefs.gdtool.normal_entry.NormalEntry(
                'insert_entry', 
                response)

        _logger.info("New entry created with ID [%s].", normalized_entry.id)

        return normalized_entry

    @_marshall
    def truncate(self, normalized_entry):

        _logger.info("Truncating entry [%s].", normalized_entry.id)

        client = self.__auth.get_client()

        file_ = \
            apiclient.http.MediaFileUpload(
                '/dev/null',
                mimetype=normalized_entry.mime_type)

        args = { 
            'fileId': normalized_entry.id, 
# TODO(dustin): Can we omit 'body'?
            'body': {}, 
            'media_body': file_,
        }

        response = client.files().update(**args).execute()
        self.__assert_response_kind(response, 'drive#file')

        _logger.debug("Truncate complete: [%s]", normalized_entry.id)

        return response

    @_marshall
    def update_entry(self, normalized_entry, filename=None, data_filepath=None, 
                     mime_type=None, parents=None, modified_datetime=None, 
                     accessed_datetime=None, is_hidden=False, 
                     description=None):

        _logger.info("Updating entry [%s].", normalized_entry)

        client = self.__auth.get_client()

        # Build request-body.
        
        body = {}

        if mime_type is None:
            mime_type = normalized_entry.mime_type

        body['mimeType'] = mime_type 

        if filename is not None:
            body['title'] = filename
        
        if parents is not None:
            body['parents'] = parents

        if is_hidden is not None:
            body['labels'] = { "hidden": is_hidden }

        if description is not None:
            body['description'] = description

        set_mtime = True
        if modified_datetime is not None:
            body['modifiedDate'] = modified_datetime
        else:
            body['modifiedDate'] = \
                gdrivefs.time_support.get_flat_normal_fs_time_from_dt()

        if accessed_datetime is not None:
            set_atime = True
            body['lastViewedByMeDate'] = accessed_datetime
        else:
            set_atime = False

        # Build request-arguments.

        args = { 
            'fileId': normalized_entry.id, 
            'body': body, 
            'setModifiedDate': set_mtime, 
            'updateViewedDate': set_atime,
        }

        if data_filepath is not None:
            _logger.debug("We'll be sending a file in the update: [%s] [%s]", 
                          normalized_entry.id, data_filepath)

            # We can only upload large files using resumable-uploads.
            args.update({
                'media_body': 
                    apiclient.http.MediaFileUpload(
                        data_filepath, 
                        mimetype=mime_type, 
                        resumable=True,
                        chunksize=_DEFAULT_UPLOAD_CHUNK_SIZE_B),
# TODO(dustin): Documented, but does not exist.
#                'uploadType': 'resumable',
            })

        _logger.debug("Sending entry update: [%s]", normalized_entry.id)

        request = client.files().update(**args)

        result = self.__finish_upload(
                    normalized_entry.title,
                    request,
                    data_filepath is not None)

        normalized_entry = \
            gdrivefs.gdtool.normal_entry.NormalEntry('update_entry', result)

        _logger.debug("Entry updated: [%s]", normalized_entry)

        return normalized_entry

    def __finish_upload(self, filename, request, has_file):
        """Finish a resumable-upload is a file was given, or just execute the 
        request if not.
        """

        if has_file is False:
            return request.execute()

        _logger.debug("We need to finish updating the entry's data: [%s]", 
                      filename)

        result = None
        while result is None:
            status, result = request.next_chunk()

            if status:
                if status.total_size == 0:
                    _logger.debug("Uploaded (zero-length): [%s]", filename)
                else:
                    _logger.debug("Uploaded [%s]: %.2f%%", 
                                  filename, status.progress() * 100)

        return result

    @_marshall
    def rename(self, normalized_entry, new_filename):

        result = gdrivefs.gdfs.fsutility.split_path_nolookups(new_filename)
        (path, filename_stripped, mime_type, is_hidden) = result

        _logger.debug("Renaming entry [%s] to [%s]. IS_HIDDEN=[%s]",
                      normalized_entry, filename_stripped, is_hidden)

        return self.update_entry(normalized_entry, filename=filename_stripped, 
                                 is_hidden=is_hidden)

    @_marshall
    def remove_entry(self, normalized_entry):

        _logger.info("Removing entry with ID [%s].", normalized_entry.id)

        client = self.__auth.get_client()

        args = { 'fileId': normalized_entry.id }

        try:
            result = client.files().delete(**args).execute()
        except Exception as e:
            if e.__class__.__name__ == 'HttpError' and \
               str(e).find('File not found') != -1:
                raise NameError(normalized_entry.id)

            _logger.exception("Could not send delete for entry with ID [%s].",
                              normalized_entry.id)
            raise

        _logger.info("Entry deleted successfully.")

_THREAD_STORAGE = None
def get_gdrive():
    """Return an instance of _GdriveManager unique to each thread (we can't 
    reuse sockets between threads).
    """

    global _THREAD_STORAGE

    if _THREAD_STORAGE is None:
        _THREAD_STORAGE = threading.local()

    try:
        return _THREAD_STORAGE.gm
    except AttributeError:
        _THREAD_STORAGE.gm = _GdriveManager()
        return _THREAD_STORAGE.gm
