import logging
from apiclient.discovery import DISCOVERY_URI

class Conf(object):
    """Manages options."""

    api_credentials = {
        "web": { "client_id": "1056816309698.apps.googleusercontent.com",
                 "client_secret": "R7FJFlbtWXgUoG3ZjIAWUAzv",
                 "redirect_uris": [],
                 "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                 "token_uri": "https://accounts.google.com/o/oauth2/token"
               }}
    
    auth_temp_path                      = '/var/cache/gdfs'
    auth_cache_filepath                 = None #'credcache'
    gd_to_normal_mapping_filepath       = '/etc/gdfs/mime_mapping.json'
    extension_mapping_filepath          = '/etc/gdfs/extension_mapping.json'
    change_check_interval_s             = .5
    query_decay_intermed_prefix_length  = 7
    file_jobthread_max_idle_time        = 60
    file_chunk_size_kb                  = 1024
    file_download_temp_path             = '/tmp/gdrivefs'
    file_download_temp_max_age_s        = 86400
    file_default_mime_type              = 'application/octet-stream'
    change_check_frequency_s            = 10
    hidden_flags_list_local             = [u'trashed', u'restricted']
    hidden_flags_list_remote            = [u'trashed']
    cache_cleanup_check_frequency_s     = 60
    cache_entries_max_age               = 8 * 60 * 60
    cache_status_post_frequency_s       = 10
    report_emit_frequency_s             = 60
    google_discovery_service_url        = DISCOVERY_URI
    default_buffer_read_blocksize       = 65536
    default_mimetype                    = 'application/octet-stream'
    directory_mimetype                  = u'application/vnd.google-apps.folder'
    default_perm_folder                 = '777'
    default_perm_file_editable          = '666'
    default_perm_file_noneditable       = '444'

    max_readahead_entries = 10
    """How many extra entries to retrieve when an entry is accessed that is 
    not currently cached.
    """

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
        if key not in Conf.__dict__:
            raise KeyError(key)

        setattr(Conf, key, value)

