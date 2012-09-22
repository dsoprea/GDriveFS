import logging

class Conf(object):
    """Manages options."""

    auth_temp_path                      = '/var/cache/gdfs'
    auth_cache_filepath                 = None #'credcache'
    auth_secrets_filepath               = '/etc/gdfs/client_secrets.json'
    gd_to_normal_mapping_filepath       = '/etc/gdfs/mime_mapping.json'
    extension_mapping_filepath          = '/etc/gdfs/extension_mapping.json'
    change_check_interval_s             = .5
    query_decay_intermed_prefix_length  = 7
    file_jobthread_max_idle_time        = 60
    file_chunk_size_kb                  = 1024
    file_download_temp_path             = '/tmp/gdrivefs'
    file_download_temp_max_age_s        = 86400
    change_check_frequency_s            = 10
    hidden_flags_list_local             = [u'trashed', u'restricted']
    hidden_flags_list_remote            = [u'trashed']
    cache_cleanup_check_frequency_s     = 60
    cache_entries_max_age               = 8 * 60 * 60
    cache_status_post_frequency_s       = 10
    report_emit_frequency_s             = 60

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
            logging.error("Can not set invalid configuration item [%s]." % (key))
            raise KeyError(key)

        setattr(Conf, key, value)

