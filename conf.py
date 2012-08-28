class Conf(object):
    """Manages options."""

    auth_temp_path                      = '/var/cache/gdfs'
    auth_cache_filename                 = 'credcache'
    auth_secrets_filepath               = '/etc/gdfs/client_secrets.json'
    gd_to_normal_mapping_filepath       = '/etc/gdfs/mime_mapping.json'
    extension_mapping_filepath          = '/etc/gdfs/extension_mapping.json'
    change_check_interval_s             = .5
    query_decay_intermed_prefix_length  = 7

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
            raise Exception

    @staticmethod
    def set(key, value):
        setattr(Conf, key, value)

