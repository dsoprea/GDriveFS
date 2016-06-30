import logging
import re
import dateutil.parser
import json
import time
import pprint

from time import mktime
from mimetypes import guess_type
from numbers import Number
from datetime import datetime

from gdrivefs.conf import Conf
from gdrivefs.utility import utility
from gdrivefs.errors import ExportFormatError
from gdrivefs.time_support import get_flat_normal_fs_time_from_dt

_logger = logging.getLogger(__name__)


class NormalEntry(object):
    __directory_mimetype = Conf.get('directory_mimetype')

    __properties_extra = [
        'is_directory', 
        'is_visible', 
        'parents', 
        'download_types',
        'modified_date',
        'modified_date_epoch',
        'mtime_byme_date',
        'mtime_byme_date_epoch',
        'atime_byme_date',
        'atime_byme_date_epoch',
    ]

    def __init__(self, gd_resource_type, raw_data):
        self.__info = {}
        self.__parents = []
        self.__raw_data = raw_data
        self.__cache_data = None
        self.__cache_mimetypes = None
        self.__cache_dict = {}

        # Return True if reading from this file should return info and deposit 
        # the data elsewhere. This is predominantly determined by whether we 
        # can get a file-size up-front, or we have to decide on a specific 
        # mime-type in order to do so.

        requires_mimetype = u'fileSize' not in self.__raw_data and \
                            raw_data[u'mimeType'] != self.__directory_mimetype

        self.__info['requires_mimetype'] = \
            requires_mimetype
        
        self.__info['title'] = \
            raw_data[u'title']
        
        self.__info['mime_type'] = \
            raw_data[u'mimeType']
        
        self.__info['labels'] = \
            raw_data[u'labels']
        
        self.__info['id'] = \
            raw_data[u'id']
        
        self.__info['last_modifying_user_name'] = \
            raw_data[u'lastModifyingUserName']
        
        self.__info['writers_can_share'] = \
            raw_data[u'writersCanShare']

        self.__info['owner_names'] = \
            raw_data[u'ownerNames']
        
        self.__info['editable'] = \
            raw_data[u'editable']
        
        self.__info['user_permission'] = \
            raw_data[u'userPermission']

        self.__info['link'] = \
            raw_data.get(u'embedLink')
        
        self.__info['file_size'] = \
            int(raw_data.get(u'fileSize', 0))
        
        self.__info['file_extension'] = \
            raw_data.get(u'fileExtension')
        
        self.__info['md5_checksum'] = \
            raw_data.get(u'md5Checksum')
        
        self.__info['image_media_metadata'] = \
            raw_data.get(u'imageMediaMetadata')

        self.__info['download_links'] = \
            raw_data.get(u'exportLinks', {})

        try:
            self.__info['download_links'][self.__info['mime_type']] = \
                raw_data[u'downloadUrl']
        except KeyError:
            pass

        self.__update_display_name()

        for parent in raw_data[u'parents']:
            self.__parents.append(parent[u'id'])

    def __getattr__(self, key):
        return self.__info[key]

    def __str__(self):
        return ("<NORMAL ID= [%s] MIME= [%s] NAME= [%s] URIS= (%d)>" % 
                (self.id, self.mime_type, self.title, 
                 len(self.download_links)))

    def __repr__(self):
        return str(self)

    def __update_display_name(self):
        # This is encoded for displaying locally.
        self.__info['title_fs'] = utility.translate_filename_charset(self.__info['title'])

    def temp_rename(self, new_filename):
        """Set the name to something else, here, while we, most likely, wait 
        for the change at the server to propogate.
        """
    
        self.__info['title'] = new_filename
        self.__update_display_name()

    def normalize_download_mimetype(self, specific_mimetype=None):
        """If a mimetype is given, return it if there is a download-URL 
        available for it, or fail. Else, determine if a copy can downloaded 
        with the default mime-type (application/octet-stream, or something 
        similar), or return the only mime-type in the event that there's only 
        one download format.
        """

        if self.__cache_mimetypes is None:
            self.__cache_mimetypes = [[], None]
        
        if specific_mimetype is not None:
            if specific_mimetype not in self.__cache_mimetypes[0]:
                _logger.debug("Normalizing mime-type [%s] for download.  "
                              "Options: %s", 
                              specific_mimetype, self.download_types)

                if specific_mimetype not in self.download_links:
                    raise ExportFormatError("Mime-type [%s] is not available for "
                                            "download. Options: %s" % 
                                            (self.download_types))

                self.__cache_mimetypes[0].append(specific_mimetype)

            return specific_mimetype

        if self.__cache_mimetypes[1] is None:
            # Try to derive a mimetype from the filename, and see if it matches
            # against available export types.
            (mimetype_candidate, _) = guess_type(self.title_fs, True)
            if mimetype_candidate is not None and \
               mimetype_candidate in self.download_links:
                mime_type = mimetype_candidate

            # If there's only one download link, resort to using it (perhaps it was 
            # an uploaded file, assigned only one type).
            elif len(self.download_links) == 1:
                mime_type = self.download_links.keys()[0]

            else:
                raise ExportFormatError("A correct mime-type needs to be "
                                        "specified. Options: %s" % 
                                        (self.download_types))

            self.__cache_mimetypes[1] = mime_type

        return self.__cache_mimetypes[1]

    def __convert(self, data):
        if isinstance(data, dict):
            list_ = [("K(%s)=V(%s)" % (self.__convert(key), 
                                  self.__convert(value))) \
                     for key, value \
                     in data.iteritems()]

            final = '; '.join(list_)
            return final
        elif isinstance(data, list):
            final = ', '.join([('LI(%s)' % (self.__convert(element))) \
                               for element \
                               in data])
            return final
        elif isinstance(data, unicode):
            return utility.translate_filename_charset(data)
        elif isinstance(data, Number):
            return str(data)
        elif isinstance(data, datetime):
            return get_flat_normal_fs_time_from_dt(data)
        else:
            return data

    def get_data(self):
            original = dict([(key.encode('ASCII'), value) 
                                for key, value 
                                in self.__raw_data.iteritems()])

            distilled = self.__info

            extra = dict([(key, getattr(self, key)) 
                                for key 
                                in self.__properties_extra])

            data_dict = {'original': original,
                         #'distilled': distilled,
                         'extra': extra}

            return data_dict

    @property
    def xattr_data(self):
        if self.__cache_data is None:
            data_dict = self.get_data()
            
            attrs = {}
            for a_type, a_dict in data_dict.iteritems():
#                self.__log.debug("Setting [%s]." % (a_type))
                for key, value in a_dict.iteritems():
                    fqkey = ('user.%s.%s' % (a_type, key))
                    attrs[fqkey] = self.__convert(value)
 
            self.__cache_data = attrs

        return self.__cache_data

    @property
    def is_directory(self):
        """Return True if we represent a directory."""
        return (self.__info['mime_type'] == self.__directory_mimetype)

    @property
    def is_visible(self):
        if [ flag 
             for flag, value 
             in self.labels.items() 
             if flag in Conf.get('hidden_flags_list_local') and value ]:
            return False
        else:
            return True

    @property
    def parents(self):
        return self.__parents

    @property
    def download_types(self):
        return self.download_links.keys()

    @property
    def modified_date(self):
        if 'modified_date' not in self.__cache_dict:
            self.__cache_dict['modified_date'] = \
                dateutil.parser.parse(self.__raw_data[u'modifiedDate'])

        return self.__cache_dict['modified_date']

    @property
    def modified_date_epoch(self):
        # mktime() only works in terms of the local timezone, so compensate 
        # (this works with DST, too).
        return mktime(self.modified_date.timetuple()) - time.timezone
        
    @property  
    def mtime_byme_date(self):
        if 'modified_byme_date' not in self.__cache_dict:
            self.__cache_dict['modified_byme_date'] = \
                dateutil.parser.parse(self.__raw_data[u'modifiedByMeDate'])

        return self.__cache_dict['modified_byme_date']

    @property
    def mtime_byme_date_epoch(self):
        return mktime(self.mtime_byme_date.timetuple()) - time.timezone

    @property
    def atime_byme_date(self):
        if 'viewed_byme_date' not in self.__cache_dict:
            self.__cache_dict['viewed_byme_date'] = \
                dateutil.parser.parse(self.__raw_data[u'lastViewedByMeDate']) \
                if u'lastViewedByMeDate' in self.__raw_data \
                else None

        return self.__cache_dict['viewed_byme_date']

    @property
    def atime_byme_date_epoch(self):
        return mktime(self.atime_byme_date.timetuple()) - time.timezone \
                if self.atime_byme_date \
                else None
