import logging
import re
import dateutil.parser

from time import mktime

from gdrivefs.conf import Conf
from gdrivefs.utility import get_utility


class NormalEntry(object):
    default_general_mime_type = 'application/octet-stream'

    def __init__(self, gd_resource_type, raw_data):
        # LESSONLEARNED: We had these set as properties, but CPython was 
        #                reusing the reference between objects.

        self.__log = logging.getLogger().getChild('NormalEntry')

        self.__info = {}
        self.__parents = []
        self.__raw_data = raw_data

        try:
            self.__info['mime_type']                  = raw_data[u'mimeType']
            self.__info['labels']                     = raw_data[u'labels']
            self.__info['id']                         = raw_data[u'id']
            self.__info['title']                      = raw_data[u'title']
            self.__info['last_modifying_user_name']   = raw_data[u'lastModifyingUserName']
            self.__info['writers_can_share']          = raw_data[u'writersCanShare']
            self.__info['owner_names']                = raw_data[u'ownerNames']
            self.__info['editable']                   = raw_data[u'editable']
            self.__info['user_permission']            = raw_data[u'userPermission']
            self.__info['modified_date']              = dateutil.parser.parse(raw_data[u'modifiedDate'])
            self.__info['modified_date_epoch']        = int(mktime(self.__info['modified_date'].timetuple()))
            self.__info['created_date']               = dateutil.parser.parse(raw_data[u'createdDate'])
            self.__info['created_date_epoch']         = int(mktime(self.__info['created_date'].timetuple()))

            self.__info['download_links']         = raw_data[u'exportLinks']          if u'exportLinks'           in raw_data else { }
            self.__info['link']                   = raw_data[u'embedLink']            if u'embedLink'             in raw_data else None
            self.__info['modified_by_me_date']    = raw_data[u'modifiedByMeDate']     if u'modifiedByMeDate'      in raw_data else None
            self.__info['last_viewed_by_me_date'] = raw_data[u'lastViewedByMeDate']   if u'lastViewedByMeDate'    in raw_data else None
            self.__info['file_size']              = int(raw_data[u'fileSize'])        if u'fileSize'              in raw_data else 0
            self.__info['file_extension']         = raw_data[u'fileExtension']        if u'fileExtension'         in raw_data else None
            self.__info['md5_checksum']           = raw_data[u'md5Checksum']          if u'md5Checksum'           in raw_data else None
            self.__info['image_media_metadata']   = raw_data[u'imageMediaMetadata']   if u'imageMediaMetadata'    in raw_data else None

            if u'downloadUrl' in raw_data:
                self.__info['download_links'][self.__info['mime_type']] = raw_data[u'downloadUrl']

            # This is encoded for displaying locally.
            self.__info['title_fs'] = get_utility().translate_filename_charset(raw_data[u'title'])

            for parent in raw_data[u'parents']:
                self.__parents.append(parent[u'id'])

        except (KeyError) as e:
            self.__log.exception("Could not normalize entry on raw key [%s]. Does not exist in source." % (str(e)))
            raise

    def __getattr__(self, key):
        return self.__info[key]

    def __str__(self):
        return ("<NORMAL ID= [%s] MIME= [%s] NAME= [%s] URIS= (%d)>" % 
                (self.id, self.mime_type, self.title, 
                 len(self.download_links)))

    def __repr__(self):
        return str(self)

    def normalize_download_mimetype(self, preferred_mimetype, force_preferred=False):
        """If there's a download-URL for the given mime-type, return the same
        mime-type. Else, if there's a generic (octet-stream) download 
        available, return that mime-type.
        """

# TODO: The download-links might be empty. Under which files is this the case?        

        # We just check this, since we'll potentially have to pull the first 
        # item, later.
        if not self.download_links:
            return None

        if preferred_mimetype in self.download_links:
            return preferred_mimetype

        if force_preferred is True:
            return None

        if NormalEntry.default_general_mime_type in self.download_links:
            return NormalEntry.default_general_mime_type

        # If there's only one download link, resort to using it (perhaps it was 
        # an uploaded file, assigned only one type).
        if len(self.download_links) == 1:
            return self.download_links.values()[0]

        return None

    @property
    def is_directory(self):
        """Return True if we represent a directory."""
        return get_utility().is_directory(self)

    @property
    def requires_displaceable(self):
        """Return True if reading from this file should return info and deposit 
        the data elsewhere. This is predominantly determined by whether we can
        get a file-size up-front.
        """
        return (u'fileSize' not in self.__raw_data or 
                int(self.__raw_data[u'fileSize']) == 0)

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
    def normalized_mime_type(self):
        try:
            return get_utility().get_normalized_mime_type(self)
        except:
            self.__log.exception("Could not render a mime-type for entry with"
                              " ID [%s], for read." % (self.id))
            raise

    @property
    def parents(self):
        return self.__parents

    @property
    def raw_data(self):
        return self.__raw_data

