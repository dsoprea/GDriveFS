import logging
import re
import dateutil.parser

from time import mktime

from gdrivefs.conf import Conf
from gdrivefs.utility import get_utility


class NormalEntry(object):
    __log = None
    raw_data = None

    def __init__(self, gd_resource_type, raw_data):
        # LESSONLEARNED: We had these set as properties, but CPython was 
        #                reusing the reference between objects.

        self.__log = logging.getLogger().getChild('NormalEntry')

        self.info = { }
        self.parents = [ ]
        self.raw_data = raw_data

        try:
            self.info['mime_type']                  = raw_data[u'mimeType']
            self.info['labels']                     = raw_data[u'labels']
            self.info['id']                         = raw_data[u'id']
            self.info['title']                      = raw_data[u'title']
            self.info['last_modifying_user_name']   = raw_data[u'lastModifyingUserName']
            self.info['writers_can_share']          = raw_data[u'writersCanShare']
            self.info['owner_names']                = raw_data[u'ownerNames']
            self.info['editable']                   = raw_data[u'editable']
            self.info['user_permission']            = raw_data[u'userPermission']
            self.info['modified_date']              = dateutil.parser.parse(raw_data[u'modifiedDate'])
            self.info['modified_date_epoch']        = int(mktime(self.info['modified_date'].timetuple()))
            self.info['created_date']               = dateutil.parser.parse(raw_data[u'createdDate'])
            self.info['created_date_epoch']         = int(mktime(self.info['created_date'].timetuple()))

            self.info['download_links']         = raw_data[u'exportLinks']          if u'exportLinks'           in raw_data else { }
            self.info['link']                   = raw_data[u'embedLink']            if u'embedLink'             in raw_data else None
            self.info['modified_by_me_date']    = raw_data[u'modifiedByMeDate']     if u'modifiedByMeDate'      in raw_data else None
            self.info['last_viewed_by_me_date'] = raw_data[u'lastViewedByMeDate']   if u'lastViewedByMeDate'    in raw_data else None
            self.info['file_size']              = int(raw_data[u'fileSize'])        if u'fileSize'              in raw_data else 0
            self.info['file_extension']         = raw_data[u'fileExtension']        if u'fileExtension'         in raw_data else None
            self.info['md5_checksum']           = raw_data[u'md5Checksum']          if u'md5Checksum'           in raw_data else None
            self.info['image_media_metadata']   = raw_data[u'imageMediaMetadata']   if u'imageMediaMetadata'    in raw_data else None

            if u'downloadUrl' in raw_data:
                self.info['download_links'][self.info['mime_type']] = raw_data[u'downloadUrl']

            # This is encoded for displaying locally.
            self.info['title_fs'] = get_utility().translate_filename_charset(raw_data[u'title'])

            for parent in raw_data[u'parents']:
                self.parents.append(parent[u'id'])

            self.__log.debug("Entry with ID [%s] is visible? %s" % (self.id, self.is_visible))

        except (KeyError) as e:
            self.__log.exception("Could not normalize entry on raw key [%s]. Does not exist in source." % (str(e)))
            raise

    def __getattr__(self, key):
        if key not in self.info:
            return None

        return self.info[key]

    def __str__(self):
        return ("<NORMAL [%s] [%s] [%s]>" % (self.id, self.mime_type, 
                                             self.title))

    def __repr__(self):
        return str(self)

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
        return (u'fileSize' not in self.raw_data)

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


