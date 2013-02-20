import logging

from gdrivefs.general.livereader_base import LiveReaderBase
from gdrivefs.gdtool.drive import drive_proxy


class AccountInfo(LiveReaderBase):
    """Encapsulates our account info."""

    __log = None
    __map = {'root_id': u'rootFolderId',
             'largest_change_id': (u'largestChangeId', int),
             'quota_bytes_total': (u'quotaBytesTotal', int),
             'quota_bytes_used': (u'quotaBytesUsed', int)}

    def __init__(self):
        LiveReaderBase.__init__(self)

        self.__log = logging.getLogger().getChild('AccountInfo')

    def get_data(self, key):
        try:
            return drive_proxy('get_about_info')
        except:
            self.__log.exception("get_about_info() call failed.")
            raise

    def __getattr__(self, key):
        target = AccountInfo.__map[key]
        _type = None
        
        if target.__class__ == tuple:
            (target, _type) = target

        value = self[target]
        if _type is not None:
            value = _type(value)

        return value

    @property
    def keys(self):
        return AccountInfo.__map.keys()

