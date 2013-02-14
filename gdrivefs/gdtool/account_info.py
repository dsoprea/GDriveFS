import logging

from gdrivefs.general.livereader_base import LiveReaderBase
from gdrivefs.gdtool.drive import drive_proxy


class AccountInfo(LiveReaderBase):
    """Encapsulates our account info."""

    __log = None

    def __init__(self):
        LiveReaderBase.__init__(self)

        self.__log = logging.getLogger().getChild('AccountInfo')

    def get_data(self, key):
        try:
            return drive_proxy('get_about_info')
        except:
            self.__log.exception("get_about_info() call failed.")
            raise

    @property
    def root_id(self):
        return self[u'rootFolderId']

    @property
    def largest_change_id(self):
        return int(self[u'largestChangeId'])

    @property
    def quota_bytes_total(self):
        return int(self[u'quotaBytesTotal'])

    @property
    def quota_bytes_used(self):
        return int(self[u'quotaBytesUsed'])

