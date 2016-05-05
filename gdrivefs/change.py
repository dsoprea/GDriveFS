import logging
import threading
import time

import gdrivefs.state

from gdrivefs.conf import Conf
from gdrivefs.gdtool.account_info import AccountInfo
from gdrivefs.gdtool.drive import get_gdrive
from gdrivefs.cache.volume import PathRelations, EntryCache

_logger = logging.getLogger(__name__)
_logger.setLevel(logging.WARNING)


class _ChangeManager(object):
    def __init__(self):
        self.at_change_id = AccountInfo.get_instance().largest_change_id
        _logger.debug("Latest change-ID at startup is (%d)." % 
                      (self.at_change_id))

        self.__t = None
        self.__t_quit_ev = threading.Event()

    def mount_init(self):
        """Called when filesystem is first mounted."""

        self.__start_check()

    def mount_destroy(self):
        """Called when the filesystem is unmounted."""

        self.__stop_check()

    def __check_changes(self):
        _logger.info("Change-processing thread running.")

        interval_s = Conf.get('change_check_frequency_s')
        cm = get_change_manager()

        while self.__t_quit_ev.is_set() is False and \
                gdrivefs.state.GLOBAL_EXIT_EVENT.is_set() is False:
            _logger.debug("Checking for changes.")

            try:
                is_done = cm.process_updates()
            except:
                _logger.exception("Squelching an exception that occurred "
                                  "while reading/processing changes.")

                # Force another check, soon.
                is_done = False

            # If there are still more changes, take them as quickly as 
            # possible.
            if is_done is True:
                _logger.debug("No more changes. Waiting.")
                time.sleep(interval_s)
            else:
                _logger.debug("There are more changes to be applied. Cycling "
                              "immediately.")

        _logger.info("Change-processing thread terminating.")

    def __start_check(self):
        _logger.info("Starting change-processing thread.")

        self.__t = threading.Thread(target=self.__check_changes)
        self.__t.start()

    def __stop_check(self):
        _logger.info("Stopping change-processing thread.")

        self.__t_quit_ev.set()
        self.__t.join()

    def process_updates(self):
        """Process any changes to our files. Return True if everything is up to
        date or False if we need to be run again.
        """
# TODO(dustin): Reimplement using the "watch" interface. We'll have to find 
#               more documentation:
#
#               https://developers.google.com/drive/v2/reference/changes/watch
#
        start_at_id = (self.at_change_id + 1)

        gd = get_gdrive()
        result = gd.list_changes(start_change_id=start_at_id)

        (largest_change_id, next_page_token, changes) = result

        _logger.debug("The latest reported change-ID is (%d) and we're "
                      "currently at change-ID (%d).",
                      largest_change_id, self.at_change_id)

        _logger.info("(%d) changes will now be applied." % (len(changes)))

        for change_id, change_tuple in changes:
            # Apply the changes. We expect to be running them from oldest to 
            # newest.

            _logger.info("========== Change with ID (%d) will now be applied. ==========" %
                            (change_id))

            try:
                self.__apply_change(change_id, change_tuple)
            except:
                _logger.exception("There was a problem while processing change"
                                  " with ID (%d). No more changes will be "
                                  "applied." % (change_id))
                return False

            self.at_change_id = change_id

        return (next_page_token is None)

    def __apply_change(self, change_id, change_tuple):
        """Apply changes to our filesystem reported by GD. All we do is remove 
        the current record components, if it's valid, and then reload it with 
        what we were given. Note that since we don't necessarily know
        about the entries that have been changed, this also allows us to slowly
        increase our knowledge of the filesystem (of, obviously, only those 
        things that change).
        """

        (entry_id, was_deleted, entry) = change_tuple
        
        is_visible = entry.is_visible if entry else None

        _logger.info("Applying change with change-ID (%d), entry-ID [%s], "
                     "and is-visible of [%s]",
                     change_id, entry_id, is_visible)

        # First, remove any current knowledge from the system.

        _logger.debug("Removing all trace of entry with ID [%s] "
                      "(apply_change).", entry_id)

        PathRelations.get_instance().remove_entry_all(entry_id)

        # If it wasn't deleted, add it back.

        _logger.debug("Registering changed entry with ID [%s].", entry_id)

        if is_visible:
            path_relations = PathRelations.get_instance()
            path_relations.register_entry(entry)

_instance = None
def get_change_manager():
    global _instance

    if _instance is None:
        _instance = _ChangeManager()

    return _instance
