import logging

from threading import Lock

from gdrivefs.gdtool import AccountInfo, drive_proxy

class _ChangeManager(object):
    at_change_id = None

    def __init__(self):
        try:
            self.at_change_id = AccountInfo.get_instance().largest_change_id
        except:
            logging.exception("Could not get largest change-ID.")
            raise

        logging.info("Latest change-ID at startup is (%d)." % 
                     (self.at_change_id))

    def mount_init(self):
        """Called when filesystem is first mounted."""
# TODO: Fill-in.
        logging.info("Change init.")

    def mount_destroy(self):
        """Called when the filesystem is unmounted."""
# TODO: Fill-in.
        logging.info("Change destroy.")

    def __get_updates(self):

        try:
            return drive_proxy('list_changes', start_change_id=(self.at_change_id + 1))
        except:
            logging.exception("Could not get changes since change with ID "
                              "(%d)." % (self.at_change_id))
            raise

    def process_updates(self):
        """Process any changes to our files. Return True if everything is up to
        date or False if we need to be run again.
        """

        try:
            (largest_change_id, next_page_token, changes) = \
                self.__get_updates()
        except:
            logging.exception("Could not retrieve updates. Skipped.")
            raise

        logging.debug("The latest reported change-ID is (%d) and we're "
                      "currently at change-ID (%d)." % (largest_change_id, 
                                                        self.at_change_id))

        if largest_change_id == self.at_change_id:
            logging.debug("Files confirmed up-to-date.")
            return True

        logging.info("(%d) changes will now be applied." % (len(changes)))

        for change_id, change_tuple in changes.iteritems():
            # Apply the changes. We expect to be running them from oldest to 
            # newest.

            try:
                self.__apply_change(change_tuple)
            except:
                logging.exception("There was a problem while processing change"
                                  " with ID (%d). No more changes will be "
                                  "applied." % (change_id))
                return False

            self.at_change_id = change_id

        return (next_page_token == None)

    def __apply_change(self, change_tuple):

        (entry_id, was_deleted, entry) = change_tuple

# TODO: Finish this.

def get_change_manager():
    with get_change_manager.lock:
        if not get_change_manager.instance:
            get_change_manager.instance = _ChangeManager()

        return get_change_manager.instance

get_change_manager.instance = None
get_change_manager.lock = Lock()

