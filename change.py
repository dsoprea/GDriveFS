import logging

from threading import Lock, Timer

from gdrivefs.gdtool import AccountInfo, drive_proxy
from gdrivefs.conf import Conf
from gdrivefs.cache import PathRelations, EntryCache

def _sched_check_changes():
    
    logging.debug("Doing scheduled check for changes.")

    get_change_manager().process_updates()

    # Schedule next invocation.
    t = Timer(Conf.get('change_check_frequency_s'), _sched_check_changes)
    t.start()

    _sched_check_changes.timer = t

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

        logging.info("Change init.")

        _sched_check_changes()

    def mount_destroy(self):
        """Called when the filesystem is unmounted."""

        logging.info("Change destroy.")

        try:
            _sched_check_changes.timer
        except:
            message = "No timer was defined. This should not be possible."

            logging.error(message)
            raise Exception(message)

        logging.info("Cancelling current change-timer.")
        _sched_check_changes.timer.cancel()

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
            logging.debug("No files affected.")
            return True

        logging.info("(%d) changes will now be applied." % (len(changes)))

        for change_id, change_tuple in changes.iteritems():
            # Apply the changes. We expect to be running them from oldest to 
            # newest.

            logging.info("Change with ID (%d) will now be applied." %
                         (change_id))

            try:
                self.__apply_change(change_id, change_tuple)
            except:
                logging.exception("There was a problem while processing change"
                                  " with ID (%d). No more changes will be "
                                  "applied." % (change_id))
                return False

            self.at_change_id = change_id

        return (next_page_token == None)

    def __apply_change(self, change_id, change_tuple):
        """Apply changes to our filesystem reported by GD. All we do is remove 
        the current record components, if it's valid, and then reload it with 
        what we were given. Note that since we don't necessarily know
        about the entries that have been changed, this also allows us to slowly
        increase our knowledge of the filesystem (of, obviously, only those 
        things that change).
        """

        (entry_id, was_deleted, entry) = change_tuple
        
        logging.info("Applying change with change-ID (%d), entry-ID [%s], and "
                     "is-visible of [%s]" % (change_id, entry_id, entry.is_visible))

        # First, remove any current knowledge from the system.

        logging.debug("Removing all trace of entry with ID [%s]." % (entry_id))

        try:
            PathRelations.get_instance().remove_entry_all(entry_id)
        except:
            logging.exception("There was a problem remove entry with ID [%s] "
                              "from the caches." % (entry_id))
            raise

        # If it wasn't deleted, add it back.

        logging.debug("Registering changed entry with ID [%s]." % (entry_id))

        if entry.is_visible:
            path_relations = PathRelations.get_instance()

            try:
                path_relations.register_entry(entry)
            except:
                logging.exception("Could not register changed entry with ID "
                                  "[%s] with path-relations cache." % 
                                  (entry_id))
                raise

def get_change_manager():
    with get_change_manager.lock:
        if not get_change_manager.instance:
            get_change_manager.instance = _ChangeManager()

        return get_change_manager.instance

get_change_manager.instance = None
get_change_manager.lock = Lock()

