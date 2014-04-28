import logging

from threading import Lock, Timer

from gdrivefs.conf import Conf
from gdrivefs.timer import Timers
from gdrivefs.gdtool.account_info import AccountInfo
from gdrivefs.gdtool.drive import drive_proxy
from gdrivefs.cache.volume import PathRelations, EntryCache

_logger = logging.getLogger(__name__)

def _sched_check_changes():
    logging.debug("Doing scheduled check for changes.")

    try:
        get_change_manager().process_updates()
        logging.debug("Updates have been processed. Rescheduling.")

        # Schedule next invocation.
        t = Timer(Conf.get('change_check_frequency_s'), _sched_check_changes)

        Timers.get_instance().register_timer('change', t)
    except:
        _logger.exception("Exception while managing changes.")
        raise

class _ChangeManager(object):
    def __init__(self):
        self.at_change_id = AccountInfo.get_instance().largest_change_id
        _logger.debug("Latest change-ID at startup is (%d)." % 
                      (self.at_change_id))

    def mount_init(self):
        """Called when filesystem is first mounted."""

        _logger.debug("Scheduling change monitor.")
        _sched_check_changes()

    def mount_destroy(self):
        """Called when the filesystem is unmounted."""

        _logger.debug("Change destroy.")

    def process_updates(self):
        """Process any changes to our files. Return True if everything is up to
        date or False if we need to be run again.
        """
# TODO(dustin): Is there any way that we can block on this call?
        start_at_id = (self.at_change_id + 1)

        _logger.debug("Requesting changes.")
        result = drive_proxy('list_changes', start_change_id=start_at_id)
        (largest_change_id, next_page_token, changes) = result

        _logger.debug("The latest reported change-ID is (%d) and we're "
                      "currently at change-ID (%d)." % 
                      (largest_change_id, self.at_change_id))

        if largest_change_id == self.at_change_id:
            _logger.debug("No entries have changed.")
            return True

        _logger.info("(%d) changes will now be applied." % (len(changes)))

        for change_id, change_tuple in changes.iteritems():
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
        
        is_visible = entry.is_visible if entry else None

        _logger.info("Applying change with change-ID (%d), entry-ID [%s], "
                        "and is-visible of [%s]" % 
                        (change_id, entry_id, is_visible))

        # First, remove any current knowledge from the system.

        _logger.debug("Removing all trace of entry with ID [%s] "
                         "(apply_change)." % (entry_id))

        try:
            PathRelations.get_instance().remove_entry_all(entry_id)
        except:
            _logger.exception("There was a problem remove entry with ID "
                                 "[%s] from the caches." % (entry_id))
            raise

        # If it wasn't deleted, add it back.

        _logger.debug("Registering changed entry with ID [%s]." % 
                         (entry_id))

        if is_visible:
            path_relations = PathRelations.get_instance()

            try:
                path_relations.register_entry(entry)
            except:
                _logger.exception("Could not register changed entry with "
                                     "ID [%s] with path-relations cache." % 
                                     (entry_id))
                raise

def get_change_manager():
    with get_change_manager.lock:
        if not get_change_manager.instance:
            get_change_manager.instance = _ChangeManager()

        return get_change_manager.instance

get_change_manager.instance = None
get_change_manager.lock = Lock()

