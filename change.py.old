def apply_changes():
    """Go and get a list of recent changes, and then apply them. This is a 
    separate mechanism because it is too complex an action to put into 
    _GdriveManager, and it can't be put into _FileCache because it would create 
    a cyclical relationship with _GdriveManager."""

    # Get cache object.

    try:
        file_cache = get_cache()
    except:
        logging.exception("Could not acquire cache.")
        raise

    # Get latest change-ID to use as a marker.

    try:
        local_latest_change_id = file_cache.get_latest_change_id(self)
    except:
        logging.exception("Could not get latest change-ID.")
        raise

    # Move through the changes.

    page_token = None
    page_num = 0
    all_changes = []
    while(1):
        logging.debug("Retrieving first page of changes using page-token [%s]." 
                      % (page_token))

        # Get page.

        try:
            change_tuple = drive_proxy('list_changes', page_token=page_token)
            (largest_change_id, next_page_token, changes) = change_tuple
        except:
            logging.exception("Could not get changes for page_token [%s] on "
                              "page (%d)." % (page_token, page_num))
            raise

        logging.info("We have retrieved (%d) recent changes." % (len(changes)))

        # Determine whether we're getting changes added since last time. This 
        # is only really relevant just the first time, as the same value is
        # returned in all subsequent pages.

        if local_latest_change_id != None and largest_change_id <= local_latest_change_id:
            if largest_change_id < local_latest_change_id:
                logging.warning("For some reason, the remote change-ID (%d) is"
                                " -less- than our local change-ID (%d)." % 
                                (largest_change_id, local_largest_change_id))
                return

        # If we're here, this is either the first time, or there have actually 
        # been changes. Collect all of the change information.

        for change_id, change in changes.iteritems():
            all_changes[change_id] = change

        if next_page_token == None:
            break

        page_num += 1 

    # We now have a list of all changes.

    if not changes:
        logging.info("No changes were reported.")

    else:
        logging.info("We will now apply (%d) changes." % (len(changes)))

        try:
            file_cache.apply_changes(changes)
        except:
            logging.exception("An error occured while applying changes.")
            raise

        logging.info("Changes were applied successfully.")

class ChangeMonitor(Thread):
    """The change-management thread."""

    def __init__(self):
        super(self.__class__, self).__init__()
        self.stop_event = Event()

    def run(self):
        while(1):
            if self.stop_event.isSet():
                logging.info("ChangeMonitor is terminating.")
                break
        
            try:
                new_random = random.randint(1, 10)
                q.put(new_random, False)
                log_me("Child put (%d)." % (new_random), True)

            except Full:
                log_me("Can not add new item. Full.")
            
            time.sleep(Conf.get('change_check_interval_s'))

#change_monitor_thread = ChangeMonitor()
#change_monitor_thread.start()

