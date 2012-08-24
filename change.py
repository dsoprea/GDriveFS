
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

