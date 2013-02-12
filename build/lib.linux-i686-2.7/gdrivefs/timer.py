from threading import Timer, Lock

import logging

class Timers(object):
    timers = None
    lock = Lock()

    def __init__(self):
        with self.lock:
            self.timers = { }

        logging.debug("Timers manager initialized.")

    @staticmethod
    def get_instance():
        with Timers.singleton_lock:
            if not Timers.instance:
                Timers.instance = Timers()

        return Timers.instance

    def register_timer(self, name, timer):
        with self.lock:
            self.timers[name] = timer

    def cancel_all(self):
        """Cancelling all timer threads. This might be called multiple times 
        depending on how we're terminated.
        """

        with self.lock:
            for name, timer in self.timers.items():
                logging.debug("Cancelling timer [%s]." % (name))
                timer.cancel()

                del self.timers[name]

Timers.instance = None
Timers.singleton_lock = Lock()

