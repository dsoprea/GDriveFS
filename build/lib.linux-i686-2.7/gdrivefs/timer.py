from threading import Timer, Lock

import logging

class Timers(object):
    timers = None
    lock = Lock()
    autostart_default = True

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

    def set_autostart_default(self, flag):
        """This can be set to keep the timers from actually starting, if we
        don't want to spawn off new threads."""

        Timers.autostart_default = flag

    def register_timer(self, name, timer, autostart=None):
        if autostart is None:
            autostart = Timers.autostart_default

        with self.lock:
            if name not in self.timers:
                self.timers[name] = timer

                if autostart:
                    timer.start()

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

