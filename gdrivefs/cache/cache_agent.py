import logging
import threading
import time
import datetime

#import gdrivefs.report

import gdrivefs.state

from gdrivefs.conf import Conf
from gdrivefs.cache.cache_registry import CacheRegistry, CacheFault

_logger = logging.getLogger(__name__)
_logger.setLevel(logging.INFO)


class CacheAgent(object):
    """A particular namespace within the cache."""

    registry        = None
    resource_name   = None
    max_age         = None

    fault_handler       = None
    cleanup_pretrigger  = None

    report              = None
    report_source_name  = None

    def __init__(self, resource_name, max_age, fault_handler=None, 
                 cleanup_pretrigger=None):
        _logger.debug("CacheAgent(%s,%s,%s,%s)" % (resource_name, max_age, 
                                                   type(fault_handler), 
                                                   cleanup_pretrigger))

        self.registry = CacheRegistry.get_instance(resource_name)
        self.resource_name = resource_name
        self.max_age = max_age

        self.fault_handler = fault_handler
        self.cleanup_pretrigger = cleanup_pretrigger

#        self.report = Report.get_instance()
#        self.report_source_name = ("cache-%s" % (self.resource_name))

        self.__t = None
        self.__t_quit_ev = threading.Event()

        self.__start_cleanup()

    def __del__(self):
        self.__stop_cleanup()

# TODO(dustin): Currently disabled. The system doesn't rely on it, and it's 
#               just another thread that unnecessarily runs, and trips up our 
#               ability to test individual components in simple isolation. It
#               needs to be refactored.
#
#               We'd like to either refactor into a multiprocessing worker, or
#               just send to statsd (which would be kindof cool).
#        self.__post_status()

#    def __del__(self):
#
#        if self.report.is_source(self.report_source_name):
#            self.report.remove_all_values(self.report_source_name)
#        pass

#    def __post_status(self):
#        """Send the current status to our reporting tool."""
#
#        num_values = self.registry.count(self.resource_name)
#
#        self.report.set_values(self.report_source_name, 'count', 
#                               num_values)
#
#        status_post_interval_s = Conf.get('cache_status_post_frequency_s')
#        status_timer = Timer(status_post_interval_s, self.__post_status)
#
#        Timers.get_instance().register_timer('status', status_timer)

    def __cleanup(self):
        """Scan the current cache and determine items old-enough to be 
        removed.
        """

        cleanup_interval_s = Conf.get('cache_cleanup_check_frequency_s')

        _logger.info("Cache-cleanup thread running: %s", self)

        while self.__t_quit_ev.is_set() is False and \
                  gdrivefs.state.GLOBAL_EXIT_EVENT.is_set() is False:
            _logger.debug("Doing clean-up for cache resource with name [%s]." % 
                          (self.resource_name))

            cache_dict = self.registry.list_raw(self.resource_name)

            total_keys = [ (key, value_tuple[1]) for key, value_tuple \
                                in cache_dict.iteritems() ]

            cleanup_keys = [ key for key, value_tuple \
                                in cache_dict.iteritems() \
                                if (datetime.datetime.now() - value_tuple[1]).seconds > \
                                        self.max_age ]

            _logger.debug("Found (%d) entries to clean-up from entry-cache." % 
                          (len(cleanup_keys)))

            if cleanup_keys:
                for key in cleanup_keys:
                    _logger.debug("Cache entry [%s] under resource-name [%s] "
                                  "will be cleaned-up." % 
                                  (key, self.resource_name))

                    if self.exists(key, no_fault_check=True) == False:
                        _logger.debug("Entry with ID [%s] has already been "
                                      "cleaned-up." % (key))
                    else:
                        self.remove(key)
            else:
                _logger.debug("No cache-cleanup required.")
                time.sleep(cleanup_interval_s)

        _logger.info("Cache-cleanup thread terminating: %s", self)

    def __start_cleanup(self):
        _logger.info("Starting cache-cleanup thread: %s", self)

        self.__t = threading.Thread(target=self.__cleanup)
        self.__t.start()

    def __stop_cleanup(self):
        _logger.info("Stopping cache-cleanup thread: %s", self)

        self.__t_quit_ev.set()
        self.__t.join()

    def set(self, key, value):
        _logger.debug("CacheAgent.set(%s,%s)" % (key, value))

        return self.registry.set(self.resource_name, key, value)

    def remove(self, key):
        _logger.debug("CacheAgent.remove(%s)" % (key))

        return self.registry.remove(self.resource_name, 
                                    key, 
                                    cleanup_pretrigger=self.cleanup_pretrigger)

    def get(self, key, handle_fault = None):

        if handle_fault == None:
            handle_fault = True

        _logger.debug("CacheAgent.get(%s)" % (key))

        try:
            result = self.registry.get(self.resource_name, 
                                       key, 
                                       max_age=self.max_age, 
                                       cleanup_pretrigger=self.cleanup_pretrigger)
        except CacheFault:
            _logger.debug("There was a cache-miss while requesting item with "
                          "ID (key).")

            if self.fault_handler == None or not handle_fault:
                raise

            result = self.fault_handler(self.resource_name, key)
            if result is None:
                raise

        return result

    def exists(self, key, no_fault_check=False):
        _logger.debug("CacheAgent.exists(%s)" % (key))

        return self.registry.exists(self.resource_name, key, 
                                    max_age=self.max_age,
                                    cleanup_pretrigger=self.cleanup_pretrigger,
                                    no_fault_check=no_fault_check)

    def __getitem__(self, key):
        return self.get(key)

    def __setitem__(self, key, value):
        return self.set(key, value)

    def __delitem__(self, key):
        return self.remove(key)
