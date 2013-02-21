import logging

from datetime import datetime

from collections import OrderedDict
from threading import Timer
from gdrivefs.timer import Timers
from gdrivefs.conf import Conf

from gdrivefs.cache.cache_registry import CacheRegistry, CacheFault
from gdrivefs.report import Report

class CacheAgent(object):
    """A particular namespace within the cache."""

    __log = None

    registry        = None
    resource_name   = None
    max_age         = None

    fault_handler       = None
    cleanup_pretrigger  = None

    report              = None
    report_source_name  = None

    def __init__(self, resource_name, max_age, fault_handler=None, 
                 cleanup_pretrigger=None):
        self.__log = logging.getLogger().getChild('CacheAgent')

        self.__log.debug("CacheAgent(%s,%s,%s,%s)" % (resource_name, max_age, 
                                                   type(fault_handler), 
                                                   cleanup_pretrigger))

        self.registry = CacheRegistry.get_instance(resource_name)
        self.resource_name = resource_name
        self.max_age = max_age

        self.fault_handler = fault_handler
        self.cleanup_pretrigger = cleanup_pretrigger

        self.report = Report.get_instance()
        self.report_source_name = ("cache-%s" % (self.resource_name))

        # Run a clean-up cycle to get it scheduled.
#        self.__cleanup_check()
        self.__post_status()

    def __del__(self):

        if self.report.is_source(self.report_source_name):
            self.report.remove_all_values(self.report_source_name)

    def __post_status(self):
        """Send the current status to our reporting tool."""

        try:
            num_values = self.registry.count(self.resource_name)
        except:
            self.__log.exception("Could not get count of values for resource "
                                 "with name [%s]." % (self.resource_name))
            raise

        try:
            self.report.set_values(self.report_source_name, 'count', 
                                   num_values)
        except:
            self.__log.exception("Cache could not post status for resource "
                                 "with name [%s]." % (self.resource_name))
            raise

        status_post_interval_s = Conf.get('cache_status_post_frequency_s')
        status_timer = Timer(status_post_interval_s, self.__post_status)

        Timers.get_instance().register_timer('status', status_timer)

    def __cleanup_check(self):
        """Scan the current cache and determine items old-enough to be 
        removed.
        """

        self.__log.debug("Doing clean-up for cache resource with name [%s]." % 
                      (self.resource_name))

        try:
            cache_dict = self.registry.list_raw(self.resource_name)
        except:
            self.__log.exception("Could not do clean-up check with resource-"
                                 "name [%s]." % (self.resource_name))
            raise

        total_keys = [ (key, value_tuple[1]) for key, value_tuple \
                            in cache_dict.iteritems() ]

        cleanup_keys = [ key for key, value_tuple \
                            in cache_dict.iteritems() \
                            if (datetime.now() - value_tuple[1]).seconds > \
                                    self.max_age ]

        self.__log.info("Found (%d) entries to clean-up from entry-cache." % 
                        (len(cleanup_keys)))

        if cleanup_keys:
            for key in cleanup_keys:
                self.__log.debug("Cache entry [%s] under resource-name [%s] "
                                 "will be cleaned-up." % (key, 
                                                          self.resource_name))

                if self.exists(key, no_fault_check=True) == False:
                    self.__log.debug("Entry with ID [%s] has already been "
                                     "cleaned-up." % (key))
                else:
                    try:
                        self.remove(key)
                    except:
                        self.__log.exception("Cache entry [%s] under resource-"
                                             "name [%s] could not be cleaned-"
                                             "up." % (key, self.resource_name))
                        raise

            self.__log.debug("Scheduled clean-up complete.")

        cleanup_interval_s = Conf.get('cache_cleanup_check_frequency_s')
        cleanup_timer = Timer(cleanup_interval_s, self.__cleanup_check)

        Timers.get_instance().register_timer('cleanup', cleanup_timer)

    def set(self, key, value):
        self.__log.debug("CacheAgent.set(%s,%s)" % (key, value))

        return self.registry.set(self.resource_name, key, value)

    def remove(self, key):
        self.__log.debug("CacheAgent.remove(%s)" % (key))

        return self.registry.remove(self.resource_name, 
                                    key, 
                                    cleanup_pretrigger=self.cleanup_pretrigger)

    def get(self, key, handle_fault = None):

        if handle_fault == None:
            handle_fault = True

        self.__log.debug("CacheAgent.get(%s)" % (key))

        try:
            result = self.registry.get(self.resource_name, 
                                       key, 
                                       max_age=self.max_age, 
                                       cleanup_pretrigger=self.cleanup_pretrigger)
        except CacheFault:
            self.__log.debug("There was a cache-miss while requesting item "
                             "with ID (key).")

            if self.fault_handler == None or not handle_fault:
                raise

            try:
                result = self.fault_handler(self.resource_name, key)
            except:
                self.__log.exception("There was an exception in the fault-"
                                     "handler, handling for key [%s].", key)
                raise

            if result == None:
                raise

        return result

    def exists(self, key, no_fault_check=False):
        self.__log.debug("CacheAgent.exists(%s)" % (key))

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

