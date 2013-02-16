import logging

from threading import RLock
from datetime import datetime


class CacheFault(Exception):
    pass


class CacheRegistry(object):
    """The main cache container."""

    __rlock = RLock()
    __log = None

    def __init__(self):
        self.__log = logging.getLogger().getChild('CacheReg')
        self.__cache = { }

    @staticmethod
    def get_instance(resource_name):
    
        with CacheRegistry.__rlock:
            try:
                CacheRegistry.__instance;
            except:
                CacheRegistry.__instance = CacheRegistry()

            if resource_name not in CacheRegistry.__instance.__cache:
                CacheRegistry.__instance.__cache[resource_name] = { }

        return CacheRegistry.__instance

    def set(self, resource_name, key, value):

        self.__log.debug("CacheRegistry.set(%s,%s,%s)" % (resource_name, key, 
                                                          value))

        with CacheRegistry.__rlock:
            try:
                old_tuple = self.__cache[resource_name][key]
            except:
                old_tuple = None

            self.__cache[resource_name][key] = (value, datetime.now())

        return old_tuple

    def remove(self, resource_name, key, cleanup_pretrigger=None):

        self.__log.debug("CacheRegistry.remove(%s,%s,%s)" % 
                         (resource_name, key, type(cleanup_pretrigger)))

        with CacheRegistry.__rlock:
            try:
                old_tuple = self.__cache[resource_name][key]
            except:
                raise

            self.__cleanup_entry(resource_name, key, True, 
                                 cleanup_pretrigger=cleanup_pretrigger)

        return old_tuple[0]

    def get(self, resource_name, key, max_age, cleanup_pretrigger=None):
        
        trigger_given_phrase = ('None' 
                                if cleanup_pretrigger == None 
                                else '<given>')

        self.__log.debug("CacheRegistry.get(%s,%s,%s,%s)" % 
                         (resource_name, key, max_age, trigger_given_phrase))

        with CacheRegistry.__rlock:
            try:
                (value, timestamp) = self.__cache[resource_name][key]
            except:
                raise CacheFault("NonExist")

            if max_age != None and \
               (datetime.now() - timestamp).seconds > max_age:
                self.__cleanup_entry(resource_name, key, False, 
                                     cleanup_pretrigger=cleanup_pretrigger)
                raise CacheFault("Stale")

        return value

    def list_raw(self, resource_name):
        
        self.__log.debug("CacheRegistry.list(%s)" % (resource_name))

        with CacheRegistry.__rlock:
            try:
                return self.__cache[resource_name]
            except:
                self.__log.exception("Could not list raw-entries under cache "
                                  "labelled with resource-name [%s]." %
                                  (resource_name))
                raise

    def exists(self, resource_name, key, max_age, cleanup_pretrigger=None, 
               no_fault_check=False):

        self.__log.debug("CacheRegistry.exists(%s,%s,%s,%s)" % (resource_name, 
                      key, max_age, cleanup_pretrigger))
        
        with CacheRegistry.__rlock:
            try:
                (value, timestamp) = self.__cache[resource_name][key]
            except:
                return False

            if max_age != None and not no_fault_check and \
                    (datetime.now() - timestamp).seconds > max_age:
                self.__cleanup_entry(resource_name, key, False, 
                                     cleanup_pretrigger=cleanup_pretrigger)
                return False

        return True

    def count(self, resource_name):

        return len(self.__cache[resource_name])

    def __cleanup_entry(self, resource_name, key, force, 
                        cleanup_pretrigger=None):

        self.__log.debug("Doing clean-up for resource_name [%s] and key "
                         "[%s]." % (resource_name, key))

        if cleanup_pretrigger != None:
            self.__log.debug("Running pre-cleanup trigger for resource_name "
                             "[%s] and key [%s]." % (resource_name, key))

            try:
                cleanup_pretrigger(resource_name, key, force)
            except:
                self.__log.exception("Cleanup-trigger failed.")
                raise

        try:
            del self.__cache[resource_name][key]
        except:
            self.__log.exception("Could not clean-up entry with resource_name "
                              "[%s] and key [%s]." % (resource_name, key))
            raise

