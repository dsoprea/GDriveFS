import logging

from gdrivefs.cache.cache_agent import CacheAgent

class CacheClientBase(object):
    """Meant to be inherited by a class. Is used to configure a particular 
    namespace within the cache.
    """

    __log = None

    @property
    def cache(self):
        try:
            return self._cache
        except:
            pass

        self._cache = CacheAgent(self.child_type, self.max_age, 
                                 fault_handler=self.fault_handler, 
                                 cleanup_pretrigger=self.cleanup_pretrigger)

        return self._cache

    def __init__(self):
        self.__log = logging.getLogger().getChild('CacheClientBase')
        child_type = self.__class__.__bases__[0].__name__
        max_age = self.get_max_cache_age_seconds()
        
        self.__log.debug("CacheClientBase(%s,%s)" % (child_type, max_age))

        self.child_type = child_type
        self.max_age = max_age

        self.init()

    def fault_handler(self, resource_name, key):
        pass

    def cleanup_pretrigger(self, resource_name, key, force):
        pass

    def init(self):
        pass

    def get_max_cache_age_seconds(self):
        raise NotImplementedError("get_max_cache_age() must be implemented in "
                                  "the CacheClientBase child.")

    @classmethod
    def get_instance(cls):
        """A helper method to dispense a singleton of whomever is inheriting "
        from us.
        """

        class_name = cls.__name__

        try:
            CacheClientBase.__instances
        except:
            CacheClientBase.__instances = { }

        try:
            return CacheClientBase.__instances[class_name]
        except:
            CacheClientBase.__instances[class_name] = cls()
            return CacheClientBase.__instances[class_name]


