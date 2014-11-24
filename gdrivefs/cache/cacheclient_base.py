import logging

from gdrivefs.cache.cache_agent import CacheAgent

_logger = logging.getLogger(__name__)


class CacheClientBase(object):
    """Meant to be inherited by a class. Is used to configure a particular 
    namespace within the cache.
    """



# TODO(dustin): This is a terrible object, and needs to be refactored. It 
#               doesn't provide any way to cleanup itself or CacheAgent, or any 
#               way to invoke a singleton of CacheAgent whose thread we can 
#               easier start or stop. Since this larger *wraps* CacheAgent, we 
#               might just dispose of it.



    @property
    def cache(self):
        try:
            return self.__cache
        except:
            pass

        self.__cache = CacheAgent(self.child_type, self.max_age, 
                                 fault_handler=self.fault_handler, 
                                 cleanup_pretrigger=self.cleanup_pretrigger)

        return self.__cache

    def __init__(self):
        child_type = self.__class__.__bases__[0].__name__
        max_age = self.get_max_cache_age_seconds()
        
        _logger.debug("CacheClientBase(%s,%s)" % (child_type, max_age))

        self.child_type = child_type
        self.max_age = max_age

        self.init()

    def __del__(self):
        del self.__cache

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


