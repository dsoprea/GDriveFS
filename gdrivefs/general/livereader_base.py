import logging


class LiveReaderBase(object):
    """A base object for data that can be retrieved on demand."""

    __log = None
    __data = None

    def __getitem__(self, key):
        self.__log = logging.getLogger().getChild('LiveReaderBase')
        child_name = self.__class__.__name__

        self.__log.debug("Key [%s] requested on LiveReaderBase type [%s]." % 
                         (key, child_name))

        try:
            return self.__data[key]
        except:
            pass

        try:
            self.__data = self.get_data(key)
        except:
            self.__log.exception("Could not retrieve data for live-updater "
                                 "wrapping [%s]." % (child_name))
            raise

        try:
            return self.__data[key]
        except:
            self.__log.exception("We just updated live-updater wrapping [%s], "
                                 "but we must've not been able to find entry "
                                 "[%s]." % (child_name, key))
            raise

    def get_data(self, key):
        raise NotImplementedError("get_data() method must be implemented in "
                                  "the LiveReaderBase child.")

    @classmethod
    def get_instance(cls):
        """A helper method to dispense a singleton of whomever is inheriting "
        from us.
        """

        class_name = cls.__name__

        try:
            LiveReaderBase.__instances
        except:
            LiveReaderBase.__instances = { }

        try:
            return LiveReaderBase.__instances[class_name]
        except:
            LiveReaderBase.__instances[class_name] = cls()
            return LiveReaderBase.__instances[class_name]


