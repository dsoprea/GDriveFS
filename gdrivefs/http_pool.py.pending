from contextlib import contextmanager
from gevent.queue import Queue

from gevent import monkey; 
monkey.patch_socket()

from httplib2 import Http


class HttpPool(object):
    def __init__(self, size, factory=Http):
        self.__size = size
        self.__factory = factory
        self.__pool = Queue()

        for i in xrange(self.__size):
            self.__pool.put(self.__factory())

    @contextmanager
    def reserve(self):
        http = self.__pool.get()
        yield http
        self.__pool.put(http)

    def request(self, *args, **kwargs):
        with self.reserve() as http:
            return http.request(*args, **kwargs)

