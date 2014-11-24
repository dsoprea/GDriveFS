import logging

from threading import Timer

from gdrivefs.conf import Conf
from gdrivefs.timer import Timers

_logger = logging.getLogger(__name__)


class Report(object):
    """A tool for gathering statistics and emitting them to the log."""

    data = { }

    def __init__(self):
        _logger.debug("Initializing Report singleton.")

        self.__emit_log()

    def __emit_log(self):
        for source_name, source_data in self.data.iteritems():
            pairs = [ ("%s= [%s]" % (k, v)) 
                        for k, v 
                        in source_data.iteritems() ]
            _logger.info("RPT EMIT(%s): %s", source_name, ', '.join(pairs))

        report_emit_interval_s = Conf.get('report_emit_frequency_s')
        emit_timer = Timer(report_emit_interval_s, self.__emit_log)

        Timers.get_instance().register_timer('emit', emit_timer)

    def remove_all_values(self, source_name):

        del self.data[source_name]

    def get_values(self, source_name):

        return self.data[source_name]

    def is_source(self, source_name):

        return source_name in self.data

    def set_values(self, source_name, key, value):
    
        _logger.debug("Setting reporting key [%s] with source [%s].",
                      key, source_name)

        if source_name not in self.data:
            self.data[source_name] = { }

        self.data[source_name][key] = value

_instance = None

def get_report_instance():
    global _instance

    if _instance is None:
        _instance = Report()

    return _instance
