import logging

from threading import Timer

from gdrivefs.conf import Conf
from gdrivefs.timer import Timers

class Report(object):
    """A tool for gathering statistics and emitting them to the log."""

    data = { }

    def __init__(self):
        logging.debug("Initializing Report singleton.")

        self.__emit_log()

    @staticmethod
    def get_instance():
        if not Report.instance:
            try:
                Report.instance = Report()
            except:
                logging.exception("Could not create Report.")
                raise

        return Report.instance

    def __emit_log(self):
        for source_name, source_data in self.data.iteritems():
            pairs = [ ("%s= [%s]" % (k, v)) 
                        for k, v 
                        in source_data.iteritems() ]
            logging.info("RPT EMIT(%s): %s" % (source_name, ', '.join(pairs)))

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
    
        logging.debug("Setting reporting key [%s] with source [%s]." % 
                      (key, source_name))

        if source_name not in self.data:
            self.data[source_name] = { }

        self.data[source_name][key] = value

Report.instance = None



