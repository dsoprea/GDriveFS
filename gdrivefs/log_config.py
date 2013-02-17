from logging import getLogger, Filter, Formatter, DEBUG
from logging.handlers import SysLogHandler, TimedRotatingFileHandler
from os.path import abspath, dirname

import gdrivefs

default_logger = getLogger()
default_logger.setLevel(DEBUG)

# TODO: For errors: SMTPHandler

root_path = abspath(dirname(gdrivefs.__file__) + '/..')
log_filepath = ('%s/logs/gdrivefs.log' % (root_path))

#syslog_format = 'MC:%(name)s %(levelname)s %(message)s'
flat_format = '%(asctime)s [%(name)s %(levelname)s] %(message)s'

formatter = Formatter(flat_format)

log_file = TimedRotatingFileHandler(log_filepath, 'D', backupCount=5)
log_file.setFormatter(formatter)
default_logger.addHandler(log_file)

