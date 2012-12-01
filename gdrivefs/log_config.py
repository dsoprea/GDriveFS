import logging
import logging.handlers

from syslog import LOG_LOCAL0

default_logger = logging.getLogger()
default_logger.setLevel(logging.DEBUG)

log_syslog = logging.handlers.SysLogHandler('/dev/log', facility=LOG_LOCAL0)

log_format = 'GD: %(levelname)s %(message)s'
log_syslog.setFormatter(logging.Formatter(log_format))

default_logger.addHandler(log_syslog)

