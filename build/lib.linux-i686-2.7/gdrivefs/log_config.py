import logging
import logging.handlers

from syslog import LOG_LOCAL0

root_logger = logging.getLogger()
root_logger.setLevel(logging.DEBUG)

log_syslog = logging.handlers.SysLogHandler('/dev/log', facility=LOG_LOCAL0)

log_format = 'GD: %(name)-12s %(levelname)-7s %(message)s'
log_syslog.setFormatter(logging.Formatter(log_format))

root_logger.addHandler(log_syslog)

