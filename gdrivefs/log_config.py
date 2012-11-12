import logging

from getpass import getuser
from os import environ
from os.path import dirname, exists

import gdrivefs
app_path = dirname(gdrivefs.__file__)

log_paths = [('%s/logs' % (app_path)), 
             '/var/log/gdrivefs', 
            ]

log_filename = 'gdrivefs.log'

format = '%(asctime)s  %(levelname)s %(message)s'

for log_path in log_paths:
    if exists(log_path):
        logging.basicConfig(
                level       = logging.DEBUG,
                format      = format,
                filename    = ('%s/%s' % (log_path, log_filename))
            )

        break

# Hook console logging.
#if ('SW_DEBUG' in environ and environ['SW_DEBUG'] or
#    'RI_DEBUG' in environ and environ['RI_DEBUG']
#   ) and 'RI_CONSOLE_LOG_ACTIVE' not in environ:
#    log_console = logging.StreamHandler()
#    log_console.setLevel(logging.DEBUG)
#    log_console.setFormatter(logging.Formatter(format))
#
#    logging.getLogger('').addHandler(log_console)
#
#    environ['RI_CONSOLE_LOG_ACTIVE'] = '1'

