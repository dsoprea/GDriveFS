import logging

from getpass import getuser

current_user = getuser()

if current_user == 'root':
    log_filepath = '/var/log/gdrivefs.log'

else:
    log_filepath = 'gdrivefs.log'

logging.basicConfig(
        level       = logging.DEBUG, 
        format      = '%(asctime)s  %(levelname)s %(message)s',
        filename    = log_filepath
    )

