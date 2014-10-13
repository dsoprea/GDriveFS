import os
import logging
import logging.handlers

import gdrivefs.config

_FMT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
_FORMATTER = logging.Formatter(_FMT)

logger = logging.getLogger()

sh = logging.StreamHandler()
sh.setFormatter(_FORMATTER)
logger.addHandler(sh)

sh2 = logging.handlers.SysLogHandler()
sh2.setFormatter(_FORMATTER)
logger.addHandler(sh2)

if gdrivefs.config.IS_DEBUG is True:
    logger.setLevel(logging.DEBUG)
else:
    logger.setLevel(logging.WARNING)

