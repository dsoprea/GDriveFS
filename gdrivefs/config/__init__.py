import os

IS_DEBUG = bool(int(os.environ.get('GD_DEBUG', '0')))
