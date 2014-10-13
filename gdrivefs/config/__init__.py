import os

IS_DEBUG = bool(int(os.environ.get('DEBUG', '0')))
