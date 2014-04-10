from collections import namedtuple

from gdrivefs import log_config

TypedEntry = namedtuple('TypedEntry', ['entry_id', 'mime_type'])


from ._version import get_versions
__version__ = get_versions()['version']
del get_versions
