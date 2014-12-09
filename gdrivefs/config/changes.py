import os

MONITOR_CHANGES = bool(int(os.environ.get('GD_MONITOR_CHANGES', '1')))
