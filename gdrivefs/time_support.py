from math import floor
from datetime import datetime
from dateutil.tz import tzlocal, tzutc

DTF_DATETIME = '%Y%m%d-%H%M%S'
DTF_DATETIMET = '%Y-%m-%dT%H:%M:%S'
DTF_DATE = '%Y%m%d'
DTF_TIME = '%H%M%S'

def build_rfc3339_phrase(datetime_obj):
    datetime_phrase = datetime_obj.strftime(DTF_DATETIMET)
    us = datetime_obj.strftime('%f')

    seconds = datetime_obj.utcoffset().total_seconds()

    if seconds is None:
        datetime_phrase += 'Z'
    else:
        # Append: decimal, 6-digit uS, -/+, hours, minutes
        datetime_phrase += ('.%.6s%s%02d:%02d' % (
                            us.zfill(6),
                            ('-' if seconds < 0 else '+'),
                            abs(int(floor(seconds / 3600))),
                            abs(seconds % 3600)
                            ))

    return datetime_phrase

def get_normal_dt_from_epoch(epoch):
    dt = datetime.fromtimestamp(epoch, tzlocal())
    return normalize_dt(dt)

def normalize_dt(dt=None):
    if dt is None:
        dt = datetime.now()

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=tzlocal())

    return dt.astimezone(tzutc())

def get_flat_normal_fs_time_from_dt(dt=None):
    if dt is None:
        dt = datetime.now()

    dt_normal = normalize_dt(dt)
    return build_rfc3339_phrase(dt_normal)

def get_flat_normal_fs_time_from_epoch(epoch):
    dt_normal = get_normal_dt_from_epoch(epoch)
    return build_rfc3339_phrase(dt_normal)

