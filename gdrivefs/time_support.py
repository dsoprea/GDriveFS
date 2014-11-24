from math import floor
from datetime import datetime
from dateutil.tz import tzlocal, tzutc

DTF_DATETIME = '%Y%m%d-%H%M%S'
DTF_DATETIMET = '%Y-%m-%dT%H:%M:%S'
DTF_DATE = '%Y%m%d'
DTF_TIME = '%H%M%S'

def get_normal_dt_from_rfc3339_phrase(phrase):
    stripped = phrase[:phrase.rindex('.')]
    dt = datetime.strptime(stripped, DTF_DATETIMET).replace(tzinfo=tzutc())

#    print("get_normal_dt_from_rfc3339_phrase(%s) => %s" % (phrase, dt))

    return dt

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

#    print("build_rfc3339_phrase(%s) => %s" % (datetime_obj, datetime_phrase))
    return datetime_phrase

def get_normal_dt_from_epoch(epoch):
    dt = datetime.fromtimestamp(epoch, tzlocal())
    normal_dt = normalize_dt(dt)

#    print("get_normal_dt_from_epoch(%s) => %s" % (epoch, normal_dt))
    return normal_dt

def normalize_dt(dt=None):
    if dt is None:
        dt = datetime.now()

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=tzlocal())

    normal_dt = dt.astimezone(tzutc())

#    print("normalize_dt(%s) => %s" % (dt, normal_dt))
    return normal_dt

def get_flat_normal_fs_time_from_dt(dt=None):
    if dt is None:
        dt = datetime.now()

    dt_normal = normalize_dt(dt)
    flat_normal = build_rfc3339_phrase(dt_normal)

#    print("get_flat_normal_fs_time_from_dt(%s) => %s" % (dt, flat_normal))
    return flat_normal

def get_flat_normal_fs_time_from_epoch(epoch):
    dt_normal = get_normal_dt_from_epoch(epoch)
    flat_normal = build_rfc3339_phrase(dt_normal)

#    print("get_flat_normal_fs_time_from_epoch(%s) => %s" % (epoch, flat_normal))
    return flat_normal
