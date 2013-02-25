from math import floor

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

