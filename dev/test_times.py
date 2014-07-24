#!/usr/bin/env python2.7

import sys
sys.path.insert(0, '..')

import datetime
import time
import dateutil.tz

from gdrivefs.conf import Conf
Conf.set('auth_cache_filepath', '/var/cache/creds/gdfs')

import gdrivefs.gdfs.gdfuse
import gdrivefs.gdtool.drive
import gdrivefs.time_support

auth = gdrivefs.gdtool.drive.GdriveAuth()
client = auth.get_client()

def get_phrase(epoch):
    dt = datetime.datetime.utcfromtimestamp(entry.modified_date_epoch)
    return datetime.datetime.strftime(dt, gdrivefs.time_support.DTF_DATETIMET)

print("Before:\n")

(entry, path, filename) = gdrivefs.gdfs.gdfuse.get_entry_or_raise(
                            '/20140426-171136')

print(entry.modified_date)
print(entry.modified_date.utctimetuple())
print(entry.modified_date_epoch)

print("From epoch: %s" % (get_phrase(entry.modified_date_epoch)))

print("\nSending:\n")

now = time.time()
(atime, mtime) = (now, now)

mtime_phrase = gdrivefs.time_support.get_flat_normal_fs_time_from_epoch(mtime)
atime_phrase = gdrivefs.time_support.get_flat_normal_fs_time_from_epoch(atime)

print("mtime: %s" % (mtime_phrase))

entry = gdrivefs.gdtool.drive.drive_proxy('update_entry', 
            normalized_entry=entry, 
            modified_datetime=mtime_phrase,
            accessed_datetime=atime_phrase)

print("\nAfter:\n")

print(entry.modified_date)
print(entry.modified_date_epoch)

print("From epoch: %s" % (get_phrase(entry.modified_date_epoch)))

print("Done.")

#response = client.files().get(fileId='1xxGrmEAv4-2ZM1MYj4UXpnxUp73d2VmtI9TdFERrSbM').execute()

#            entry = gdrivefs.gdtool.drive.drive_proxy('update_entry', normalized_entry=entry, 
#                                modified_datetime=mtime_phrase,
#                                accessed_datetime=atime_phrase)

