#!/usr/bin/env python2.7

import sys
sys.path.insert(0, '..')

from pprint import pprint

from gdrivefs.gdfs.gdfuse import set_auth_cache_filepath
from gdrivefs.gdtool.drive import GdriveAuth

auth = GdriveAuth()
client = auth.get_client()

#client.files()
#sys.exit()

#response = client.about().get().execute()
#sys.exit()

#request = client.files().media_get()
#response = client.files().list().execute()
#response = client.files().get(fileId='1xxGrmEAv4-2ZM1MYj4UXpnxUp73d2VmtI9TdFERrSbM').execute()

#pprint(response.keys())

#for entry in response['items']:
#    pprint(dir(entry))
#    sys.exit()

#pprint(dir(response))

from gdrivefs.gdtool.download_agent import get_download_agent_external,\
                                           DownloadRequest
from gdrivefs import TypedEntry
from gdrivefs.time_support import get_normal_dt_from_rfc3339_phrase

from time import sleep
from datetime import datetime
from dateutil.tz import tzutc

dae = get_download_agent_external()

dae.start()

te = TypedEntry(entry_id='0B5Ft2OXeDBqSRGxHajVMT0pob1k', 
                mime_type='application/pdf')
url='https://doc-0c-1c-docs.googleusercontent.com/docs/securesc/svig2vvms8dc5kautokn617oteonvt69/vaj2tcji2mjes3snt7t8brteu3slfqhp/1394452800000/06779401675395806531/06779401675395806531/0B5Ft2OXeDBqSRGxHajVMT0pob1k?h=16653014193614665626&e=download&gd=true'
mtime_dt = get_normal_dt_from_rfc3339_phrase('2014-03-09T19:46:25.191Z')

dr = DownloadRequest(typed_entry=te, 
                     url=url, 
                     bytes=None, 
                     expected_mtime_dt=mtime_dt)

with dae.sync_to_local(dr) as f:
    print("Yielded: %s" % (f))

try:
    while 1:
        sleep(1)
except KeyboardInterrupt:
    print("Test loop has ended.")

dae.stop()

