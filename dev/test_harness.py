#!/usr/bin/env python2.7

import sys
sys.path.insert(0, '.')

from pprint import pprint

from gdrivefs.gdfs.gdfuse import set_auth_cache_filepath
from gdrivefs.gdtool.drive import GdriveAuth

auth = GdriveAuth()
client = auth.get_client()

#print(dir(client.files()))
#sys.exit()

#response = client.about().get().execute()
#request = client.files().media_get()
#response = client.files().list().execute()
#response = client.files().get(fileId='1xxGrmEAv4-2ZM1MYj4UXpnxUp73d2VmtI9TdFERrSbM').execute()

#pprint(response.keys())

#for entry in response['items']:
#    pprint(dir(entry))
#    sys.exit()

#pprint(dir(response))

from gdrivefs.gdtool.download_agent import download_agent_external
from time import sleep

download_agent_external.start()

try:
    while 1:
        sleep 1
except KeyboardInterrupt:
    print("Test loop has ended.")

download_agent_external.stop()

