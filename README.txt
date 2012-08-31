GDriveFS
========

An innovative FUSE wrapper for Google Drive.

**This project is under active development**. It is currently incomplete. To 
the outsider, it is only sufficient as a working example of the concepts 
involved.

Design goals:

> Limited caching.
> Cleanup thread to manage cleanup of aged cache items.
> Thread for monitoring changes via "changes" functionality of API.
x Complete stat() implementation.
x Seamlessly work around duplicate-file allowances in Google Drive. (DONE)
x Seamlessly manage file-type versatility in Google Drive (files do not retain 
  a particular mime-type under GD). (DONE)
x Allow for multiple references to the same files. (DONE)
x Allow copy-from using default formats as well as allowing one to be chosen on-the-fly. (DONE)

Also, a design choice of other implementations is to make the user get API keys 
for Google Drive, and this doesn't sense. Our implementation is built against 
OAuth 2.0 as a native application. You should just have to visit the 
authorization URL once, plug-in the auth-code, and be done with it.

Format Management
=================

Google Drive will store Google Document files without a standard format. If 
you wish to download them, you have to select which format you'd like to 
download it as. One of the more exciting features of this FUSE implementation 
is the flexibility in both downloading in a default format, while still 
allowing you to elect a different format on the fly. See the section below
labeled "Displaceables".

The following is an example directory-listing, as a result of the above. In
addition, notice the following features:

> Manages duplicates by appending index numbers (e.g. "<filename> (2)").
> Mtimes, permissions, and ownership are correct.
> Sizes are zero for file-types that Google hosts free of charge. These are 
  always the files that don't have a strict, default format (the length is 
  unknown).
> Hidden files are prefixed with ".", thus hiding them from normal listings.
> "Trashed" files are excluded from listings.

dustin@host1:~$ sudo ls -la /tmp/test
total 4
drwxrwxrwx  2 root root     0 Nov 12  2008 .
drwxrwxrwt 14 root root  4096 Aug 31 01:39 ..
-rw-rw-rw-  1 root root  1000 Feb 19  2011 .12-9-10 great north partial list-no iga.xls
-rw-rw-rw-  1 root root  1000 Aug 23 07:28 Copy of Little League Newsletter.txt
-rw-rw-rw-  1 root root  1000 Mar  6  2010 Current Company Agenda.txt
drwxrwxrwx  2 root root     0 Aug 26 08:36 HelloFax
drwxrwxrwx  2 root root     0 Apr 24 18:40 HelloFax (2)
-rw-rw-rw-  1 root root  1000 Nov 28  2011 Imported from Google Notebook - My Notebook.txt
drwxrwxrwx  2 root root     0 Nov 21  2008 New Folder
-rw-rw-rw-  1 root root  1000 May 13  2010 Provisioning Letter.txt
-rw-rw-rw-  1 root root 45056 Oct 26  2011 Resume 20111026.doc
-rw-rw-rw-  1 root root  1000 Apr 21  2010 RHT Testimonial 2005- 2003.txt
-rw-rw-rw-  1 root root  1000 Oct 20  2010 searches - standard.xls
-rw-rw-rw-  1 root root  3234 Dec 23  2011 testOnDemandRTSPServer.cpp.gz
drwxrwxrwx  2 root root     0 Aug 26 08:36 Untitled document
-rw-rw-rw-  1 root root  1000 Aug 20 08:24 Untitled document.txt
-rw-rw-rw-  1 root root  1000 Aug 20 08:25 Untitled document.txt (2)


Displaceables
=============

Google Drive is where any documents are written from Google Documents. Google 
will store these files in an agnostic file whose format will not be determined 
until you download it in a specific format. Because the file is not stored in a 
particular format, it doesn't have a size. Because it doesn't have a size, all 
data returned from a read() operation will be ignored.

To get around this, a read of these types of files will instead write the 
requested file to a temporary location, and always return a 
stub with file-information. This stub is always/exactly 1000 bytes long (padded 
with spaces), and the temporary location is available under "FilePath":

$ cat /tmp/hello/Maceo_new_resume11.txt
{"Length": 22743, "FilePath": "/tmp/gdrivefs/1SvieoVpe_Gcfr7qG0px5juaqzmey08mTD5d7IVKjWU.textplain", "EntryId": "1SvieoVpe_Gc-fr7qG0px5juaqzmey08mTD5d7IVKjWU", "Displaceable": true, "Title": "Maceo_new_resume11", "Labels": {"restricted": false, "starred": false, "viewed": true, "hidden": false, "trashed": false}, "OriginalMimeType": "application/vnd.google-apps.document", "ExportTypes": ["text/html", "text/plain", "application/vnd.oasis.opendocument.text", "application/rtf", "application/pdf", "application/msword"], "FinalMimeType": "text/plain"} 

Because of this, any file returned from GD without a filesize is referred to as 
a "displaceable" file.

NOTE: Similar stub information can be displayed for any file by appending "$" 
      to the requested filename.

Export Formats
==============

To access a file in something other than the default format, append 
"#<file extension>" to the filename:

    cp "searches - standard.xls#pdf" "/tmp/output.pdf"

A list of available MIME-types can be seen under "ExportTypes" in the stub info. 
The effective MIME-type that will be used can be found under "FinalMimeType".


Dustin Oprea
myselfasunder, gmail.com

