** IN DEVELOPMENT **

GDriveFS
========

An innovative FUSE wrapper for Google Drive developed under Python 2.7 . Work
has been done to start making GDriveFS compatible with Python 3, but most of
this will probably be completed only after initial development has been 
finished.

**This project is under active development**. It is currently incomplete. To 
the outsider, it is only sufficient as a working example of the concepts 
involved.

Design goals:

> Cleanup thread to manage cleanup of aged cache items.
x Thread for monitoring changes via "changes" functionality of API. (DONE)
x Complete stat() implementation. (DONE)
x Seamlessly work around duplicate-file allowances in Google Drive. (DONE)
x Seamlessly manage file-type versatility in Google Drive (Google Doc files do 
  not have a particular format). (DONE)
x Allow for many-to-one references on the files. (DONE)

Also, a design choice of other implementations is to make the user get API keys 
for Google Drive, and this doesn't sense. Our implementation is built against 
OAuth 2.0 as a native application. You should just have to visit the 
authorization URL once, plug-in the auth-code, and be done with it.

Installation
============

Via PyPi:

  sudo pip install gdrivefs

Manually:

  Expand into a directory named "gdrivefs" in the Python path, and run:
  
    python setup.py

Usage
=====

Before you can mount the account, you must authorize GDriveFS to access it. 
GDriveFS works by producing a URL that you must visit in a browser. Google will
ask for your log-in information and authorization, and then give you an author-
ization code. You then pass this code back to the GDriveFS utility along with
a file-path of where you want it to store the authorization information ("auth
storage file"). Then, you can mount it whenever you'd like.

Since this is FUSE, you must be running as root to mount.

1) To get the authorization URL:

  gdfstool auth -u

  Output:

    To authorize FUSE to use your Google Drive account, visit the following URL to produce an authorization code:

    https://accounts.google.com/o/oauth2/auth?scope=https%3A%2F%2Fwww.googleapis.com%2Fauth%2Fdrive+https%3A%2F%2Fwww.googleapis.com%2Fauth%2Fdrive.file&redirect_uri=urn%3Aietf%3Awg%3Aoauth%3A2.0%3Aoob&response_type=code&client_id=626378760250.apps.googleusercontent.com&access_type=offline
    
2) To set the authorization-code, you must also provide the auth-storage file 
   that you would like to save it as. The name and location of this file is 
   arbitrary:

  gdfstool auth -a /var/cache/gdfs/credcache "4/WUsOa-Sm2RhgQCQStf9_NFAMMbRC.cj4LQYdXFdwfshQV0ieZDAqA-C7ecwI"

  Output:

    Authorization code recorded.

3) There are three ways to mount the account:

  a) Via script (either using the main script "gdfstool mount" or the helper 
     scripts "gdfs"/"mount.gdfs"):

    gdfs [-h] [-d] [-o OPT] auth_storage_file mountpoint

  b) Via /etc/fstab:

     /var/cache/gdfs/credcache /tmp/hello gdfs defaults 0 0

  c) Directly via gdfstool:

    gdfstool mount /var/cache/gdfs/credcache /tmp/hello

Options
=======

Any of the configuration values in conf.Conf can be overwritten as "-o" 
options. You may pass the full array of FUSE options this way, as well.

Format Management
=================

Google Drive will store Google Document files without a standard format. If 
you wish to download them, you have to select which format you'd like to 
download it as. One of the more exciting features of this FUSE implementation 
is the flexibility in choosing which format to download on the fly. See the 
section below labeled "Displaceables".

The following is an example directory-listing, as a result of the above. In
addition, notice the following features:

> Manages duplicates by appending index numbers (e.g. "<filename> (2)").
> Mtimes, permissions, and ownership are correct.
> Sizes are zero for file-types that Google hosts free of charge. These are 
  always the files that don't have a strict, default format (the length is 
  unknown).
> Hidden files are prefixed with ".", thus hiding them from normal listings.
> "Trashed" files are excluded from listings.
> Any file that will require a mime-type in order to be downloaded has a "#" as
  the last character of its filename.

dustin@host1:~$ sudo ls -la /tmp/test
total 4
drwxrwxrwx  2 root root     0 Nov 12  2008 .
drwxrwxrwt 14 root root  4096 Aug 31 01:39 ..
-rw-rw-rw-  1 root root  1000 Feb 19  2011 .12-9-10 great north partial list-no iga.xls#
-rw-rw-rw-  1 root root  1000 Aug 23 07:28 Copy of Little League Newsletter.txt
-rw-rw-rw-  1 root root  1000 Mar  6  2010 Current Company Agenda.txt
drwxrwxrwx  2 root root     0 Aug 26 08:36 HelloFax
drwxrwxrwx  2 root root     0 Apr 24 18:40 HelloFax (2)
-rw-rw-rw-  1 root root  1000 Nov 28  2011 Imported from Google Notebook - My Notebook.txt
drwxrwxrwx  2 root root     0 Nov 21  2008 New Folder
-rw-rw-rw-  1 root root  1000 May 13  2010 Provisioning Letter.txt
-rw-rw-rw-  1 root root 45056 Oct 26  2011 Resume 20111026.doc#
-rw-rw-rw-  1 root root  1000 Apr 21  2010 RHT Testimonial 2005- 2003.txt
-rw-rw-rw-  1 root root  1000 Oct 20  2010 searches - standard.xls#
-rw-rw-rw-  1 root root  3234 Dec 23  2011 testOnDemandRTSPServer.cpp.gz
drwxrwxrwx  2 root root     0 Aug 26 08:36 Untitled document
-rw-rw-rw-  1 root root  1000 Aug 20 08:24 Untitled document.txt
-rw-rw-rw-  1 root root  1000 Aug 20 08:25 Untitled document.txt (2)


Downloaded Google Document Files
================================

  Example:

    root@dustintank:/mnt/gdrivefs# cp Copy\ of\ Dear\ Biola.docx#application+pdf /target

Displaceables
=============

Google Documents stores all of its data on Google Drive. Google will store 
these files in an agnostic file entry whose format will not be determined until 
you download it in a specific format. Because the file is not stored in a 
particular format, it doesn't have a size. Because it doesn't have a size, the 
OS will not issue reads for more than (0) bytes. 

To get around this, a read of these types of files will only return exactly 
1000 bytes of JSON-encoded "stub data".. Information about the entry, including 
the file-path that we've stored it to. This example also shows how we've 
specified a mime-type in order to get a PDF version of a Google Document file.

root@dustintank:/mnt/gdrivefs# cp Copy\ of\ Dear\ Biola.docx#application+pdf /target

root@dustintank:/mnt# cat /tmp/Copy\ of\ Dear\ Biola.docx#application+pdf 
{"ImageMediaMetadata": null, 
 "Length": 58484, 
 "FilePath": "/tmp/gdrivefs/displaced/Copy of Dear Biola.docx.application+pdf", 
 "EntryId": "1Ih5yvXiNN588EruqrzBv_RBvsKbEvcyquStaJuTZ1mQ", 
 "Title": "Copy of Dear Biola.docx", 
 "RequiresMimeType": true, 
 "Labels": {"restricted": false, 
            "starred": false, 
            "viewed": true, 
            "hidden": false, 
            "trashed": false}, 
 "OriginalMimeType": "application/vnd.google-apps.document", 
 "ExportTypes": ["text/html", 
                 "application/pdf", 
                 "application/vnd.openxmlformats-officedocument.wordprocessingml.document", 
                 "application/vnd.oasis.opendocument.text", 
                 "application/rtf", "text/plain"], 
 "FinalMimeType": "application/pdf"}

From this, you can tell that the file was originally a Google Documents' 
mimetype, and now its a PDF mime-type. You can also see various flags, as well 
as the location that the actual, requested file was stored to.


Cache/Change Management
=======================

No cache is maintained. Updates are performed every few seconds using GD's
"change" functionality.

Misc Notes
==========

A file will be marked as hidden on Google Drive if it has a prefixing dot. 
However, Linux/Unix doesn't care about the "hidden" attribute. If you create a 
file on Google Drive, somewhere else, and want it to truly be hidden via this 
software, make sure you add the prefixing dot.

Me
==

Dustin Oprea
myselfasunder, gmail.com

