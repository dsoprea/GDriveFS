GDriveFS
========

An innovative FUSE wrapper for Google Drive developed under Python 2.7 . Work
has been done to start making GDriveFS compatible with Python 3, but most of
this will probably be completed only after initial development has been 
finished.

UPDATE:

This project is under active development, but should now be mostly functionally 
complete (less a couple of more minor FUSE calls). I could use some help in 
testing it.


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


IMPORTANT
=========

Both PyPI and the Google Code downloads for google_api_python_client have an
old version of their libraries, prior to when they fixed some Unicode problems
that might cause failure when dealing with downloads/uploads of certain types
of files.

To install from Mercurial, do the following:

  hg clone https://code.google.com/p/google-api-python-client

  cd google-api-python-client
  sudo python setup.py install
  sudo python setup.py install_egg_info


Installation
============

Via PyPi:

  sudo pip install gdrivefs

Manually:

  Expand into a directory named "gdrivefs" in the Python path, and run:
  
    sudo python setup.py install
    sudo python setup.py install_egg_info

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

     gdfs -o allow_other /var/cache/gdrivefs.auth /mnt/gdrivefs

  b) Via /etc/fstab:

     /var/cache/gdrivefs.auth /mnt/gdrivefs gdfs allow_other 0 0

  c) Directly via gdfstool:

    gdfstool mount /var/cache/gdrivefs.auth /mnt/gdrivefs


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

If a mime-type isn't provided when requesting a file that requires a mime-type 
in order to download, GDFS will make a guess based on whether the extension in 
the filename (if one exists) can be mapped to a mime-type that is available 
among the export-types provided by GD for that specific file.


The following is an example directory-listing. Notice the following features:

> Manages duplicates by appending index numbers (e.g. "<filename> (2)").
> Mtimes, permissions, and ownership are correct.
> Sizes are zero for file-types that Google hosts free of charge. These are 
  always the files that don't have a strict, default format (the length is 
  unknown).
> Hidden files are prefixed with ".", thus hiding them from normal listings.
> "Trashed" files are excluded from listings.
> Any file that will require a mime-type in order to be downloaded has a "#" as
  the last character of its filename.

-rw-rw-rw- 1 root root       0 Feb 17 07:52 20130217-145200
-rw-rw-rw- 1 root root       0 Feb 17 08:04 20130217-150358
-rw-rw-rw- 1 root root  358356 Feb 15 15:06 American-Pika-with-Food.jpg
-rw-rw-rw- 1 root root    1000 Oct 25 03:53 Dear Biola.docx#
-rw-rw-rw- 1 root root    1000 Oct 25 02:47 Dear Biola.docx (1)#
-rw-rw-rw- 1 root root    1000 Oct 15 14:29 Reflection.docx#
-rw-rw-rw- 1 root root 1536036 Nov 28 22:37 lotterynumbers01.png
drwxrwxrwx 2 root root    4096 Oct  4 06:08 Scratchpad#
drwxrwxrwx 2 root root    4096 Dec  1 19:21 testdir_1421#
-rw-rw-rw- 1 root root       5 Dec  2 08:50 testfile_0350
-rw-rw-rw- 1 root root       0 Dec  2 21:17 .testfile_0417.swp
-rw-rw-rw- 1 root root       0 Dec  3 00:38 testfile_1937
-rw-rw-rw- 1 root root       0 Dec  2 23:13 testfile_hidden_1812
-rw-rw-rw- 1 root root    1000 Oct  4 02:13 Untitled document#


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

# cp Copy\ of\ Dear\ Biola.docx#application+pdf /target

# cat /tmp/Copy\ of\ Dear\ Biola.docx#application+pdf 
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


Extended Attributes
===================

Extended attributes allow access to arbitrary, filesystem-specific data. You 
may access any of the properties that Google Drive provides for a given entry, 
plus a handful of extra ones. The values are JSON-encoded.

    Listing attributes:

        $ getfattr American-Pika-with-Food.jpg

        # file: American-Pika-with-Food.jpg
        user.extra.download_types
        user.extra.is_directory
        user.extra.is_visible
        user.extra.parents
        user.original.alternateLink
        user.original.createdDate
        user.original.downloadUrl
        user.original.editable
        user.original.etag
        user.original.fileExtension
        user.original.fileSize
        user.original.iconLink
        user.original.id
        user.original.imageMediaMetadata
        user.original.kind
        user.original.labels
        user.original.lastModifyingUser
        user.original.lastModifyingUserName
        user.original.md5Checksum
        user.original.mimeType
        user.original.modifiedByMeDate
        user.original.modifiedDate
        user.original.originalFilename
        user.original.ownerNames
        user.original.owners
        user.original.parents
        user.original.quotaBytesUsed
        user.original.selfLink
        user.original.shared
        user.original.thumbnailLink
        user.original.title
        user.original.userPermission
        user.original.webContentLink
        user.original.writersCanShare

    Getting specific attribute:

        $ getfattr --only-values -n user.original.id American-Pika-with-Food.jpg | json_reformat 

        "0B5Ft2OXeDBqSSGFIanJ2Z2c3RWs"

        $ getfattr --only-values -n user.original.modifiedDate American-Pika-with-Food.jpg | json_reformat 

        "2013-02-15T15:06:09.691Z"

        $ getfattr --only-values -n user.original.labels American-Pika-with-Food.jpg | json_reformat 

        {
          "restricted": "False",
          "starred": "False",
          "trashed": "False",
          "hidden": "False",
          "viewed": "False"
        }

    You can use PHP to extract information from the JSON at the command-line:

        $ getfattr --only-values -n user.original.id \
            gdrivefs/American-Pika-with-Food.jpg | \
            php -r "print(json_decode(fgets(STDIN)));"

          Returns: 0B5Ft2OXeDBqSSGFIanJ2Z2c3RWs

        $ getfattr --only-values -n user.original.labels \
            gdrivefs/American-Pika-with-Food.jpg | \
            php -r "print(json_decode(fgets(STDIN))->restricted);"

          Returns: False


Misc Notes
==========

A file will be marked as hidden on Google Drive if it has a prefixing dot. 
However, Linux/Unix doesn't care about the "hidden" attribute. If you create a 
file on Google Drive, somewhere else, and want it to truly be hidden via this 
software, make sure you add the prefixing dot.


Dustin Oprea
dustin, randomingenuity.com

