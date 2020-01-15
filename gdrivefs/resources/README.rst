|Build\_Status|

GDriveFS is an innovative *FUSE* wrapper for *Google Drive*. It is Python 2/3 compatible.


--------------
Latest Changes
--------------

- Though you can still use the previous authorization flow, there is now a very simple authorization flow that may be used instead by using the 'auth_automatic' subcommand on the 'gdfstool'. Whe you run this command, the browser will automatically be opened, you may or may not be prompted for authorization by Google, a redirection will occur, and we will then automatically record the authorization code. GDFS will temporarily open a small webserver on a random port in order to receive the response. **This effectively makes authorization a one-step process for the user.** See below for more details.

- There is now a default file-path for the credentials ("auth storage file"). Just use "default" and "$HOME/.gdfs/creds" will be the file-path used. See below for more details.

- The 'auth' subcommand on the 'gdfstool' command is now obsolete. Though you may continue to use this subcommand, please start using the 'auth_get_url' and 'auth_write' subcommands as this subcommand will be removed in the future.


------------
Installation
------------

In order to install his, we're going to use PIP (to access PyPI). Under Ubuntu,
this is done via::

    $ sudo apt-get install python-pip

You'll also need to equip your system to perform builds in order to install
some of the dependencies. Under Ubuntu, this is done via::

    $ sudo apt-get install build-essential python-dev

Now, to install GDriveFS::

    $ sudo pip install gdrivefs


-----
Usage
-----

Important
=========

If you wish to mount your Google Drive account at bootup, we recommend that you wrap it as a service. If you list it in /etc/fstab, you have bootup issues since your networking system may not be active when your filesystems are mounted unless you have deliberately configured the order in order to give you a guarantee.


Overview
========

Before you can mount the account, you must authorize *GDriveFS* to access it.
*GDriveFS* works by producing a URL that you must visit in a browser. Google
will ask for your log-in information and authorization, and then give you an
authorization code. You then pass this code back to the *GDriveFS* utility
along with a file-path of where you want it to store the authorization
information ("auth storage file" or "credentials file"). Then, you can mount it
whenever you'd like.

Since this is *FUSE*, you must be running as root or under the right group to mount.


Credentials File
================

Also referred to as the "auth storage" file.

In previous versions, you were required to provide a file-path to write and read the authorization code to. There is now a default ($HOME/.gdfs/creds). Just literally use the string "default" whereever the credentials file-path is required in order to use this default file-path.


Automatic Authorization Flow
----------------------------

There is now a simplified flow that will automatically open the system Web browser, do any authentication necessary, and automatically write the authorization-code to disk::

    $ gdfstool auth_automatic
    Authorization code recorded.

    $ gdfs default /mnt/gdrivefs

This automatic flow will require GDFS to temporarily start a small, internal webserver on the first available port.


Manual Authorization Flow
-------------------------


If you need to manually get the URL, browse to it, get the authorization code, and then call the 'auth_write' subcommand to store it:

1. To get an authorization URL::

    $ gdfstool auth_get_url
    To authorize FUSE to use your Google Drive account, visit the following URL to produce an authorization code:

    https://accounts.google.com/o/oauth2/auth?scope=https%3A%2F%2Fwww.googleapis.com%2Fauth%2Fdrive+https%3A%2F%2Fwww.googleapis.com%2Fauth%2Fdrive.file&redirect_uri=urn%3Aietf%3Awg%3Aoauth%3A2.0%3Aoob&response_type=code&client_id=626378760250.apps.googleusercontent.com&access_type=offline

2. To set the authorization-code, you must also provide the auth-storage file
   that you would like to save it as. The name and location of this file is
   arbitrary::

    $ gdfstool auth_write "4/WUsOa-Sm2RhgQtf9_NFAMMbRC.cj4LQYdXfshQV0ieZDAqA-C7ecwI"
    Authorization code recorded.


Mounting
========

Once you're ready to mount::

    $ gdfs -o allow_other default /mnt/gdrivefs


Optimization
============

By default, FUSE uses a very conservative block-size. On systems that support it, you may elect to use the "big_writes" option. This may dramatically increase the block-size (which improves the speed of transfers). There doesn't appear to be any authoritative documentation as to what systems support it or what the improvements might be, but, so far, it seems like Linux supports it, OSX doesn't, and FUSE will go from using 4K blocks to using 64K blocks.

To use this, pass "big_writes" in the "-o" option-string::

    $ sudo gdfs -o big_writes /home/user/.gdfs/creds /mnt/gdrivefs


Vagrant
=======

A Vagrantfile has been made available in the event that you would like to mount your account from a system that isn't FUSE compatible (like a Mac) or you are having issues installing GDriveFS somewhere else and would like to debug.

To install Vagrant::

    $ sudo apt-get install vagrant

To start and provision the instance::

    $ cd gdrivefs/vagrant
    $ vagrant up
    Bringing machine 'default' up with 'virtualbox' provider...
    ==> default: Importing base box 'ubuntu/trusty64'...
    ==> default: Matching MAC address for NAT networking...
    ==> default: Checking if box 'ubuntu/trusty64' is up to date...
    ==> default: Setting the name of the VM: vagrant_default_1413437502948_22866
    ==> default: Clearing any previously set forwarded ports...
    ==> default: Clearing any previously set network interfaces...
    ==> default: Preparing network interfaces based on configuration...
        default: Adapter 1: nat
    ==> default: Forwarding ports...

    ...

    ==> default: Using /usr/lib/python2.7/dist-packages
    ==> default: Finished processing dependencies for gdrivefs==0.13.14
    ==> default: To authorize FUSE to use your Google Drive account, visit the following URL to produce an authorization code:
    ==> default:
    ==> default: https://accounts.google.com/o/oauth2/auth?scope=https%3A%2F%2Fwww.googleapis.com%2Fauth%2Fdrive+https%3A%2F%2Fwww.googleapis.com%2Fauth%2Fdrive.file&redirect_uri=urn%3Aietf%3Awg%3Aoauth%3A2.0%3Aoob&response_type=code&client_id=1056816309698.apps.googleusercontent.com&access_type=offline
    ==> default:
    ==> default: Once you have retrieved your authorization string, run:
    ==> default:
    ==> default: sudo gdfstool auth_write <authcode>
    ==> default:

This may take a few more minutes the first time, as it might need to acquire the Ubuntu 14.04 image if not already available.

To log into the guest instance::

    $ vagrant ssh

The GDFS source directory will be mounted at `/gdrivefs`, and the scripts will be in the path.

**If you're familiar with Vagrant, you can copy the Vagrantfile and modify it to mount an additional path from the host system in the guest instance, and then use this to access your files from an incompatible system.**


Developing/Debugging
====================

Mounting GDFS in debugging-mode will run GDFS in the foreground, and enable debug-logging.

Just set the `GD_DEBUG` environment variable to "1"::

    root@vagrant-ubuntu-trusty-64:/home/vagrant# GD_DEBUG=1 gdfs /home/user/.gdfs/creds /mnt/g
    2014-12-09 04:09:17,204 [gdrivefs.utility INFO] No mime-mapping was found.
    2014-12-09 04:09:17,204 [gdrivefs.utility INFO] No extension-mapping was found.
    2014-12-09 04:09:17,258 [__main__ DEBUG] Mounting GD with creds at [/home/user/.gdfs/creds]: /mnt/g
    2014-12-09 04:09:17,259 [root DEBUG] Debug: True
    2014-12-09 04:09:17,260 [root DEBUG] PERMS: F=777 E=666 NE=444
    2014-12-09 04:09:17,262 [gdrivefs.drive DEBUG] Getting authorized HTTP tunnel.
    2014-12-09 04:09:17,262 [gdrivefs.drive DEBUG] Got authorized tunnel.
    FUSE library version: 2.9.2
    nullpath_ok: 0
    nopath: 0
    utime_omit_ok: 0
    unique: 1, opcode: INIT (26), nodeid: 0, insize: 56, pid: 0
    INIT: 7.22
    flags=0x0000f7fb
    max_readahead=0x00020000
    2014-12-09 04:09:22,839 [gdrivefs.fsutility DEBUG] --------------------------------------------------
    2014-12-09 04:09:22,841 [gdrivefs.fsutility DEBUG] >>>>>>>>>> init(23) >>>>>>>>>> (0)
    2014-12-09 04:09:22,841 [gdrivefs.fsutility DEBUG] DATA: path= [/]
    2014-12-09 04:09:22,842 [gdrivefs.gdfuse INFO] Activating change-monitor.
    2014-12-09 04:09:23,002 [gdrivefs.fsutility DEBUG] <<<<<<<<<< init(23) (0)
       INIT: 7.19
       flags=0x00000011


Troubleshooting Steps
=====================

- If your *setuptools* package is too old, you might see the following
  [annoying] error::

    error: option --single-version-externally-managed not recognized

  See `What does “error: option --single-version-externally-managed not recognized” indicate? <http://stackoverflow.com/questions/14296531/what-does-error-option-single-version-externally-managed-not-recognized-ind>`_.

  Apparently, the solution is to make sure that you have a healthy copy of
  *Distribute* and to, then, uninstall *setuptools*. However, this doesn't seem
  to [always] work. You might prefer to use the "easy_install" method, below.

- If you see an error about antlr-python-runtime, try the following to install
  gdrivefs::

    $ sudo pip install --allow-unverified antlr-python-runtime --allow-external antlr-python-runtime gdrivefs


-------
Options
-------

Any of the configuration values in the `conf.Conf` module can be overwritten as
"-o" options. You may pass the full array of *FUSE* options this way, as well.


-----------------
Format Management
-----------------

*Google Drive* will store *Google Document* files without a standard format. If
you wish to download them, you have to select which format you'd like to
download it as. One of the more exciting features of this *FUSE* implementation
is the flexibility in choosing which format to download on the fly. See the
section below labeled "Displaceables".

If a mime-type isn't provided when requesting a file that requires a mime-type
in order to download, *GDFS* will make a guess based on whether the extension
in the filename (if one exists) can be mapped to a mime-type that is available
among the export-types provided by *GD* for that specific file.


The following is an example directory-listing::

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

Notice the following features:

- Manages duplicates by appending index numbers (e.g. "<filename> (2)").
- Mtimes, permissions, and ownership are correct.
- Sizes are zero for file-types that Google hosts free of charge. These are
  always the files that don't have a strict, default format (the length is
  unknown).
- Hidden files are prefixed with ".", thus hiding them from normal listings.
- "Trashed" files are excluded from listings.
- Any file that will require a mime-type in order to be downloaded has a "#" as
  the last character of its filename.


-------------
Displaceables
-------------

*Google Documents* stores all of its data on *Google Drive*. Google will store
these files in an agnostic file entry whose format will not be determined until
you download it in a specific format. Because the file is not stored in a
particular format, it doesn't have a size. Because it doesn't have a size, the
OS will not issue reads for more than (0) bytes.

To get around this, a read of these types of files will only return exactly
1000 bytes of JSON-encoded "stub data".. Information about the entry, including
the file-path that we've stored it to.

This example also shows how we've specified a mime-type in order to get a PDF
version of a *Google Document* file::

    $ cp Copy\ of\ Dear\ Biola.docx#application+pdf /target
    $ cat /tmp/Copy\ of\ Dear\ Biola.docx#application+pdf

Something like the following will be displayed::

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

From this, you can tell that the file was originally a *Google Documents*
mimetype, and now its a PDF mime-type. You can also see various flags, as well
as the location that the actual, requested file was stored to.


-----------------------
Cache/Change Management
-----------------------

A cache of both the file/folder entries is maintained, as well as a knowledge
of file/folder relationships. However, updates are performed every few seconds
using *GD's* "change" functionality.


-----------
Permissions
-----------

The default UID/GID of files is that of the current user. The default
permissions (modes) are the following:

=================  ====
Entry Type         Perm
=================  ====
Folder             777
Editable file      666
Non-editable file  444
=================  ====

Whether or not a file is "editable" is [obviously] an attribute reported by
*Google Drive*.

These settings can be overridden via the "-o" comma-separated set of
command-line options. See below.


Permission-Related Options
==========================

Related Standard FUSE
---------------------

These options change the behavior at the *FUSE* level (above *GDFS*). See "*man
mount.fuse*" for all options.

===================  ==============================================
Option               Description
-------------------  ----------------------------------------------
umask=M              Prescribe the umask value for -all- entries.
uid=N                Change the default UID.
gid=N                Change the default GID.
allow_other          Allow other users access.
default_permissions  Enforce the permission modes (off, by default)
===================  ==============================================


GDFS-Specific
-------------

=================================  ============================================
Option                             Description
---------------------------------  --------------------------------------------
default_perm_folder=nnn            Default mode for folders.
default_perm_file_noneditable=nnn  Default mode for non-editable files.
default_perm_file_editable=nnn     Default mode for editable files (see above).
=================================  ============================================


Example::

    allow_other,default_permissions,default_perm_folder=770,default_perm_file_noneditable=440,default_perm_file_editable=660


-------------------
Extended Attributes
-------------------

Extended attributes allow access to arbitrary, filesystem-specific data. You
may access any of the properties that *Google Drive* provides for a given entry,
plus a handful of extra ones.

Listing attributes::

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

Getting specific attribute::

    $ getfattr --only-values -n user.original.id American-Pika-with-Food.jpg

    0B5Ft2OXeDBqSSGFIanJ2Z2c3RWs

    $ getfattr --only-values -n user.original.modifiedDate American-Pika-with-Food.jpg

    2013-02-15T15:06:09.691Z

    $ getfattr --only-values -n user.original.labels American-Pika-with-Food.jpg

    K(restricted)=V(False); K(starred)=V(False); K(viewed)=V(False); K(hidden)=V(False); K(trashed)=V(False)

This used to be rendered as JSON, but since the *xattr* utilities add their
own quotes/etc.., it was more difficult to make sense of the values.


----------
Misc Notes
----------

- A file will be marked as hidden on *Google Drive* if it has a prefixing dot. However, Linux/Unix doesn't care about the "hidden" attribute. If you create a file on *Google Drive*, somewhere else, and want it to truly be hidden via this software, make sure you add the prefixing dot.

- If you have a need to do a developer install, use "pip install -e" rather than "python setup.py develop". The latter will [now] break because of the dependencies that are eggs.


.. |Build_Status| image:: https://travis-ci.org/dsoprea/PySvn.svg?branch=master
   :target: https://travis-ci.org/dsoprea/PySvn
