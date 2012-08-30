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
x Seamlessly work around duplicate-file allowances in Google Drive (DONE).
x Seamlessly manage file-type versatility in Google Drive (files do not retain 
  a particular mime-type under GD) (DONE).
x Allow for multiple references to the same files.
x Allow copy-from using default formats as well as allowing one to be chosen on-the-fly.

Also, a design choice of other implementations is to make the user get API keys 
for Google Drive. This is a moronic choice. Our implementation is built against 
OAuth 2.0 as a native application. You should just have to visit the 
authorization URL once, plug-in the auth-code, and be done with it.

Format Management
=================

Google Drive will typically strip uploaded files of their standard formats. If 
you wish to re-download it, you have to select which format you'd like to 
download it as. One of the more exciting features of this FUSE implementation 
is the flexibility in both assigning a default format, while still allowing you
to elect a different format on the fly.

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
drwxrwxrwx  2 root root    0 Nov 12  2008 .
drwxrwxrwt 14 root root 4096 Aug 30 03:17 ..
-rw-rw-rw-  1 root root    0 Feb 19  2011 .north partial list.xls
-rw-rw-rw-  1 root root    0 Aug 23 07:28 Copy of Little League Newsletter.txt
-rw-rw-rw-  1 root root    0 Mar  6  2010 Current Company Agenda.txt
drwxrwxrwx  2 root root    0 Aug 26 08:36 HelloFax
drwxrwxrwx  2 root root    0 Apr 24 18:40 HelloFax (2)
-rw-rw-rw-  1 root root    0 Nov 28  2011 Imported from Google Notebook - My Notebook.txt
drwxrwxrwx  2 root root    0 Nov 21  2008 New Folder
-rw-rw-rw-  1 root root    0 May 13  2010 Provisioning Letter.txt
-rw-rw-rw-  1 root root    0 Apr 21  2010 RHT Testimonial 2005- 2003.txt
-rw-rw-rw-  1 root root    0 Oct 20  2010 searches - standard.xls
-rw-rw-rw-  1 root root 3234 Dec 23  2011 testOnDemandRTSPServer.cpp.gz
drwxrwxrwx  2 root root    0 Aug 26 08:36 Untitled document
-rw-rw-rw-  1 root root    0 Aug 20 08:24 Untitled document.txt
-rw-rw-rw-  1 root root    0 Aug 20 08:25 Untitled document.txt (2)


One terrific feature of this implementation is the simplicity of reading from 
a non-default format:

    cp "searches - standard.xls#pdf" "/tmp/output.pdf"

Dustin Oprea
myselfasunder, gmail.com

