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
> Complete stat() implementation.
x Seamlessly work around duplicate-file allowances in Google Drive (DONE).
x Seamlessly manage file-type versatility in Google Drive (files do not retain 
  a particular mime-type under GD) (DONE).

Also, a design choice of other implementations is to make the user get API keys 
for Google Drive. This is a moronic choice. Our implementation is built against 
OAuth 2.0 as a native application. You should just have to visit the 
authorization URL once, plug-in the auth-code, and be done with it.

Notes
=====

Google Drive will typically strip uploaded files of their standard formats. If 
you wish to re-download it, you have to selected which format you'd like to 
download it as. One of the more exciting features of this FUSE implementation 
is that it will dynamically assign extensions based on a series of rules. The 
user may also choose to change these rules via configuration. Some of the 
default mappings are as follows. They can be overidden via JSON configuration 
files:

    # Default mime-types for GD mime-types.
    gd_to_normal_mime_mappings = {
            'application/vnd.google-apps.document':     'text/plain',
            'application/vnd.google-apps.spreadsheet':  'application/vnd.ms-excel',
            'application/vnd.google-apps.presentation': 'application/vnd.ms-powerpoint',
            'application/vnd.google-apps.drawing':      'application/pdf',
            'application/vnd.google-apps.audio':        'audio/mpeg',
            'application/vnd.google-apps.photo':        'image/png',
            'application/vnd.google-apps.video':        'video/x-flv'
        }

    # Default extensions for mime-types.
    default_extensions = { 
            'text/plain':                       'txt',
            'application/vnd.ms-excel':         'xls',
            'application/vnd.ms-powerpoint':    'ppt',
            'application/pdf':                  'pdf',
            'audio/mpeg':                       'mp3',
            'image/png':                        'png',
            'video/x-flv':                      'flv'
        }

Notice that, by default, all "document" types will be translated to 
"text/plain" files since this is the norm in a console-based Linux system. 

The following is an example directory-listing, as a result of the above (the 
permissions, ownership, and size are still only partially implemented). This 
implementation also manages duplicates by appending index numbers (e.g. 
"<filename> (2)"), as you can see:

    -r--r--r-- 1 root root 0 Mar  6  2010 Current Company Agenda.txt
    -r--r--r-- 1 root root 0 Nov 28  2011 Imported from Google Notebook - My Notebook.txt
    drwxr-xr-x 2 root root 0 Dec 31  1969 New Folder
    -r--r--r-- 1 root root 0 May 13  2010 Provisioning Letter.txt
    -r--r--r-- 1 root root 0 Oct 22  2011 Punch_List10-21-11.docx.txt
    -r--r--r-- 1 root root 0 Apr 21  2010 RHT Testimonial 2005- 2003.txt
    -r--r--r-- 1 root root 0 Oct 20  2010 searches - standard.xls
    -r--r--r-- 1 root root 0 Aug 20 08:24 Untitled document.txt
    -r--r--r-- 1 root root 0 Aug 20 08:25 Untitled document.txt (2)


Dustin Oprea
myselfasunder, gmail.com

