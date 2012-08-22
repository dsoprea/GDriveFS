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
> Seamlessly work-around duplicate-file allowances in Google Drive.

Also, a design choice of other implementations is to make the user get API keys 
for Google Drive. This is a moronic choice. Our implementation is built against 
OAuth 2.0 as a native application. You should just have to visit the 
authorization URL once, plug-in the auth-code, and be done with it.


Dustin Oprea
