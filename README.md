GDriveFS
========

An innovative FUSE wrapper for Google Drive.

Design goals:

> Limited, but configurable, caching.
> Cleanup thread to manage cleanup of aged cache items.
> Continual monitoring of changes to fiel-structure via "changes" functionality 
  of API.
> Complete stat() implementation (or as much as allowed by API, which should be 
  close to complete).

Also, a design choice of other implementations is to make the user get API keys for Google Drive. This is a moronic choice. Our implementation is built against OAuth 2.0 as a native application. You should just have to visit the authorization URL once, plug-in the auth-code, and be done with it.


Dustin Oprea
