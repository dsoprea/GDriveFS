class AuthorizationError(Exception):
    """All authorization-related errors inherit from this."""
    pass

class AuthorizationFailureError(AuthorizationError):
    """There was a general authorization failure."""
    pass
        
class AuthorizationFaultError(AuthorizationError):
    """Our authorization is not available or has expired."""
    pass

class MustIgnoreFileError(Exception):
    """An error requiring us to ignore the file."""
    pass

class FilenameQuantityError(MustIgnoreFileError):
    """Too many filenames share the same name in a single directory."""
    pass

class ExportFormatError(Exception):
    """A format was not available for export."""
    pass

