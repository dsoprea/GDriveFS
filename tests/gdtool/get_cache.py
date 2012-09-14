from unittest import TestCase, main

from gdrivefs.gdtool import get_cache, drive_proxy

class GetCacheTestCase(TestCase):
    """Test the _FileCache class via the get_cache() call."""

    file_cache  = None
    drive_proxy = None

    def setUp(self):
        self.file_cache = get_cache()

    def tearDown(self):

        # Clear the singletons.
        get_cache.file_cache    = None
        drive_proxy.gp          = None

        # Clear our reference.
        self.file_cache = None

#    def test_config(self):
#        files = drive_proxy('list_files')
#
#        for file_tuple in files:
#            (entry, filename)

if __name__ == '__main__':
    main()


