from unittest import TestCase, main

from gdrivefs.gdtool import drive_proxy

class GetDriveTestCase(TestCase):
    """Test the _GdriveManager class via _GoogleProxy via get_drive()."""

    drive_proxy = None

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_list_files(self):
        """Test the list_files() call on the Drive object."""

        files = drive_proxy('list_files')

        print(files)

if __name__ == '__main__':
    main()


