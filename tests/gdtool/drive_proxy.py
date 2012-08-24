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

#        files = drive_proxy('list_files')

#        print(files)

    def test_list_files_by_parent_id(self):

        from gdrivefs.gdtool import drive_proxy
        drive_proxy('list_files')

        parent_id = '0AJFt2OXeDBqSUk9PVA'

        entries = drive_proxy('get_children_under_parent_id', parent_id=parent_id)

        print(entries)

    def test_get_about(self):

        return

        response = drive_proxy('get_about_info')

#        print(response[u'rootFolderId'])
        import pprint
#        pprint.pprint(response[u'importFormats'])
        pprint.pprint(response[u'exportFormats'])

if __name__ == '__main__':
    main()


