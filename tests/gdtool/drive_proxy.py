from unittest import TestCase, main

from gdrivefs.gdtool import drive_proxy, AccountInfo
from gdrivefs.cache import EntryCache, PathRelations

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

        return

        from gdrivefs.gdtool import drive_proxy
        drive_proxy('list_files')

        parent_id = '0AJFt2OXeDBqSUk9PVA'

        entries = drive_proxy('get_children_under_parent_id', parent_id=parent_id)

        print(entries)

    def test_get_about(self):

        entry_id_1 = u'11EIs1ZxCykme0FnAdY8Xm_ktUCQ9y5lHC3EwAKFsiFk'
        entry1 = EntryCache.get_instance().cache.get(entry_id_1)
#        result = PathRelations.get_instance().register_entry(entry1)

        entry_id_2 = u'0AJFt2OXeDBqSUk9PVA'
#        entry2 = EntryCache.get_instance().cache.get(entry_id_2)
#        result = PathRelations.get_instance().register_entry(entry2)

        path_relations = PathRelations.get_instance()

        #print(len(entry.parents))
#        path_relations.dump_ll()

#        print(AccountInfo().root_id)

#        path_relations.dump_entry_clause('0AJFt2OXeDBqSUk9PVA')
#        PathRelations.get_instance().dump_entry_clause('0B5Ft2OXeDBqSSmdIek1aajZtVDA')
#        return
#        entry_clause = path_relations.get_entry_clause_by_id(entry_id_1)
        #result = path_relations.find_path_components_goandget('/')

        result = path_relations.get_child_filenames_from_entry_id(entry_id_2)

        from pprint import pprint
        pprint(result)

#        result = EntryCache.get_instance().cache.get(u'11EIs1ZxCykme0FnAdY8Xm_ktUCQ9y5lHC3EwAKFsiFk')
#        result = EntryCache.get_instance().cache.get(u'11EIs1ZxCykme0FnAdY8Xm_ktUCQ9y5lHC3EwAKFsiFk')
#        result = EntryCache.get_instance().cache.get(u'11EIs1ZxCykme0FnAdY8Xm_ktUCQ9y5lHC3EwAKFsiFk')
#        result = EntryCache.get_instance().cache.get(u'11EIs1ZxCykme0FnAdY8Xm_ktUCQ9y5lHC3EwAKFsiFk')
#        print(result)
        return
        

#        result = AccountInfo().root_id

        #about = drive_proxy('get_about_info')

#        entries = drive_proxy('get_children_under_parent_id', parent_id=about.root_id)
        #entries = drive_proxy('get_parents_over_child_id', child_id=u'11EIs1ZxCykme0FnAdY8Xm_ktUCQ9y5lHC3EwAKFsiFk')


#        print(response[u'rootFolderId'])
        import pprint
#        pprint.pprint(response[u'importFormats'])
        pprint.pprint(result)

if __name__ == '__main__':
    main()


