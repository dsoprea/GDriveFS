from unittest import TestCase, main

from gdrivefs.gdtool import drive_proxy, AccountInfo
from gdrivefs.cache import EntryCache, PathRelations
from gdrivefs.utility import get_utility

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

        entries = drive_proxy('list_files')

        from pprint import pprint
        import json
        with open('/tmp/entries', 'w') as f:
            for entry in entries:
                f.write("%s\n" % (json.dumps(entry.info)))

    def test_get_changes(self):

        from gdrivefs.change import get_change_manager
        get_change_manager().process_updates()

        import sys
        sys.exit()

        (largest_change_id, next_page_token, changes) = drive_proxy('list_changes')

        print("Largest Change ID: [%s]" % (largest_change_id))
        print("Next Page Token: [%s]" % (next_page_token))

        from pprint import pprint
        pprint(len(changes))
        print

        for change_id, (entry_id, was_deleted, entry) in changes.iteritems():
            print("%d> [%s] D:[%s] [%s]" % (change_id, entry_id, was_deleted, entry.title if entry else '<deleted>'))

#            pprint(change_id)
#            pprint(change_info)

#            import sys
#            sys.exit()
#            pprint("%s, %s, %s" % (type(change[0]), type(change[1]), type(change[2])))

    def test_get_parents_containing_id(self):

        return
        
        entry_id = u'11EIs1ZxCykme0FnAdY8Xm_ktUCQ9y5lHC3EwAKFsiFk'

        try:
            parent_ids = drive_proxy('get_parents_containing_id', 
                                     child_id=entry_id)
        except:
            logging.exception("Could not retrieve parents for child with ID "
                              "[%s]." % (entry_id))
            raise

        from pprint import pprint
        pprint(parent_ids)

    def test_download_file(self):

        return

        from gdrivefs.gdtool import drive_proxy
        http = drive_proxy('get_authed_http')

        normalized_entry = EntryCache.get_instance().cache.get('1DcIWAjj-pnSCXBQa3kHJQuL-QMRoopx8Yx_LVhfRigk')
        mime_type = 'text/plain'

        files = drive_proxy('download_to_local', normalized_entry=normalized_entry, mime_type=mime_type)

        return

        from pprint import pprint
        url = files[16].download_links[u'text/plain']
        pprint(url)

        data = http.request(url)
        response_headers = data[0]

        import re
        r = re.compile('Range')
        found = [("%s: %s" % (k, v)) for k, v in response_headers.iteritems() if r.match(k)]
        if found:
            print("Found: %s" % (", ".join(found)))

        print(">>>===============================================")
#        print(data[1][:200])
        print("<<<===============================================")

    def test_get_about(self):

        return

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

    def test_remove_entry(self):

        return

        from gdrivefs.cache import PathRelations

        path_relations = PathRelations.get_instance()
        entry_clause = path_relations.get_clause_from_path('HelloFax')

        filenames = path_relations.get_child_filenames_from_entry_id(entry_clause[3])
        
        root_id = u'0AJFt2OXeDBqSUk9PVA'
        middle_id = entry_clause[3]
        child_id = u'0B5Ft2OXeDBqSTmpjSHlVbEV5ajg'

#        from pprint import pprint
#        pprint(filenames)

#        path_relations.dump_entry_clause(middle_id)

        print("1: =============================")
        path_relations.dump_ll()
        print("2: =============================")
#        print("middle: %s" % (middle_id))
#        return

        path_relations.remove_entry_recursive(middle_id)
#        path_relations.remove_entry(middle_id)

        print("3: =============================")
        path_relations.dump_ll()

        return

        try:
            path_relations.dump_entry_clause(root_id)
        except:
            print("<No root.>")

        try:
            path_relations.dump_entry_clause(middle_id)
        except:
            print("<No middle.>")

        try:
            path_relations.dump_entry_clause(child_id)
        except:
            print("<No child.>")

    def test_insert_entry(self):

        import datetime
#        filename = ("NewFolder_%s" % (datetime.datetime.now().strftime("%H%M%S")))
#        entry = drive_proxy('create_directory', filename=filename)

        filename = ("NewFile_%s.txt" % (datetime.datetime.now().strftime("%H%M%S")))
        entry = drive_proxy('create_file', filename=filename, data_filepath='/tmp/tmpdata.txt', parents=[])

        print(entry.id)

if __name__ == '__main__':
    main()


