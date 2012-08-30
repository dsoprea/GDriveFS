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

        from gdrivefs.gdtool import drive_proxy
        entries = drive_proxy('list_files')

        from pprint import pprint
        import json
        with open('/tmp/entries', 'w') as f:
            for entry in entries:
                f.write("%s\n" % (json.dumps(entry.info)))

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

if __name__ == '__main__':
    main()


