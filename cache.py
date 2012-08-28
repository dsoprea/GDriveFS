import logging

from collections    import OrderedDict
from threading      import Lock
from datetime       import datetime
from collections    import deque

from utility import get_utility
from gdtool import drive_proxy, NormalEntry, AccountInfo
from conf import Conf

class CacheFault(Exception):
    pass

class _CacheRegistry(object):
    """The main cache container."""

    cache = { }

    def __init__(self):
        pass

    @staticmethod
    def get_instance(resource_name):
    
        logging.debug("CacheRegistry(%s)" % (resource_name))

        try:
            _CacheRegistry.__instance;
        except:
            try:
                _CacheRegistry.__instance = _CacheRegistry()
            except:
                logging.exception("Could not manufacture singleton "
                                 "CacheRegistry instance.")
                raise

        if resource_name not in _CacheRegistry.__instance.cache:
            _CacheRegistry.__instance.cache[resource_name] = { }

        return _CacheRegistry.__instance

    def set(self, resource_name, key, value):

        logging.debug("CacheRegistry.set(%s,%s,%s)" % (resource_name, key, type(value)))

        try:
            old_tuple = self.cache[resource_name][key]
        except:
            old_tuple = None

        self.cache[resource_name][key] = (value, datetime.now())

        return old_tuple

    def remove(self, resource_name, key, cleanup_trigger=None):

        logging.debug("CacheRegistry.remove(%s,%s,%s)" % (resource_name, key, 
                      type(cleanup_trigger)))

        try:
            old_tuple = self.cache[resource_name][key]
        except:
            raise

        self.__cleanup_entry(resource_name, key, True, 
                             cleanup_trigger=cleanup_trigger)

        return old_tuple[0]

    def get(self, resource_name, key, max_age, cleanup_trigger=None):
        
        trigger_given_phrase = ('None' 
                                if cleanup_trigger == None 
                                else '<given>')

        logging.debug("CacheRegistry.get(%s,%s,%s,%s)" % (resource_name, key, 
                      max_age, trigger_given_phrase))

        try:
            (value, timestamp) = self.cache[resource_name][key]
        except:
            raise CacheFault("NonExist")

        if max_age != None and (datetime.now() - timestamp).seconds > max_age:
            self.__cleanup_entry(resource_name, key, False, 
                                 cleanup_trigger=cleanup_trigger)
            raise CacheFault("Stale")

        return value

    def exists(self, resource_name, key, max_age, cleanup_trigger=None):

        logging.debug("CacheRegistry.exists(%s,%s,%s,%s)" % (resource_name, key, 
                      max_age, cleanup_trigger))
        
        try:
            (value, timestamp) = self.cache[resource_name][key]
        except:
            return False

        if max_age != None and (datetime.now() - timestamp).seconds > max_age:
            self.__cleanup_entry(resource_name, key, False, 
                                 cleanup_trigger=cleanup_trigger)
            return False

        return True

    def __cleanup_entry(self, resource_name, key, force, cleanup_trigger=None):

        logging.debug("Doing clean-up for resource_name [%s] and key [%s]." % 
                      (resource_name, key))

        try:
            del self.cache[resource_name][key]
        except:
            logging.exception("Could not clean-up entry with resource_name "
                              "[%s] and key [%s]." % (resource_name, key))
            raise

        if cleanup_trigger != None:
            logging.debug("Running clean-up trigger for resource_name [%s] and"
                          " key [%s]." % (resource_name, key))

            try:
                cleanup_trigger(resource_name, key, force)
            except:
                logging.exception("Cleanup-trigger failed.")
                raise

class _CacheAgent(object):
    """A particular namespace within the cache."""

    registry        = None
    resource_name   = None
    max_age         = None

    fault_handler   = None
    cleanup_trigger = None

    def __init__(self, resource_name, max_age, fault_handler=None, 
                 cleanup_trigger=None):
        logging.debug("CacheAgent(%s,%s,%s,%s)" % (resource_name, max_age, 
                                                   type(fault_handler), 
                                                   cleanup_trigger))

        self.registry = _CacheRegistry.get_instance(resource_name)
        self.resource_name = resource_name
        self.max_age = max_age

        self.fault_handler = fault_handler
        self.cleanup_trigger = cleanup_trigger

    def set(self, key, value):
        logging.debug("CacheAgent.set(%s,%s)" % (key, type(value)))

        return self.registry.set(self.resource_name, key, value)

    def remove(self, key):
        logging.debug("CacheAgent.remove(%s)" % (key))

        return self.registry.remove(self.resource_name, key, 
                                    cleanup_trigger=self.cleanup_trigger)

    def get(self, key, handle_fault = None):

        if handle_fault == None:
            handle_fault = True

        logging.debug("CacheAgent.get(%s)" % (key))

        try:
            result = self.registry.get(self.resource_name, key, 
                                       max_age=self.max_age, 
                                       cleanup_trigger=self.cleanup_trigger)
        except (CacheFault):
            logging.debug("There was a cache-miss while requesting item with "
                          "ID (key).")

            if self.fault_handler == None or not handle_fault:
                raise

            try:
                result = self.fault_handler(self.resource_name, key)
            except:
                logging.exception("There was an exception in the fault-"
                                  "handler, handling for key [%s].", key)
                raise

            if result == None:
                raise

        return result

    def exists(self, key):
        logging.debug("CacheAgent.exists(%s)" % (key))

        return self.registry.exists(self.resource_name, key, 
                                    max_age=self.max_age,
                                    cleanup_trigger=self.cleanup_trigger)

    def __getitem__(self, key):
        return self.get(key)

    def __setitem__(self, key, value):
        return self.set(key, value)

    def __delitem__(self, key):
        return self.remove(key)

class CacheClient(object):
    """Meant to be inherited by a class. Is used to configure a particular 
    namespace within the cache.
    """

    @property
    def cache(self):
        try:
            return self._cache
        except:
            pass

        self._cache = _CacheAgent(self.child_type, self.max_age, 
                                 fault_handler=self.fault_handler, 
                                 cleanup_trigger=self.cleanup_trigger)

        return self._cache

    def __init__(self):
        child_type = self.__class__.__bases__[0].__name__
        max_age = self.get_max_cache_age_seconds()
        
        logging.debug("CacheClient(%s,%s)" % (child_type, max_age))

        self.child_type = child_type
        self.max_age = max_age

        self.init()

    def fault_handler(self, resource_name, key):
        pass

    def cleanup_trigger(self, resource_name, key, force):
        pass

    def init(self):
        pass

    def get_max_cache_age_seconds(self):
        raise NotImplementedError("get_max_cache_age() must be implemented in "
                                  "the CacheClient child.")

    @classmethod
    def get_instance(cls):
        """A helper method to dispense a singleton of whomever is inheriting "
        from us.
        """

        class_name = cls.__name__

        try:
            CacheClient.__instances
        except:
            CacheClient.__instances = { }

        try:
            return CacheClient.__instances[class_name]
        except:
            CacheClient.__instances[class_name] = cls()
            return CacheClient.__instances[class_name]

class PathRelations(object):
    """Manages physical path representations of all of the entries in our "
    account.
    """

    entry_ll = { }
    path_cache = { }

    @staticmethod
    def get_instance():

        try:
            return _CacheRegistry.__instance;
        except:
            pass

        _CacheRegistry.__instance = PathRelations()
        return _CacheRegistry.__instance

    def remove_entry(self, entry_id):

        # Clip from path cache.
        
        key = None
        for path, this_entry_id in self.path_cache.items():
            if this_entry_id == entry_id:
                key = path
                break
        
        if key != None:
            del self.path_cache[key]

        # Ensure that the entry-ID is valid.

        try:
            entry_clause = self.entry_ll[entry_id]
        except:
            logging.exception("Could not remove invalid entry with ID [%s]." % 
                              (entry_id))
            raise

        # Clip us from the list of children on each of our parents.

        entry_parents = entry_clause[1]
        entry_children = entry_clause[2]

        parents_to_remove = [ ]
        if entry_parents:
            for parent_clause in entry_parents:
                # A placeholder has an entry and parents field (fields 0, 1) of 
                # None.

                (parent, parent_parents, parent_children, parent_id, all_children_loaded) \
                    = parent_clause

                # Integrity-check that the parent we're referencing is still 
                # in the list.
                if parent_id not in self.entry_ll:
                    logging.warn("Parent with ID [%s] on entry with ID [%s] is"
                                 " not valid." % (parent_id, entry_id))
                    continue
            
                updated_children = [ child_tuple 
                                     for child_tuple 
                                     in parent_children 
                                     if child_tuple[1] != entry_clause ]
# TODO: Confirm that this works. We tested it.. It should.
                if parent_children != updated_children:
                    parent_children[:] = updated_children

                else:
                    logging.error("Entry with ID [%s] referenced parent with ID "
                                  "[%s], but not vice-versa." % (entry_id, 
                                                                 parent_id))

                # If the parent now has no children and is a placeholder, advise 
                # that we remove it.
                if not parent_children and parent == None:
                    parents_to_remove.append(parent)

        # Remove/neutralize entry, now that references have been removed.

        set_placeholder = len(entry_children) > 0

        if set_placeholder:
            # Just nullify the entry information, but leave the clause. We had 
            # children that still need a parent.

            entry_clause[0] = None
            entry_clause[1] = None

        else:
            try:
                del self.entry_ll[entry_id]
            except:
                logging.exception("Could not remove entry with ID [%s]. We've "
                                  "previously confirmed it to exist. There "
                                  "might've been a cyclic reference that "
                                  "caused it to be removed during clean-up." % 
                                  (entry_id))
                raise

        return parents_to_remove

    def dump_entry_clause(self, entry_id):
        """Shows info on a single entry_clause. Does not assume that all of "
        the information is correct/consistent.
        """

        try:
            entry_clause = self.entry_ll[entry_id]
        except:
            logging.exception("Entry with ID [%s] is not valid." % (entry_id))
            raise

        (entry, parents, children, entry_id_recorded, all_children_loaded) = entry_clause

        print("Entry ID [%s]\n" % (entry_id))

        title_phrase = (entry.title if entry else '<none>')

        print("Entry title: %s" % (title_phrase))

        if parents != None:
            print("Parents: (%d)" % (len(parents)))

            for parent in parents:
                title = parent[0].title if parent[0] else '<none>'
                print("  %s: %s" % (title, parent[3]))
        else:
            print("Parents: <none>")

        if children != None:
            print("Children: (%d)" % (len(children)))

            for (filename, child_clause) in children:
                print("  %s: %s" % (filename, child_clause[0].id))
        else:
            print("Children: <none>")

        print("Recorded Entry Id: %s" % (entry_id_recorded))

    def get_proper_filenames(self, entry_clause):
        """Return what was determined to be the unique filename for this "
        particular entry for each of its respective parents. This will return 
        the standard 'title' value as a scalar when the root entry, and a 
        dictionary of parent-IDs to unique-filenames when not.

        This call is necessary because GD allows duplicate filenames until any 
        one folder. Note that a consequence of both this and the fact that GD 
        allows the same file to be listed under multiple folders means that a 
        file may look like "filename" under one and "filename (2)" under 
        another.
        """

        found = { }
        parents = entry_clause[1]
        if not parents:
            return entry_clause[0].title_fs

        else:
            for parent_clause in parents:
                matching_children = [ filename for filename, child_clause in parent_clause[2] if child_clause == entry_clause ]
                if not matching_children:
                    logging.error("No matching entry-ID [%s] was not found "
                                  "among children of entry's parent with ID "
                                  "[%s] for proper-filename lookup." % 
                                  (entry_clause[3], parent_clause[3]))

                else:
                    found[parent_clause[3]] = matching_children[0]

        return found

    def dump_ll(self):

        i = 0
        for entry_id, entry_clause in self.entry_ll.iteritems():
            entry_phrase = ('<entry>' if entry_clause[0] else '<none>')

            if entry_clause[1] != None:
                parents_phrase = ('(%d)' % len(entry_clause[1]))
            else:
                parents_phrase = '<none>'

            if entry_clause[2] != None:
                children_phrase = ('(%d)' % len(entry_clause[2]))
            else:
                children_phrase = '<none>'

            entry_id_phrase = ('[%s]' % entry_clause[3])

            print("(%d) %s" % (i, entry_id))
            print("  (E= %s, P= %s, C= %s, I= %s)" % (entry_phrase, parents_phrase, children_phrase, entry_id_phrase))
            print("  %s\n" % (entry_clause[0].title if entry_clause[0] else '<none>'))

            i += 1

    def register_entry(self, normalized_entry):

        logging.debug("We're registering entry with ID [%s]." % (normalized_entry.id))

        if [ flag 
             for flag, value 
             in normalized_entry.labels.items() 
             if flag in [u'restricted', u'trashed'] and value ]:
            return None

        entry_id = normalized_entry.id

        if normalized_entry.__class__ is not NormalEntry:
            raise Exception("PathRelations expects to register an object of "
                            "type NormalEntry, not [%s]." % 
                            (type(normalized_entry)))

        logging.info("Registering entry with ID [%s] within path-relations." %
                     (entry_id))

        if entry_id in self.entry_ll:
            logging.debug("Entry to register with ID [%s] already exists "
                          "within path-relations, and will be removed in lieu "
                          "of update." % (entry_id))

            # Remove this entry, and any placeholder-parents that are left 
            # empty as a result of the removal of it.

            entries_to_remove = deque([ entry_id ])
            while 1:
                if not len(entries_to_remove):
                    break

                entry_id_to_remove = entries_to_remove.popleft()

                logging.info("Removing entry with ID [%s] as a result of the "
                             "preliminary clean-up prior to the add of ID "
                             "[%s]." % (entry_id_to_remove, entry_id))

                try:
                    additional_to_remove = self.remove_entry(entry_id_to_remove)
                    entries_to_remove.extend(additional_to_remove)
                except:
                    logging.exception("Could not remove existing entry with ID "
                                      "[%s] prior to its update." % (entry_id_to_remove))
                    raise

        # We do a linked list using object references.
        # (
        #   normalized_entry, 
        #   [ parent clause, ... ], 
        #   [ child clause, ... ], 
        #   entry-ID,
        #   < boolean indicating that we know about all children >
        # )

        logging.info("Doing add of entry with ID [%s]." % (entry_id))

        if entry_id not in self.entry_ll:
            logging.debug("Entry does not yet exist in LL.")

            entry_clause = [normalized_entry, [ ], [ ], entry_id, False]
            self.entry_ll[entry_id] = entry_clause
        else:
            logging.debug("Placeholder exists for entry-to-register with ID [%s]." % (entry_id))

            entry_clause = self.entry_ll[entry_id]
            entry_clause[0] = normalized_entry
            entry_clause[1] = [ ]

        entry_parents = entry_clause[1]
        title_fs = normalized_entry.title_fs

        logging.debug("Registering entry with title [%s]." % (title_fs))
        parent_ids = [ parent_id for parent_id in normalized_entry.parents ]
        logging.debug("Parents are: %s" % (', '.join(parent_ids)))

        for parent_id in normalized_entry.parents:
            logging.debug("Processing parent with ID [%s] of entry with ID [%s]." % (parent_id, entry_id))

            # If the parent hasn't yet been loaded, install a placeholder.
            if parent_id not in self.entry_ll:
                logging.debug("Parent is not yet registered.")

                parent_clause = [None, None, [ ], parent_id, False]
                self.entry_ll[parent_id] = parent_clause
            else:
                logging.debug("Parent has an existing entry.")

                parent_clause = self.entry_ll[parent_id]

            if parent_clause not in entry_parents:
                entry_parents.append(parent_clause)

            parent_children = parent_clause[2]

            # Register among the children of this parent, but make sure we have 
            # a unique filename among siblings.

            i = 1
            current_variation = title_fs
            elected_variation = None
            while i <= 255:
                if not [ child_name_tuple 
                         for child_name_tuple 
                         in parent_children 
                         if child_name_tuple[0] == current_variation ]:
                    elected_variation = current_variation
                    break
                    
                i += 1
                current_variation = ("%s (%s)" % (title_fs, i))

            if elected_variation == None:
                logging.error("Could not register entry with ID [%s]. There "
                              "are too many duplicate names in that "
                              "directory." % (entry_id))
                return

            # Prepend a period if it's a hidden file.
            if u'hidden' in normalized_entry.labels \
                    and normalized_entry.labels[u'hidden']:
                elected_variation = get_utility(). \
                    translate_filename_charset(".%s" % (elected_variation))

            logging.debug("Final filename is [%s]." % (current_variation))

            # Register us in the list of children on this parent.
            parent_children.append((elected_variation, entry_clause))

        logging.debug("Entry registration complete.")

        return entry_clause

    def get_children_with_contains_and_decay(self, parent_id, query_contains_string):

        logging.info("Getting child-listing under parent_id [%s] with query "
                     "[%s] WITH DECAY." % (parent_id, query_contains_string))

        # We're going to potentially repeat the call to GD several times, with 
        # varying lengths of a query (if a query_contains_string was provided 
        # and query_contains_decay is True). In addition to the query itself, 
        # we'll try a search with just the first <prefix> characters, another 
        # one with just the first character, and then a comprehensive search. 
        # The moment a filename matching query_contains_string is found, we 
        # bail. This is necessary in a situation where there are duplicate 
        # filenames in the same directory, but we don't know about all of the 
        # entries in that directory yet, nor have we determined their unique 
        # filenames, yet (filenames that Google won't know about).

        prefix_length = Conf.get('query_decay_intermed_prefix_length')
        query_contains_string_fs = get_utility(). \
            translate_filename_charset(query_contains_string)

        # We'll have to end-up searching -every- child in the directory. In the 
        # beginning, we knew that, if the file existed at all, at least the 
        # first character would be searchable. However, now the hidden files 
        # are locally modified to have a prefixing dot, and we no longer has 
        # this assumption.
        search_tokens = [query_contains_string_fs, 
                         query_contains_string_fs[:prefix_length], 
                         query_contains_string_fs[0],
                         None]

        i = 0
        found = False
        results = [ ]
        cache = EntryCache.get_instance().cache
        for search_token in search_tokens:
            logging.info("Listing entries under parent with ID [%s] and "
                         "contains-query [%s], in cycle (%d)." % 
                         (parent_id, search_token, i))

            # Get the list of children.

            try:
                matched_children = drive_proxy('get_children_under_parent_id',
                                       parent_id=parent_id, 
                                       query_contains_string=search_token)
            except:
                logging.exception("Could not list children containing [%s] under "
                                  "folder with entry-ID [%s]." % 
                                  (search_token, parent_id))
                raise

            # Induce a retrieval of each child by asking for it.

            for child_id in matched_children:
                try:
                    results.append(cache.get(child_id))
                except:
                    logging.exception("Could not retrieve entry for matched child "
                                      "with entry-ID [%s]." % (child_id))
                    raise

            # If there's a child under the given parent where the filename 
            # matches query_contains, return.

            try:
                parent_clause = self.__get_entry_clause_by_id(parent_id)
            except:
                logging.exception("Could not retrieve clause for parent-entry "
                                  "[%s] in contains-with-decay function." % 
                                  (parent_id))
                raise

            if parent_clause:
                found = [ child_tuple[1] 
                          for child_tuple 
                          in parent_clause[2] 
                          if child_tuple[0] == query_contains_string_fs ]

                if found:
                    break

            i += 1

        return (results, found)

    def __load_all_children(self, parent_id):
        logging.info("Loading children under parent with ID [%s]." % 
                     (parent_id))

        try:
            child_ids = drive_proxy('get_children_under_parent_id', 
                                    parent_id=parent_id)
        except:
            logging.exception("Could not retrieve children for parent with"
                              " ID [%s]." % (parent_id))
            raise

        logging.debug("(%d) children found." % (len(child_ids)))

        for child_id in child_ids:
            try:
                self.__get_entry_clause_by_id(child_id)
            except:
                logging.exception("Could not get entry-clause for ID [%s]." %
                                  (child_id))
                raise

        return child_ids

    def get_child_filenames_from_entry_id(self, entry_id):
        """Return the filenames contained in the folder with the given 
        entry-ID.
        """

        logging.info("Getting children under entry with ID [%s]." % (entry_id))

        try:
            entry_clause = self.__get_entry_clause_by_id(entry_id)
        except:
            logging.exception("Could not retrieve entry with ID from the path-"
                              "cache [%s]." % (entry_id))
            raise

        if not entry_clause:
            message = ("Can not list the children for an unavailable entry "
                       "with ID [%s]." % (entry_id))

            logging.error(message)
            raise Exception(message)

        if not entry_clause[4]:
            logging.debug("Not all children have been loaded for parent with "
                          "ID [%s]. Loading them now." % (entry_id))

            try:
                self.__load_all_children(entry_id)
            except:
                logging.exception("Could not load all children for parent with"
                                  " ID [%s]." % (entry_id))
                raise

        if not get_utility().is_directory(entry_clause[0]):
            message = ("Could not get child filenames for non-directory with "
                       "entry-ID [%s]." % (entry_id))

            logging.error(message)
            raise Exception(message)

        children_filenames = [ child_tuple[0] for child_tuple in entry_clause[2] ]

        logging.info("(%d) children found." % (len(children_filenames)))

        return children_filenames

    def get_clause_from_path(self, path):

        logging.info("Getting clause for path [%s]." % (path))

        try:
            path_results = self.find_path_components_goandget(path)
        except:
            logging.exception("Could not resolve path [%s] to entry." % (path))
            raise

        (entry_ids, path_parts, success) = path_results

        if not success:
            message = ("Could not resolve path [%s]." % (path))
            logging.error(message)
            raise Exception(message)

        entry_id = path_results[0][-1]
        
        logging.info("Found entry with ID [%s]." % (entry_id))

        return self.entry_ll[entry_id]

    def find_path_components_goandget(self, path):
        """Do the same thing that find_path_components() does, except that 
        when we don't have record of a path-component, try to go and find it 
        among the children of the previous path component, and then try again.
        """

        previous_results = []
        i = 0
        while 1:
            logging.info("Attempting to find path-components (go and get) for "
                         "path [%s].  CYCLE= (%d)" % (path, i))

            # See how many components can be found in our current cache.

            try:
                result = self.__find_path_components(path)
            except:
                logging.exception("There was a problem doing an iteration of "
                                  "find_path_components() on [%s]." % (path))
                raise

            # If we could resolve the entire path, return success.

            logging.debug("Found within current cache? %s" % (result[2]))

            if result[2] == True:
                return result

            # If we could not resolve the entire path, and we're no more 
            # successful than a prior attempt, we'll just have to return a 
            # partial.

            num_results = len(result[0])
            if num_results in previous_results:
                logging.debug("We couldn't improve our results. This path most"
                              " likely does not exist.")
                return result

            previous_results.append(num_results)

            logging.debug("(%d) path-components were found, but not all." % (num_results))

            # Else, we've encountered a component/depth of the path that we 
            # don't currently know about.

            # The parent is the last one found, or the root if none.
            parent_id = result[0][num_results - 1] \
                            if num_results \
                            else AccountInfo().root_id

            # The child will be the first part that was not found.
            child_name = result[1][num_results]

            logging.debug("Trying to reconcile child named [%s] under folder "
                          "with entry-ID [%s]." % (child_name, parent_id))

            try:
                results = self.get_children_with_contains_and_decay(parent_id, child_name)
            except:
                logging.exception("Could not retrieve children like [%s] under"
                                  " parent with entry-ID [%s]." % (child_name, 
                                                                   parent_id))
                raise

            filenames_phrase = ', '.join([ candidate.id for candidate in results[0] ])
            logging.debug("(%d) candidate children were found: %s" % (len(results[0]), filenames_phrase))

            i += 1

    def __find_path_components(self, path):
        """Given a path, return a list of all Google Drive entries that 
        comprise each component, or as many as can be found. As we've ensured 
        that all sibling filenames are unique, there can not be multiple 
        matches.
        """

        logging.debug("Searching for path components of [%s]." % (path))
        logging.debug("Resolving entry_clause for path [%s]." % (path))

        if path[0] == '/':
            path = path[1:]

        if len(path) and path[-1] == '/':
            path = path[:-1]

#        if path in self.path_cache:
#            return self.path_cache[path]

        logging.debug("Locating entry information for path [%s]." % (path))

        try:
            root_id = AccountInfo().root_id
        except:
            logging.exception("Could not get root-ID.")
            raise

        # Ensure that the root node is loaded.

        try:
            self.__get_entry_clause_by_id(root_id)
        except:
            logging.exception("Could not ensure root-node with entry-ID "
                              "[%s]." % (root_id))
            raise

        path_parts = path.split('/')

        entry_ptr = root_id
        parent_id = None
        i = 0
        num_parts = len(path_parts)
        results = [ ]
        while i < num_parts:
            child_filename_to_search_fs = get_utility(). \
                translate_filename_charset(path_parts[i])

            logging.debug('Checking part (%d): [%s]' % 
                          (i, child_filename_to_search_fs))

            try:
                current_clause = self.entry_ll[entry_ptr]
            except:
                # TODO: If entry with ID entry_ptr is not registered, update 
                #       children of parent parent_id. Throttle how often this 
                #       happens.

                logging.exception("Could not find current subdirectory.  "
                                  "ENTRY_ID= [%s]" % (entry_ptr))
                raise

            
            # Search this entry's children for the next filename further down 
            # in the path among this entry's children. Any duplicates should've 
            # already beeen handled as entries were stored. We name the variable 
            # just to emphasize that no ambiguity -as well as- no error will 
            # occur in the traversal process.
            first_matching_child_clause = None
            children = current_clause[2]
            
            # If they just wanted the "" path (root), return the root-ID.
            if path == "":
                found = [ root_id ]
            else:
                found = [ child_tuple[1][3] 
                          for child_tuple 
                          in children 
                          if child_tuple[0] == child_filename_to_search_fs ]
            
            if found:
                results.append(found[0])
            else:
                return (results, path_parts, False)

            # Have we traveled far enough into the linked list?
            if (i + 1) >= num_parts:
                self.path_cache[path] = current_clause
                return (results, path_parts, True)

            parent_id = entry_ptr
            entry_ptr = found[0]
            i += 1

    def __get_entry_clause_by_id(self, entry_id):
        """We may keep a linked-list of GD entries, but what we have may just 
        be placeholders. This function will make sure the data is actually here.
        """

        if entry_id in self.entry_ll and self.entry_ll[entry_id][0]:
            return self.entry_ll[entry_id]

        else:
            cache = EntryCache.get_instance().cache

            try:
                normalized_entry = cache.get(entry_id)
            except:
                logging.exception("Could not fetch normalized entry with ID "
                                  "[%s]." % (entry_id))
                raise

            try:
                return self.register_entry(normalized_entry)
            except:
                logging.exception("Could not register retrieved-entry for "
                                  "entry with ID [%s] in path-cache." % 
                                  (entry_id))
                raise
            
class EntryCache(CacheClient):
    """Manages our knowledge of file entries."""

    about = AccountInfo.get_instance()

    def __is_update_needed(self, entry_id):
        return True

    def __get_entries_to_update(self, requested_entry_id):
        # Get more entries than just what was requested, while we're at it.

        try:
            parent_ids = drive_proxy('get_parents_containing_id', 
                                     child_id=requested_entry_id)
        except:
            logging.exception("Could not retrieve parents for child with ID "
                              "[%s]." % (requested_entry_id))
            raise

        logging.debug("Found (%d) parents." % (len(parent_ids)))

        affected_entries = [ ]
        considered_entries = { }
        max_readahead_entries = Conf.get('max_readahead_entries')
        for parent_id in parent_ids:
            logging.debug("Retrieving children for parent with ID [%s]." % 
                          (parent_id))

            try:
                child_ids = drive_proxy('get_children_under_parent_id', 
                                        parent_id=parent_id)
            except:
                logging.exception("Could not retrieve children for parent with"
                                  " ID [%s]." % (requested_entry_id))
                raise

            logging.debug("(%d) children found under parent with ID [%s]." % 
                          (len(child_ids), parent_id))

            for child_id in child_ids:
                if child_id == requested_entry_id:
                    continue

                # We've already looked into this entry.

                try:
                    considered_entries[child_id]
                    continue
                except:
                    pass

                considered_entries[child_id] = True

                # Is it already cached?

                if self.cache.exists(child_id):
                    continue

                if self.__is_update_needed(child_id):
                    affected_entries.append(child_id)

                if len(affected_entries) >= (max_readahead_entries - 1):
                    break

        affected_entries[0:0] = [ requested_entry_id ]

        return affected_entries

    def __set_cache(self, entry_id, entry):

        try:
            self.cache.set(entry_id, entry)
        except:
            logging.exception("Could not store entry with ID [%s]." % 
                              (entry))
            raise

    def fault_handler(self, resource_name, requested_entry_id):
        """A requested entry wasn't stored."""

        logging.info("EntryCache has faulted on entry with ID [%s]." % 
                      (requested_entry_id))

        # Get the entries to update.

        try:
            affected_entries = self.__get_entries_to_update(requested_entry_id)
        except:
            logging.exception("Could not aggregate requested and readahead "
                              "entries to refresh.")
            raise

        # Read the entries, now.

        logging.info("(%d) primary and secondary entry/entries will be "
                     "updated." % (len(affected_entries)))

        try:
            retrieved = drive_proxy('get_entries', entry_ids=affected_entries)
        except:
            logging.exception("Could not retrieve the (%d) entries." % (len(affected_entries)))
            raise

        # Update the cache.

        path_relations = PathRelations.get_instance()

        for entry_id, entry in retrieved.iteritems():
            try:
                self.__set_cache(entry_id, entry)
            except:
                logging.exception("Could not set entry with ID [%s] in cache." % (entry_id))
                raise

            try:
                path_relations.register_entry(entry)
            except:
                logging.exception("Could not register entry with ID [%s] with path-relations cache." % (entry_id))
                raise

        logging.debug("(%d) entries were loaded." % (len(retrieved)))

        # Return the requested entry.

        try:
            return retrieved[requested_entry_id]
        except:
            logging.exception("We just updated as a result of a fault, but "
                              "entry with ID [%s] is still not available from "
                              "the cache." % (requested_entry_id))
            return None

    def cleanup_trigger(self, resource_name, entry_id, force):
        pass

    def get_max_cache_age_seconds(self):
        return None

# TODO: Start a cache clean-up thread to make sure that all old items at the 
# beginning of the cleanup_index are constantly pruned.

