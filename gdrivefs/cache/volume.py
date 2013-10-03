import logging

from collections    import deque
from threading      import RLock
from datetime       import datetime

from gdrivefs.utility import get_utility
from gdrivefs.conf import Conf
from gdrivefs.gdtool.drive import drive_proxy
from gdrivefs.gdtool.account_info import AccountInfo
from gdrivefs.gdtool.normal_entry import NormalEntry
from gdrivefs.cache.cache_registry import CacheRegistry, CacheFault
from gdrivefs.cache.cacheclient_base import CacheClientBase
from gdrivefs.errors import GdNotFoundError

CLAUSE_ENTRY            = 0 # Normalized entry.
CLAUSE_PARENT           = 1 # List of parent clauses.
CLAUSE_CHILDREN         = 2 # List of 2-tuples describing children: (filename, clause)
CLAUSE_ID               = 3 # Entry ID.
CLAUSE_CHILDREN_LOADED  = 4 # All children loaded?

def path_resolver(path):
    path_relations = PathRelations.get_instance()

    try:
        parent_clause = path_relations.get_clause_from_path(path)
    except:
        logging.exception("Could not get clause from path [%s]." % (path))
        raise GdNotFoundError()

    if not parent_clause:
        logging.debug("Path [%s] does not exist for split." % (path))
        raise GdNotFoundError()

    return (parent_clause[CLAUSE_ENTRY], parent_clause)


class PathRelations(object):
    """Manages physical path representations of all of the entries in our "
    account.
    """

    rlock = RLock()
    __log = None

    entry_ll = { }
    path_cache = { }
    path_cache_byid = { }

    @staticmethod
    def get_instance():

        with PathRelations.rlock:
            try:
                return CacheRegistry.__instance;
            except:
                pass

            CacheRegistry.__instance = PathRelations()
            return CacheRegistry.__instance

    def __init__(self):
        self.__log = logging.getLogger().getChild('PathRelate')

    def remove_entry_recursive(self, entry_id, is_update=False):
        """Remove an entry, all children, and any newly orphaned parents."""

        self.__log.info("Doing recursive removal of entry with ID [%s]." % (entry_id))

        to_remove = deque([ entry_id ])
        stat_placeholders = 0
        stat_folders = 0
        stat_files = 0
        removed = { }
        while 1:
            if not to_remove:
                break

            current_entry_id = to_remove.popleft()

            self.__log.debug("RR: Entry with ID (%s) will be removed. (%d) "
                          "remaining." % (current_entry_id, len(to_remove)))

            entry_clause = self.entry_ll[current_entry_id]

            # Any entry that still has children will be transformed into a 
            # placeholder, and not actually removed. Once the children are 
            # removed in this recursive process, we'll naturally clean-up the 
            # parent as a last step. Therefore, the number of placeholders will 
            # overlap with the number of folders (a placeholder must represent 
            # a folder. It is only there because the entry had children).

            if not entry_clause[0]:
                stat_placeholders += 1
            elif entry_clause[0].is_directory:
                stat_folders += 1
            else:
                stat_files += 1

            try:
                result = self.__remove_entry(current_entry_id, is_update)
            except:
                self.__log.debug("Could not remove entry with ID [%s] "
                              "(recursive)." % (current_entry_id))
                raise

            removed[current_entry_id] = True

            (current_orphan_ids, current_children_clauses) = result

            self.__log.debug("RR: Entry removed. (%d) orphans and (%d) children "
                          "were reported." % (len(current_orphan_ids), 
                                                len(current_children_clauses)))

            children_ids_to_remove = [ children[3] for children 
                                                in current_children_clauses ]

            to_remove.extend(current_orphan_ids)
            to_remove.extend(children_ids_to_remove)

        self.__log.debug("RR: Removal complete. (%d) PH, (%d) folders, (%d) files removed." % (stat_placeholders, stat_folders, stat_files))

        return (removed.keys(), (stat_folders + stat_files))

    def __remove_entry(self, entry_id, is_update=False):
        """Remove an entry. Updates references from linked entries, but does 
        not remove any other entries. We return a tuple, where the first item 
        is a list of any parents that, themselves, no longer have parents or 
        children, and the second item is a list of children to this entry.
        """

        with PathRelations.rlock:
            # Ensure that the entry-ID is valid.

            try:
                entry_clause = self.entry_ll[entry_id]
            except:
                self.__log.exception("Could not remove invalid entry with ID "
                                  "[%s]." % (entry_id))
                raise
            
            # Clip from path cache.

            if entry_id in self.path_cache_byid:
                self.__log.debug("Entry found in path-cache. Removing.")

                path = self.path_cache_byid[entry_id]
                del self.path_cache[path]
                del self.path_cache_byid[entry_id]

            else:
                self.__log.debug("Entry with ID [%s] did not need to be removed "
                              "from the path cache." % (entry_id))

            # Clip us from the list of children on each of our parents.

            entry_parents = entry_clause[CLAUSE_PARENT]
            entry_children_tuples = entry_clause[CLAUSE_CHILDREN]

            parents_to_remove = [ ]
            children_to_remove = [ ]
            if entry_parents:
                self.__log.debug("Entry to be removed has (%d) parents." % (len(entry_parents)))

                for parent_clause in entry_parents:
                    # A placeholder has an entry and parents field (fields 
                    # 0, 1) of None.

                    (parent, parent_parents, parent_children, parent_id, \
                        all_children_loaded) = parent_clause

                    if all_children_loaded and not is_update:
                        all_children_loaded = False

                    self.__log.debug("Adjusting parent with ID [%s]." % 
                                  (parent_id))

                    # Integrity-check that the parent we're referencing is 
                    # still in the list.
                    if parent_id not in self.entry_ll:
                        self.__log.warn("Parent with ID [%s] on entry with ID "
                                     "[%s] is not valid." % (parent_id, \
                                                                entry_id))
                        continue
            
                    old_children_filenames = [ child_tuple[0] for child_tuple 
                                                in parent_children ]

                    self.__log.debug("Old children: %s" % 
                                  (', '.join(old_children_filenames)))

                    updated_children = [ child_tuple for child_tuple 
                                         in parent_children 
                                         if child_tuple[1] != entry_clause ]

                    if parent_children != updated_children:
                        parent_children[:] = updated_children

                    else:
                        self.__log.error("Entry with ID [%s] referenced parent "
                                      "with ID [%s], but not vice-versa." % 
                                      (entry_id, parent_id))

                    updated_children_filenames = [ child_tuple[0] 
                                                    for child_tuple
                                                    in parent_children ]

                    self.__log.debug("Up. children: %s" % 
                                  (', '.join(updated_children_filenames)))

                    # If the parent now has no children and is a placeholder, 
                    # advise that we remove it.
                    if not parent_children and parent == None:
                        parents_to_remove.append(parent_id)

            else:
                self.__log.debug("Entry to be removed either has no parents, or is"
                              " a placeholder.")

            # Remove/neutralize entry, now that references have been removed.

            set_placeholder = len(entry_children_tuples) > 0

            if set_placeholder:
                # Just nullify the entry information, but leave the clause. We 
                # had children that still need a parent.

                self.__log.debug("This entry has (%d) children. We will leave a "
                              "placeholder behind." % 
                              (len(entry_children_tuples)))

                entry_clause[0] = None
                entry_clause[1] = None
            else:
                self.__log.debug("This entry does not have any children. It will "
                              "be completely removed.")

                try:
                    del self.entry_ll[entry_id]
                except:
                    self.__log.exception("Could not remove entry with ID [%s]. "
                                      "We've previously confirmed it to exist."
                                      " There might've been a cyclic reference"
                                      " that caused it to be removed during "
                                      " clean-up." % (entry_id))
                    raise

        if parents_to_remove:
            self.__log.debug("Parents that still need to be removed: %s" % 
                          (', '.join(parents_to_remove)))

        children_entry_clauses = [ child_tuple[1] for child_tuple 
                                    in entry_children_tuples ]

        self.__log.debug("Remove complete. (%d) entries were orphaned. There were"
                      " (%d) children." % 
                      (len(parents_to_remove), len(children_entry_clauses)))
        
        return (parents_to_remove, children_entry_clauses)

    def remove_entry_all(self, entry_id, is_update=False):
        """Remove the the entry from both caches. EntryCache is more of an 
        entity look-up, whereas this (PathRelations) has a bunch of expanded 
        data regarding relationships and paths. This call will first remove the 
        relationships from here, and then the entry from the EntryCache.

        We do it in this order because if we were to remove entry from the core
        library (EntryCache) first, then all of the relationships here will 
        suddenly become invalid, and although the entry will be disregistered,
        because it has references from this linked-list, those objects will be
        very much alive. On the other hand, if we remove the entry from 
        PathRelations first, then, because of the locks, PathRelations will not
        be able to touch the relationships until after we're done, here. Ergo, 
        the only thing that can happen is that something may look at the entry
        in the library.
        """

        self.__log.info("Doing complete removal of entry with ID [%s]." % 
                     (entry_id))

        with PathRelations.rlock:
            self.__log.debug("Clipping entry with ID [%s] from PathRelations and "
                             "EntryCache." % (entry_id))

            cache = EntryCache.get_instance().cache

            removed_ids = [ entry_id ]
            if self.is_cached(entry_id):
                self.__log.debug("Removing found PathRelations entries.")

                try:
                    removed_tuple = self.remove_entry_recursive(entry_id, \
                                                               is_update)
                except:
                    self.__log.exception("Could not remove entry-ID from "
                                      "PathRelations. Still continuing, though.")

                (removed_ids, number_removed) = removed_tuple

            self.__log.debug("(%d) entries will now be removed from the core-cache." % (len(removed_ids)))
            for removed_id in removed_ids:
                if cache.exists(removed_id):
                    self.__log.debug("Removing core EntryCache entry with ID [%s]." % (removed_id))

                    try:
                        cache.remove(removed_id)
                    except:
                        self.__log.exception("Could not remove entry-ID from the core"
                                          " cache. Still continuing, though.")

            self.__log.debug("All traces of entry with ID [%s] are gone." % 
                          (entry_id))

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

        with PathRelations.rlock:
            found = { }
            parents = entry_clause[1]
            if not parents:
                return entry_clause[0].title_fs

            else:
                for parent_clause in parents:
                    matching_children = [filename for filename, child_clause 
                                                  in parent_clause[2] 
                                                  if child_clause == entry_clause]
                    if not matching_children:
                        self.__log.error("No matching entry-ID [%s] was not "
                                         "found among children of entry's "
                                         "parent with ID [%s] for proper-"
                                         "filename lookup." % 
                                         (entry_clause[3], parent_clause[3]))

                    else:
                        found[parent_clause[3]] = matching_children[0]

        return found

    def register_entry(self, normalized_entry):

        self.__log.debug("We're registering entry with ID [%s] [%s]." % 
                         (normalized_entry.id, normalized_entry.title))

        with PathRelations.rlock:
            if not normalized_entry.is_visible:
                self.__log.info("We will not register entry with ID [%s] "
                                "because it's not visible." % 
                                (normalized_entry.id))
                return None

            if normalized_entry.__class__ is not NormalEntry:
                raise Exception("PathRelations expects to register an object "
                                "of type NormalEntry, not [%s]." % 
                                (type(normalized_entry)))

            entry_id = normalized_entry.id

            self.__log.info("Registering entry with ID [%s] within path-"
                            "relations." % (entry_id))

            if self.is_cached(entry_id, include_placeholders=False):
                self.__log.debug("Entry to register with ID [%s] already "
                                 "exists within path-relations, and will be "
                                 "removed in lieu of update." % (entry_id))

                self.__log.debug("Removing existing entries.")

                try:
                    self.remove_entry_recursive(entry_id, True)
                except:
                    self.__log.exception("Could not remove existing entry "
                                         "with ID [%s] prior to its update." % 
                                         (entry_id))
                    raise

            self.__log.info("Doing add of entry with ID [%s]." % (entry_id))

            cache = EntryCache.get_instance().cache

            try:
                cache.set(normalized_entry.id, normalized_entry)
            except:
                self.__log.exception("Could not set entry with ID [%s] in "
                                  "cache." % (entry_id))
                raise

            # We do a linked list using object references.
            # (
            #   normalized_entry, 
            #   [ parent clause, ... ], 
            #   [ child clause, ... ], 
            #   entry-ID,
            #   < boolean indicating that we know about all children >
            # )

            if self.is_cached(entry_id, include_placeholders=True):
                self.__log.debug("Placeholder exists for entry-to-register "
                                 "with ID [%s]." % (entry_id))

                entry_clause = self.entry_ll[entry_id]
                entry_clause[CLAUSE_ENTRY] = normalized_entry
                entry_clause[CLAUSE_PARENT] = [ ]
            else:
                self.__log.debug("Entry does not yet exist in LL.")

                entry_clause = [normalized_entry, [ ], [ ], entry_id, False]
                self.entry_ll[entry_id] = entry_clause

            entry_parents = entry_clause[CLAUSE_PARENT]
            title_fs = normalized_entry.title_fs

            self.__log.debug("Registering entry with title [%s]." % (title_fs))

            parent_ids = normalized_entry.parents if normalized_entry.parents \
                                                  is not None else []

            self.__log.debug("Parents are: %s" % (', '.join(parent_ids)))

            for parent_id in parent_ids:
                self.__log.debug("Processing parent with ID [%s] of entry "
                                 "with ID [%s]." % (parent_id, entry_id))

                # If the parent hasn't yet been loaded, install a placeholder.
                if self.is_cached(parent_id, include_placeholders=True):
                    self.__log.debug("Parent has an existing entry.")

                    parent_clause = self.entry_ll[parent_id]
                else:
                    self.__log.debug("Parent is not yet registered.")

                    parent_clause = [None, None, [ ], parent_id, False]
                    self.entry_ll[parent_id] = parent_clause

                if parent_clause not in entry_parents:
                    entry_parents.append(parent_clause)

                parent_children = parent_clause[CLAUSE_CHILDREN]
                filename_base = title_fs

                # Register among the children of this parent, but make sure we 
                # have a unique filename among siblings.

                i = 0
                current_variation = filename_base
                elected_variation = None
                while i <= 255:
                    if not [ child_name_tuple 
                             for child_name_tuple 
                             in parent_children 
                             if child_name_tuple[0] == current_variation ]:
                        elected_variation = current_variation
                        break
                        
                    i += 1
                    current_variation = ("%s (%s)" % (filename_base, i))

                if elected_variation == None:
                    self.__log.error("Could not register entry with ID [%s]. "
                                     "There are too many duplicate names in "
                                     "that directory." % (entry_id))
                    return

                self.__log.debug("Final filename is [%s]." % (current_variation))

                # Register us in the list of children on this parents 
                # child-tuple list.
                parent_children.append((elected_variation, entry_clause))

        self.__log.debug("Entry registration complete.")

        return entry_clause

    def __load_all_children(self, parent_id):
        self.__log.info("Loading children under parent with ID [%s]." % 
                     (parent_id))

        with PathRelations.rlock:
            try:
                children = drive_proxy('list_files', parent_id=parent_id)
            except:
                self.__log.exception("Could not retrieve children for parent with"
                                  " ID [%s]." % (parent_id))
                raise

            child_ids = [ ]
            if children:
                self.__log.debug("(%d) children returned and will be "
                              "registered." % (len(children)))

                for child in children:
                    try:
                        self.register_entry(child)
                    except:
                        self.__log.exception("Could not register retrieved-entry for "
                                          "child with ID [%s] in path-cache." % 
                                          (child.id))
                        raise

                self.__log.debug("Looking up parent with ID [%s] for all-"
                              "children update." % (parent_id))

                try:
                    parent_clause = self.__get_entry_clause_by_id(parent_id)
                except:
                    self.__log.exception("Could not retrieve clause for parent-entry "
                                      "[%s] in load-all-children function." % 
                                      (parent_id))
                    raise

                parent_clause[4] = True

                self.__log.debug("All children have been loaded.")

        return children

    def get_children_from_entry_id(self, entry_id):
        """Return the filenames contained in the folder with the given 
        entry-ID.
        """

        self.__log.info("Getting children under entry with ID [%s]." % (entry_id))

        with PathRelations.rlock:
            try:
                entry_clause = self.__get_entry_clause_by_id(entry_id)
            except:
                self.__log.exception("Could not retrieve entry with ID from the path-"
                                  "cache [%s]." % (entry_id))
                raise

            if not entry_clause:
                message = ("Can not list the children for an unavailable entry "
                           "with ID [%s]." % (entry_id))

                self.__log.error(message)
                raise Exception(message)

            if not entry_clause[4]:
                self.__log.debug("Not all children have been loaded for parent with "
                              "ID [%s]. Loading them now." % (entry_id))

                try:
                    self.__load_all_children(entry_id)
                except:
                    self.__log.exception("Could not load all children for parent with"
                                      " ID [%s]." % (entry_id))
                    raise

            else:
                self.__log.debug("All children for [%s] have already been loaded." % (entry_id))

            if not entry_clause[0].is_directory:
                message = ("Could not get child filenames for non-directory with "
                           "entry-ID [%s]." % (entry_id))

                self.__log.error(message)
                raise Exception(message)

            self.__log.info("(%d) children found." % 
                            (len(entry_clause[CLAUSE_CHILDREN])))

            return entry_clause[CLAUSE_CHILDREN]

    def get_children_entries_from_entry_id(self, entry_id):

        children_tuples = self.get_children_from_entry_id(entry_id)

        children_entries = [(child_tuple[0], child_tuple[1][CLAUSE_ENTRY]) 
                                for child_tuple 
                                in children_tuples]

        return children_entries

    def get_clause_from_path(self, filepath):

        self.__log.info("Getting clause for path [%s]." % (filepath))

        with PathRelations.rlock:
            try:
                path_results = self.find_path_components_goandget(filepath)
            except:
                self.__log.exception("Could not resolve path [%s] to entry." % (filepath))
                raise

            (entry_ids, path_parts, success) = path_results

            if not success:
                return None

            entry_id = path_results[0][-1]
        
            self.__log.info("Found entry with ID [%s]." % (entry_id))

            # Make sure the entry is more than a placeholder.

            try:
                self.__get_entry_clause_by_id(entry_id)
            except:
                self.__log.exception("Clause was found for path, but entry "
                                     "could not be retrieved.")
                raise

            return self.entry_ll[entry_id]

    def find_path_components_goandget(self, path):
        """Do the same thing that find_path_components() does, except that 
        when we don't have record of a path-component, try to go and find it 
        among the children of the previous path component, and then try again.
        """

        with PathRelations.rlock:
            previous_results = []
            i = 0
            while 1:
                self.__log.info("Attempting to find path-components (go and "
                                "get) for path [%s].  CYCLE= (%d)" % (path, i))

                # See how many components can be found in our current cache.

                try:
                    result = self.__find_path_components(path)
                except:
                    self.__log.exception("There was a problem doing an "
                                         "iteration of find_path_components() "
                                         "on [%s]." % (path))
                    raise

                self.__log.debug("Path resolution cycle (%d) results: %s" % 
                                 (i, result))

                # If we could resolve the entire path, return success.

                self.__log.debug("Found within current cache? %s" % 
                                 (result[2]))

                if result[2] == True:
                    return result

                # If we could not resolve the entire path, and we're no more 
                # successful than a prior attempt, we'll just have to return a 
                # partial.

                num_results = len(result[0])
                if num_results in previous_results:
                    self.__log.debug("We couldn't improve our results. This "
                                     "path most likely does not exist.")
                    return result

                previous_results.append(num_results)

                self.__log.debug("(%d) path-components were found, but not "
                                 "all." % (num_results))

                # Else, we've encountered a component/depth of the path that we 
                # don't currently know about.
# TODO: This is going to be the general area that we'd have to adjust to 
#        support multiple, identical entries. This currently only considers the 
#        first result. We should rewrite this to be recursive in order to make 
#        it easier to keep track of a list of results.
                # The parent is the last one found, or the root if none.
                parent_id = result[0][num_results - 1] \
                                if num_results \
                                else AccountInfo.get_instance().root_id

                # The child will be the first part that was not found.
                child_name = result[1][num_results]

                self.__log.debug("Trying to reconcile child named [%s] under "
                                 "folder with entry-ID [%s]." % (child_name, 
                                                                 parent_id))

                try:
                    children = drive_proxy('list_files', parent_id=parent_id, 
                                           query_is_string=child_name)
                except:
                    self.__log.exception("Could not retrieve children for "
                                         "parent with ID [%s]." % (parent_id))
                    raise
                
                for child in children:
                    try:
                        self.register_entry(child)
                    except:
                        self.__log.exception("Could not register child entry "
                                             "for entry with ID [%s] in path-"
                                             "cache." % (child.id))
                        raise

                filenames_phrase = ', '.join([ candidate.id for candidate
                                                            in children ])
                self.__log.debug("(%d) candidate children were found: %s" % 
                                 (len(children), filenames_phrase))

                i += 1

    def __find_path_components(self, path):
        """Given a path, return a list of all Google Drive entries that 
        comprise each component, or as many as can be found. As we've ensured 
        that all sibling filenames are unique, there can not be multiple 
        matches.
        """

        self.__log.debug("Searching for path components of [%s]. Now "
                         "resolving entry_clause." % (path))

        if path[0] == '/':
            path = path[1:]

        if len(path) and path[-1] == '/':
            path = path[:-1]

        if path in self.path_cache:
            return self.path_cache[path]

        with PathRelations.rlock:
            self.__log.debug("Locating entry information for path [%s]." % (path))

            try:
                root_id = AccountInfo.get_instance().root_id
            except:
                self.__log.exception("Could not get root-ID.")
                raise

            # Ensure that the root node is loaded.

            try:
                self.__get_entry_clause_by_id(root_id)
            except:
                self.__log.exception("Could not ensure root-node with entry-ID "
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

                self.__log.debug("Checking for part (%d) [%s] under parent with "
                              "ID [%s]." % (i, child_filename_to_search_fs, 
                                            entry_ptr))

                try:
                    current_clause = self.entry_ll[entry_ptr]
                except:
                    # TODO: If entry with ID entry_ptr is not registered, update 
                    #       children of parent parent_id. Throttle how often this 
                    #       happens.

                    self.__log.exception("Could not find current subdirectory.  "
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
#                    self.__log.debug("Looking for child [%s] among (%d): %s" % 
#                                  (child_filename_to_search_fs, len(children),
#                                   [ child_tuple[0] for child_tuple 
#                                     in children ]))

                    found = [ child_tuple[1][3] 
                              for child_tuple 
                              in children 
                              if child_tuple[0] == child_filename_to_search_fs ]

                if found:
                    self.__log.debug("Found matching child with ID [%s]." % (found[0]))
                    results.append(found[0])
                else:
                    self.__log.debug("Did not find matching child.")
                    return (results, path_parts, False)

                # Have we traveled far enough into the linked list?
                if (i + 1) >= num_parts:
                    self.__log.debug("Path has been completely resolved: %s" % (', '.join(results)))

                    self.path_cache[path] = (results, path_parts, True)
                    final_entry_id = results[-1]
                    self.path_cache_byid[final_entry_id] = path

                    return self.path_cache[path]

                parent_id = entry_ptr
                entry_ptr = found[0]
                i += 1

    def __get_entry_clause_by_id(self, entry_id):
        """We may keep a linked-list of GD entries, but what we have may just 
        be placeholders. This function will make sure the data is actually here.
        """

        with PathRelations.rlock:
            if self.is_cached(entry_id):
                return self.entry_ll[entry_id]

            else:
                cache = EntryCache.get_instance().cache

                try:
                    normalized_entry = cache.get(entry_id)
                except:
                    self.__log.exception("Could not fetch normalized entry with ID "
                                      "[%s]." % (entry_id))
                    raise

                try:
                    return self.register_entry(normalized_entry)
                except:
                    self.__log.exception("Could not register retrieved-entry for "
                                      "entry with ID [%s] in path-cache." % 
                                      (entry_id))
                    raise

    def is_cached(self, entry_id, include_placeholders=False):

        return (entry_id in self.entry_ll and (include_placeholders or \
                                               self.entry_ll[entry_id][0]))

class EntryCache(CacheClientBase):
    """Manages our knowledge of file entries."""

    __log = None
    about = AccountInfo.get_instance()

    def __init__(self):
        self.__log = logging.getLogger().getChild('EntryCache')
        CacheClientBase.__init__(self)

    def __get_entries_to_update(self, requested_entry_id):
        # Get more entries than just what was requested, while we're at it.

        try:
            parent_ids = drive_proxy('get_parents_containing_id', 
                                     child_id=requested_entry_id)
        except:
            self.__log.exception("Could not retrieve parents for child with ID "
                              "[%s]." % (requested_entry_id))
            raise

        self.__log.debug("Found (%d) parents." % (len(parent_ids)))

        affected_entries = [ requested_entry_id ]
        considered_entries = { }
        max_readahead_entries = Conf.get('max_readahead_entries')
        for parent_id in parent_ids:
            self.__log.debug("Retrieving children for parent with ID [%s]." % 
                          (parent_id))

            try:
                child_ids = drive_proxy('get_children_under_parent_id', 
                                        parent_id=parent_id)
            except:
                self.__log.exception("Could not retrieve children for parent with"
                                  " ID [%s]." % (requested_entry_id))
                raise

            self.__log.debug("(%d) children found under parent with ID [%s]." % 
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

                affected_entries.append(child_id)

                if len(affected_entries) >= max_readahead_entries:
                    break

        return affected_entries

    def __do_update_for_missing_entry(self, requested_entry_id):

        # Get the entries to update.

        try:
            affected_entries = self.__get_entries_to_update(requested_entry_id)
        except:
            self.__log.exception("Could not aggregate requested and readahead "
                              "entries to refresh.")
            raise

        # Read the entries, now.

        self.__log.info("(%d) primary and secondary entry/entries will be "
                     "updated." % (len(affected_entries)))

        # TODO: We have to determine when this is called, and either remove it 
        # (if it's not), or find another way to not have to load them 
        # individually.

        try:
            retrieved = drive_proxy('get_entries', entry_ids=affected_entries)
        except:
            self.__log.exception("Could not retrieve the (%d) entries." % (len(affected_entries)))
            raise

        # Update the cache.

        path_relations = PathRelations.get_instance()

        for entry_id, entry in retrieved.iteritems():
            try:
                path_relations.register_entry(entry)
            except:
                self.__log.exception("Could not register entry with ID [%s] with path-relations cache." % (entry_id))
                raise

        self.__log.debug("(%d) entries were loaded." % (len(retrieved)))

        return retrieved

    def fault_handler(self, resource_name, requested_entry_id):
        """A requested entry wasn't stored."""

        self.__log.info("EntryCache has faulted on entry with ID [%s]." % 
                      (requested_entry_id))

        try:
            retrieved = self.__do_update_for_missing_entry(requested_entry_id)
        except:
            self.__log.exception("Could not reconcile unknown entry with ID "
                              "[%s]." % (requested_entry_id))
            raise

        # Return the requested entry.

        try:
            return retrieved[requested_entry_id]
        except:
            self.__log.exception("We just updated as a result of a fault, but "
                              "entry with ID [%s] is still not available from "
                              "the cache." % (requested_entry_id))
            return None

    def cleanup_pretrigger(self, resource_name, entry_id, force):
        """The core entry cache has a clean-up process that will remove old "
        entries. This is called just before any record is removed.
        """

        # Now that the local cache-item has been removed, remove the same from
        # the PathRelations cache.

        path_relations = PathRelations.get_instance()

        if path_relations.is_cached(entry_id):
            self.__log.debug("Removing PathRelations entry for cleaned-up entry "
                          "with ID [%s]." % (entry_id))

            try:
                path_relations.remove_entry_recursive(entry_id)
            except:
                self.__log.exception("Could not remove PathRelations entry with "
                                  "ID [%s] on cleanup." % (entry_id))
                raise

    def get_max_cache_age_seconds(self):
        return Conf.get('cache_entries_max_age')

