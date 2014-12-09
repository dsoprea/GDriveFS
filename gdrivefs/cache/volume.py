import logging

from collections    import deque
from threading      import RLock
from datetime       import datetime

from gdrivefs.utility import utility
from gdrivefs.conf import Conf
from gdrivefs.gdtool.drive import get_gdrive
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

_logger = logging.getLogger(__name__)

def path_resolver(path):
    path_relations = PathRelations.get_instance()

    parent_clause = path_relations.get_clause_from_path(path)
    if not parent_clause:
#        logging.debug("Path [%s] does not exist for split.", path)
        raise GdNotFoundError()

    return (parent_clause[CLAUSE_ENTRY], parent_clause)


class PathRelations(object):
    """Manages physical path representations of all of the entries in our "
    account.
    """

    rlock = RLock()

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

    def remove_entry_recursive(self, entry_id, is_update=False):
        """Remove an entry, all children, and any newly orphaned parents."""

        _logger.debug("Recursively pruning entry with ID [%s].", entry_id)

        to_remove = deque([ entry_id ])
        stat_placeholders = 0
        stat_folders = 0
        stat_files = 0
        removed = { }
        while 1:
            if not to_remove:
                break

            current_entry_id = to_remove.popleft()
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

            result = self.__remove_entry(current_entry_id, is_update)

            removed[current_entry_id] = True

            (current_orphan_ids, current_children_clauses) = result

            children_ids_to_remove = [ children[3] for children 
                                                in current_children_clauses ]

            to_remove.extend(current_orphan_ids)
            to_remove.extend(children_ids_to_remove)

        return (removed.keys(), (stat_folders + stat_files))

    def __remove_entry(self, entry_id, is_update=False):
        """Remove an entry. Updates references from linked entries, but does 
        not remove any other entries. We return a tuple, where the first item 
        is a list of any parents that, themselves, no longer have parents or 
        children, and the second item is a list of children to this entry.
        """

        with PathRelations.rlock:
            # Ensure that the entry-ID is valid.

            entry_clause = self.entry_ll[entry_id]
            
            # Clip from path cache.

            if entry_id in self.path_cache_byid:
                path = self.path_cache_byid[entry_id]
                del self.path_cache[path]
                del self.path_cache_byid[entry_id]

            # Clip us from the list of children on each of our parents.

            entry_parents = entry_clause[CLAUSE_PARENT]
            entry_children_tuples = entry_clause[CLAUSE_CHILDREN]

            parents_to_remove = [ ]
            children_to_remove = [ ]
            if entry_parents:
                for parent_clause in entry_parents:
                    # A placeholder has an entry and parents field (fields 
                    # 0, 1) of None.

                    (parent, parent_parents, parent_children, parent_id, \
                        all_children_loaded) = parent_clause

                    if all_children_loaded and not is_update:
                        all_children_loaded = False

                    # Integrity-check that the parent we're referencing is 
                    # still in the list.
                    if parent_id not in self.entry_ll:
                        _logger.warn("Parent with ID [%s] on entry with ID "
                                     "[%s] is not valid." % 
                                     (parent_id, entry_id))
                        continue
            
                    old_children_filenames = [ child_tuple[0] for child_tuple 
                                                in parent_children ]

                    updated_children = [ child_tuple for child_tuple 
                                         in parent_children 
                                         if child_tuple[1] != entry_clause ]

                    if parent_children != updated_children:
                        parent_children[:] = updated_children

                    else:
                        _logger.error("Entry with ID [%s] referenced parent "
                                      "with ID [%s], but not vice-versa." % 
                                      (entry_id, parent_id))

                    updated_children_filenames = [ child_tuple[0] 
                                                    for child_tuple
                                                    in parent_children ]

                    # If the parent now has no children and is a placeholder, 
                    # advise that we remove it.
                    if not parent_children and parent == None:
                        parents_to_remove.append(parent_id)

            # Remove/neutralize entry, now that references have been removed.

            set_placeholder = len(entry_children_tuples) > 0

            if set_placeholder:
                # Just nullify the entry information, but leave the clause. We 
                # had children that still need a parent.

                entry_clause[0] = None
                entry_clause[1] = None
            else:
                del self.entry_ll[entry_id]

        children_entry_clauses = [ child_tuple[1] for child_tuple 
                                    in entry_children_tuples ]

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

        with PathRelations.rlock:
            cache = EntryCache.get_instance().cache

            removed_ids = [ entry_id ]
            if self.is_cached(entry_id):
                try:
                    removed_tuple = self.remove_entry_recursive(entry_id, \
                                                               is_update)
                except:
                    _logger.exception("Could not remove entry-ID from "
                                      "PathRelations. Still continuing, "
                                      "though.")

                (removed_ids, number_removed) = removed_tuple

            for removed_id in removed_ids:
                if cache.exists(removed_id):
                    try:
                        cache.remove(removed_id)
                    except:
                        _logger.exception("Could not remove entry-ID from "
                                          "the core cache. Still "
                                          "continuing, though.")

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
                        _logger.error("No matching entry-ID [%s] was not "
                                      "found among children of entry's "
                                      "parent with ID [%s] for proper-"
                                      "filename lookup." % 
                                      (entry_clause[3], parent_clause[3]))

                    else:
                        found[parent_clause[3]] = matching_children[0]

        return found

    def register_entry(self, normalized_entry):

        with PathRelations.rlock:
            if not normalized_entry.is_visible:
                return None

            if normalized_entry.__class__ is not NormalEntry:
                raise Exception("PathRelations expects to register an object "
                                "of type NormalEntry, not [%s]." % 
                                (type(normalized_entry)))

            entry_id = normalized_entry.id

#            self.__log.debug("Registering entry with ID [%s] within path-"
#                             "relations.", entry_id)

            if self.is_cached(entry_id, include_placeholders=False):
                self.remove_entry_recursive(entry_id, True)

            cache = EntryCache.get_instance().cache

            cache.set(normalized_entry.id, normalized_entry)

            # We do a linked list using object references.
            # (
            #   normalized_entry, 
            #   [ parent clause, ... ], 
            #   [ child clause, ... ], 
            #   entry-ID,
            #   < boolean indicating that we know about all children >
            # )

            if self.is_cached(entry_id, include_placeholders=True):
                entry_clause = self.entry_ll[entry_id]
                entry_clause[CLAUSE_ENTRY] = normalized_entry
                entry_clause[CLAUSE_PARENT] = [ ]
            else:
                entry_clause = [normalized_entry, [ ], [ ], entry_id, False]
                self.entry_ll[entry_id] = entry_clause

            entry_parents = entry_clause[CLAUSE_PARENT]
            title_fs = normalized_entry.title_fs

            parent_ids = normalized_entry.parents if normalized_entry.parents \
                                                  is not None else []

            for parent_id in parent_ids:

                # If the parent hasn't yet been loaded, install a placeholder.
                if self.is_cached(parent_id, include_placeholders=True):
                    parent_clause = self.entry_ll[parent_id]
                else:
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
                    current_variation = filename_base + \
                                        utility.translate_filename_charset(
                                            ' (%d)' % (i))

                if elected_variation == None:
                    _logger.error("Could not register entry with ID [%s]. "
                                  "There are too many duplicate names in "
                                  "that directory." % (entry_id))
                    return

                # Register us in the list of children on this parents 
                # child-tuple list.
                parent_children.append((elected_variation, entry_clause))

        return entry_clause

    def __load_all_children(self, parent_id):
        gd = get_gdrive()

        with PathRelations.rlock:
            children = gd.list_files(parent_id=parent_id)

            child_ids = [ ]
            if children:
                for child in children:
                        self.register_entry(child)

                parent_clause = self.__get_entry_clause_by_id(parent_id)

                parent_clause[4] = True

        return children

    def get_children_from_entry_id(self, entry_id):
        """Return the filenames contained in the folder with the given 
        entry-ID.
        """

        with PathRelations.rlock:
            entry_clause = self.__get_entry_clause_by_id(entry_id)
            if not entry_clause:
                message = ("Can not list the children for an unavailable "
                           "entry with ID [%s]." % (entry_id))

                _logger.error(message)
                raise Exception(message)

            if not entry_clause[4]:
                self.__load_all_children(entry_id)

            if not entry_clause[0].is_directory:
                message = ("Could not get child filenames for non-directory with "
                           "entry-ID [%s]." % (entry_id))

                _logger.error(message)
                raise Exception(message)

#            self.__log.debug("(%d) children found.",
#                             len(entry_clause[CLAUSE_CHILDREN]))

            return entry_clause[CLAUSE_CHILDREN]

    def get_children_entries_from_entry_id(self, entry_id):

        children_tuples = self.get_children_from_entry_id(entry_id)

        children_entries = [(child_tuple[0], child_tuple[1][CLAUSE_ENTRY]) 
                                for child_tuple 
                                in children_tuples]

        return children_entries

    def get_clause_from_path(self, filepath):

#        self.__log.debug("Getting clause for path [%s].", filepath)

        with PathRelations.rlock:
            path_results = self.find_path_components_goandget(filepath)

            (entry_ids, path_parts, success) = path_results
            if not success:
                return None

            entry_id = path_results[0][-1]
#            self.__log.debug("Found entry with ID [%s].", entry_id)

            # Make sure the entry is more than a placeholder.
            self.__get_entry_clause_by_id(entry_id)

            return self.entry_ll[entry_id]

    def find_path_components_goandget(self, path):
        """Do the same thing that find_path_components() does, except that 
        when we don't have record of a path-component, try to go and find it 
        among the children of the previous path component, and then try again.
        """

        gd = get_gdrive()

        with PathRelations.rlock:
            previous_results = []
            i = 0
            while 1:
#                self.__log.debug("Attempting to find path-components (go and "
#                                 "get) for path [%s].  CYCLE= (%d)", path, i)

                # See how many components can be found in our current cache.

                result = self.__find_path_components(path)

                # If we could resolve the entire path, return success.

                if result[2] == True:
                    return result

                # If we could not resolve the entire path, and we're no more 
                # successful than a prior attempt, we'll just have to return a 
                # partial.

                num_results = len(result[0])
                if num_results in previous_results:
                    return result

                previous_results.append(num_results)

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

                children = gd.list_files(
                                parent_id=parent_id, 
                                query_is_string=child_name)
                
                for child in children:
                    self.register_entry(child)

                filenames_phrase = ', '.join([ candidate.id for candidate
                                                            in children ])
#                self.__log.debug("(%d) candidate children were found: %s",
#                                 len(children), filenames_phrase)

                i += 1

    def __find_path_components(self, path):
        """Given a path, return a list of all Google Drive entries that 
        comprise each component, or as many as can be found. As we've ensured 
        that all sibling filenames are unique, there can not be multiple 
        matches.
        """

        if path[0] == '/':
            path = path[1:]

        if len(path) and path[-1] == '/':
            path = path[:-1]

        if path in self.path_cache:
            return self.path_cache[path]

        with PathRelations.rlock:
#            self.__log.debug("Locating entry information for path [%s].", path)

            root_id = AccountInfo.get_instance().root_id

            # Ensure that the root node is loaded.
            self.__get_entry_clause_by_id(root_id)

            path_parts = path.split('/')

            entry_ptr = root_id
            parent_id = None
            i = 0
            num_parts = len(path_parts)
            results = [ ]
            while i < num_parts:
                child_filename_to_search_fs = utility. \
                    translate_filename_charset(path_parts[i])

#                self.__log.debug("Checking for part (%d) [%s] under parent "
#                                 "with ID [%s].",
#                                 i, child_filename_to_search_fs, entry_ptr)

                current_clause = self.entry_ll[entry_ptr]
            
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
                normalized_entry = cache.get(entry_id)
                return self.register_entry(normalized_entry)

    def is_cached(self, entry_id, include_placeholders=False):

        return (entry_id in self.entry_ll and (include_placeholders or \
                                               self.entry_ll[entry_id][0]))

class EntryCache(CacheClientBase):
    """Manages our knowledge of file entries."""

    def __init__(self, *args, **kwargs):
        super(EntryCache, self).__init__(*args, **kwargs)

# TODO(dustin): This isn't used, and we don't think that it necessarily needs 
#               to be instantiated, now.
#        about = AccountInfo.get_instance()
        self.__gd = get_gdrive()

    def __get_entries_to_update(self, requested_entry_id):
        # Get more entries than just what was requested, while we're at it.

        parent_ids = self.__gd.get_parents_containing_id(requested_entry_id)

        affected_entries = [requested_entry_id]
        considered_entries = {}
        max_readahead_entries = Conf.get('max_readahead_entries')
        for parent_id in parent_ids:
            child_ids = self.__gd.get_children_under_parent_id(parent_id)

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

        affected_entries = self.__get_entries_to_update(requested_entry_id)

        # Read the entries, now.

# TODO: We have to determine when this is called, and either remove it 
# (if it's not), or find another way to not have to load them 
# individually.

        retrieved = self.__gd.get_entries(affected_entries)

        # Update the cache.

        path_relations = PathRelations.get_instance()

        for entry_id, entry in retrieved.iteritems():
            path_relations.register_entry(entry)

        return retrieved

    def fault_handler(self, resource_name, requested_entry_id):
        """A requested entry wasn't stored."""

        retrieved = self.__do_update_for_missing_entry(requested_entry_id)

        # Return the requested entry.
        return retrieved[requested_entry_id]

    def cleanup_pretrigger(self, resource_name, entry_id, force):
        """The core entry cache has a clean-up process that will remove old "
        entries. This is called just before any record is removed.
        """

        # Now that the local cache-item has been removed, remove the same from
        # the PathRelations cache.

        path_relations = PathRelations.get_instance()

        if path_relations.is_cached(entry_id):
            path_relations.remove_entry_recursive(entry_id)

    def get_max_cache_age_seconds(self):
        return Conf.get('cache_entries_max_age')

