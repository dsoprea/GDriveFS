import logging

from collections    import OrderedDict, deque
from threading      import RLock, Timer
from datetime       import datetime

from gdrivefs.utility import get_utility
from gdrivefs.gdtool import drive_proxy, NormalEntry, AccountInfo
from gdrivefs.conf import Conf
from gdrivefs.report import Report
from gdrivefs.timer import Timers

CLAUSE_ENTRY            = 0
CLAUSE_PARENT           = 1
CLAUSE_CHILDREN         = 2
CLAUSE_ID               = 3
CLAUSE_CHILDREN_LOADED  = 4

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

        with _CacheRegistry.rlock:
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

        with _CacheRegistry.rlock:
            try:
                old_tuple = self.cache[resource_name][key]
            except:
                old_tuple = None

            self.cache[resource_name][key] = (value, datetime.now())

        return old_tuple

    def remove(self, resource_name, key, cleanup_pretrigger=None):

        logging.debug("CacheRegistry.remove(%s,%s,%s)" % (resource_name, key, 
                      type(cleanup_pretrigger)))

        with _CacheRegistry.rlock:
            try:
                old_tuple = self.cache[resource_name][key]
            except:
                raise

            self.__cleanup_entry(resource_name, key, True, 
                                 cleanup_pretrigger=cleanup_pretrigger)

        return old_tuple[0]

    def get(self, resource_name, key, max_age, cleanup_pretrigger=None):
        
        trigger_given_phrase = ('None' 
                                if cleanup_pretrigger == None 
                                else '<given>')

        logging.debug("CacheRegistry.get(%s,%s,%s,%s)" % (resource_name, key, 
                      max_age, trigger_given_phrase))

        with _CacheRegistry.rlock:
            try:
                (value, timestamp) = self.cache[resource_name][key]
            except:
                raise CacheFault("NonExist")

            if max_age != None and (datetime.now() - timestamp).seconds > max_age:
                self.__cleanup_entry(resource_name, key, False, 
                                     cleanup_pretrigger=cleanup_pretrigger)
                raise CacheFault("Stale")

        return value

    def list_raw(self, resource_name):
        
        logging.debug("CacheRegistry.list(%s)" % (resource_name))

        with _CacheRegistry.rlock:
            try:
                return self.cache[resource_name]
            except:
                logging.exception("Could not list raw-entries under cache "
                                  "labelled with resource-name [%s]." %
                                  (resource_name))
                raise

    def exists(self, resource_name, key, max_age, cleanup_pretrigger=None, no_fault_check=False):

        logging.debug("CacheRegistry.exists(%s,%s,%s,%s)" % (resource_name, 
                      key, max_age, cleanup_pretrigger))
        
        with _CacheRegistry.rlock:
            try:
                (value, timestamp) = self.cache[resource_name][key]
            except:
                return False

            if max_age != None and not no_fault_check and \
                    (datetime.now() - timestamp).seconds > max_age:
                self.__cleanup_entry(resource_name, key, False, 
                                     cleanup_pretrigger=cleanup_pretrigger)
                return False

        return True

    def count(self, resource_name):

        return len(self.cache[resource_name])

    def __cleanup_entry(self, resource_name, key, force, 
                        cleanup_pretrigger=None):

        logging.debug("Doing clean-up for resource_name [%s] and key [%s]." % 
                      (resource_name, key))

        if cleanup_pretrigger != None:
            logging.debug("Running pre-cleanup trigger for resource_name [%s] "
                          "and key [%s]." % (resource_name, key))

            try:
                cleanup_pretrigger(resource_name, key, force)
            except:
                logging.exception("Cleanup-trigger failed.")
                raise

        try:
            del self.cache[resource_name][key]
        except:
            logging.exception("Could not clean-up entry with resource_name "
                              "[%s] and key [%s]." % (resource_name, key))
            raise

_CacheRegistry.rlock = RLock()

class _CacheAgent(object):
    """A particular namespace within the cache."""

    registry        = None
    resource_name   = None
    max_age         = None

    fault_handler       = None
    cleanup_pretrigger  = None

    report              = None
    report_source_name  = None

    def __init__(self, resource_name, max_age, fault_handler=None, 
                 cleanup_pretrigger=None):
        logging.debug("CacheAgent(%s,%s,%s,%s)" % (resource_name, max_age, 
                                                   type(fault_handler), 
                                                   cleanup_pretrigger))

        self.registry = _CacheRegistry.get_instance(resource_name)
        self.resource_name = resource_name
        self.max_age = max_age

        self.fault_handler = fault_handler
        self.cleanup_pretrigger = cleanup_pretrigger

        self.report = Report.get_instance()
        self.report_source_name = ("cache-%s" % (self.resource_name))

        # Run a clean-up cycle to get it scheduled.
#        self.__cleanup_check()
        self.__post_status()

    def __del__(self):

        if self.report.is_source(self.report_source_name):
            self.report.remove_all_values(self.report_source_name)

    def __post_status(self):
        """Send the current status to our reporting tool."""

        try:
            num_values = self.registry.count(self.resource_name)
        except:
            logging.exception("Could not get count of values for resource with"
                              " name [%s]." % (self.resource_name))
            raise

        try:
            self.report.set_values(self.report_source_name, 'count', 
                                   num_values)
        except:
            logging.exception("Cache could not post status for resource with "
                              "name [%s]." % (self.resource_name))
            raise

        status_post_interval_s = Conf.get('cache_status_post_frequency_s')
        status_timer = Timer(status_post_interval_s, self.__post_status)
        status_timer.start()

        Timers.get_instance().register_timer('status', status_timer)

    def __cleanup_check(self):
        """Scan the current cache and determine items old-enough to be 
        removed.
        """

        logging.debug("Doing clean-up for cache resource with name [%s]." % 
                      (self.resource_name))

        try:
            cache_dict = self.registry.list_raw(self.resource_name)
        except:
            logging.exception("Could not do clean-up check with resource-name "
                              "[%s]." % (self.resource_name))
            raise

        total_keys = [ (key, value_tuple[1]) for key, value_tuple \
                            in cache_dict.iteritems() ]

        cleanup_keys = [ key for key, value_tuple \
                            in cache_dict.iteritems() \
                            if (datetime.now() - value_tuple[1]).seconds > \
                                    self.max_age ]

        logging.info("Found (%d) entries to clean-up from entry-cache." % (len(cleanup_keys)))

        if cleanup_keys:
            for key in cleanup_keys:
                logging.debug("Cache entry [%s] under resource-name [%s] will "
                              "be cleaned-up." % (key, self.resource_name))

                if self.exists(key, no_fault_check=True) == False:
                    logging.debug("Entry with ID [%s] has already been cleaned-up." % (key))
                else:
                    try:
                        self.remove(key)
                    except:
                        logging.exception("Cache entry [%s] under resource-name [%s] "
                                          "could not be cleaned-up." % 
                                          (key, self.resource_name))
                        raise

            logging.debug("Scheduled clean-up complete.")

        cleanup_interval_s = Conf.get('cache_cleanup_check_frequency_s')
        cleanup_timer = Timer(cleanup_interval_s, self.__cleanup_check)
        cleanup_timer.start()

        Timers.get_instance().register_timer('cleanup', cleanup_timer)

    def set(self, key, value):
        logging.debug("CacheAgent.set(%s,%s)" % (key, type(value)))

        return self.registry.set(self.resource_name, key, value)

    def remove(self, key):
        logging.debug("CacheAgent.remove(%s)" % (key))

        return self.registry.remove(self.resource_name, key, 
                                    cleanup_pretrigger=self.cleanup_pretrigger)

    def get(self, key, handle_fault = None):

        if handle_fault == None:
            handle_fault = True

        logging.debug("CacheAgent.get(%s)" % (key))

        try:
            result = self.registry.get(self.resource_name, key, 
                                       max_age=self.max_age, 
                                       cleanup_pretrigger=self.cleanup_pretrigger)
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

    def exists(self, key, no_fault_check=False):
        logging.debug("CacheAgent.exists(%s)" % (key))

        return self.registry.exists(self.resource_name, key, 
                                    max_age=self.max_age,
                                    cleanup_pretrigger=self.cleanup_pretrigger,
                                    no_fault_check=no_fault_check)

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
                                 cleanup_pretrigger=self.cleanup_pretrigger)

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

    def cleanup_pretrigger(self, resource_name, key, force):
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
    path_cache_byid = { }

    @staticmethod
    def get_instance():

        with PathRelations.rlock:
            try:
                return _CacheRegistry.__instance;
            except:
                pass

            _CacheRegistry.__instance = PathRelations()
            return _CacheRegistry.__instance

    def remove_entry_recursive(self, entry_id, is_update=False):
        """Remove an entry, all children, and any newly orphaned parents."""

        logging.info("Doing recursive removal of entry with ID [%s]." % (entry_id))

        to_remove = deque([ entry_id ])
        stat_placeholders = 0
        stat_folders = 0
        stat_files = 0
        removed = { }
        while 1:
            if not to_remove:
                break

            current_entry_id = to_remove.popleft()

            logging.debug("RR: Entry with ID (%s) will be removed. (%d) "
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
                logging.debug("Could not remove entry with ID [%s] "
                              "(recursive)." % (current_entry_id))
                raise

            removed[current_entry_id] = True

            (current_orphan_ids, current_children_clauses) = result

            logging.debug("RR: Entry removed. (%d) orphans and (%d) children "
                          "were reported." % (len(current_orphan_ids), 
                                                len(current_children_clauses)))

            children_ids_to_remove = [ children[3] for children 
                                                in current_children_clauses ]

            to_remove.extend(current_orphan_ids)
            to_remove.extend(children_ids_to_remove)

        logging.debug("RR: Removal complete. (%d) PH, (%d) folders, (%d) files removed." % (stat_placeholders, stat_folders, stat_files))

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
                logging.exception("Could not remove invalid entry with ID "
                                  "[%s]." % (entry_id))
                raise
            
            # Clip from path cache.

            if entry_id in self.path_cache_byid:
                logging.debug("Entry found in path-cache. Removing.")

                path = self.path_cache_byid[entry_id]
                del self.path_cache[path]
                del self.path_cache_byid[entry_id]

            else:
                logging.debug("Entry with ID [%s] did not need to be removed "
                              "from the path cache." % (entry_id))

            # Clip us from the list of children on each of our parents.

            entry_parents = entry_clause[1]
            entry_children_tuples = entry_clause[2]

            parents_to_remove = [ ]
            children_to_remove = [ ]
            if entry_parents:
                logging.debug("Entry to be removed has (%d) parents." % (len(entry_parents)))

                for parent_clause in entry_parents:
                    # A placeholder has an entry and parents field (fields 
                    # 0, 1) of None.

                    (parent, parent_parents, parent_children, parent_id, \
                        all_children_loaded) = parent_clause

                    if all_children_loaded and not is_update:
                        all_children_loaded = False

                    logging.debug("Adjusting parent with ID [%s]." % 
                                  (parent_id))

                    # Integrity-check that the parent we're referencing is 
                    # still in the list.
                    if parent_id not in self.entry_ll:
                        logging.warn("Parent with ID [%s] on entry with ID "
                                     "[%s] is not valid." % (parent_id, \
                                                                entry_id))
                        continue
            
                    old_children_filenames = [ child_tuple[0] for child_tuple 
                                                in parent_children ]

                    logging.debug("Old children: %s" % 
                                  (', '.join(old_children_filenames)))

                    updated_children = [ child_tuple for child_tuple 
                                         in parent_children 
                                         if child_tuple[1] != entry_clause ]

                    if parent_children != updated_children:
                        parent_children[:] = updated_children

                    else:
                        logging.error("Entry with ID [%s] referenced parent "
                                      "with ID [%s], but not vice-versa." % 
                                      (entry_id, parent_id))

                    updated_children_filenames = [ child_tuple[0] 
                                                    for child_tuple
                                                    in parent_children ]

                    logging.debug("Up. children: %s" % 
                                  (', '.join(updated_children_filenames)))

                    # If the parent now has no children and is a placeholder, 
                    # advise that we remove it.
                    if not parent_children and parent == None:
                        parents_to_remove.append(parent_id)

            else:
                logging.debug("Entry to be removed either has no parents, or is"
                              " a placeholder.")

            # Remove/neutralize entry, now that references have been removed.

            set_placeholder = len(entry_children_tuples) > 0

            if set_placeholder:
                # Just nullify the entry information, but leave the clause. We 
                # had children that still need a parent.

                logging.debug("This entry has (%d) children. We will leave a "
                              "placeholder behind." % 
                              (len(entry_children_tuples)))

                entry_clause[0] = None
                entry_clause[1] = None
            else:
                logging.debug("This entry does not have any children. It will "
                              "be completely removed.")

                try:
                    del self.entry_ll[entry_id]
                except:
                    logging.exception("Could not remove entry with ID [%s]. "
                                      "We've previously confirmed it to exist."
                                      " There might've been a cyclic reference"
                                      " that caused it to be removed during "
                                      " clean-up." % (entry_id))
                    raise

        if parents_to_remove:
            logging.debug("Parents that still need to be removed: %s" % 
                          (', '.join(parents_to_remove)))

        children_entry_clauses = [ child_tuple[1] for child_tuple 
                                    in entry_children_tuples ]

        logging.debug("Remove complete. (%d) entries were orphaned. There were"
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

        logging.info("Doing complete removal of entry with ID [%s]." % 
                     (entry_id))

        with PathRelations.rlock:
            logging.debug("Clipping entry with ID [%s] from PathRelations and "
                          "EntryCache." % (entry_id))

            cache = EntryCache.get_instance().cache

            removed_ids = [ entry_id ]
            if self.is_cached(entry_id):
                logging.debug("Removing found PathRelations entries.")

                try:
                    removed_tuple = self.remove_entry_recursive(entry_id, \
                                                               is_update)
                except:
                    logging.exception("Could not remove entry-ID from "
                                      "PathRelations. Still continuing, though.")

                (removed_ids, number_removed) = removed_tuple

            logging.debug("(%d) entries will now be removed from the core-cache." % (len(removed_ids)))
            for removed_id in removed_ids:
                if cache.exists(removed_id):
                    logging.debug("Removing core EntryCache entry with ID [%s]." % (removed_id))

                    try:
                        cache.remove(removed_id)
                    except:
                        logging.exception("Could not remove entry-ID from the core"
                                          " cache. Still continuing, though.")

            logging.debug("All traces of entry with ID [%s] are gone." % 
                          (entry_id))

    def dump_entry_clause(self, entry_id):
        """Shows info on a single entry_clause. Does not assume that all of "
        the information is correct/consistent.
        """

        try:
            entry_clause = self.entry_ll[entry_id]
        except:
            logging.exception("Entry with ID [%s] is not valid." % (entry_id))
            raise

        (entry, parents, children, entry_id_recorded, all_children_loaded) = \
            entry_clause

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

        with PathRelations.rlock:
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
            print("  (E= %s, P= %s, C= %s, I= %s)" % (entry_phrase, \
                    parents_phrase, children_phrase, entry_id_phrase))
            print("Title: %s" % (entry_clause[0].title if entry_clause[0] \
                                        else '<none>'))

            if entry_clause[1] == None:
                parents_extended = '(None)'
            elif not entry_clause[1]:
                parents_extended = '<empty>'
            else:
                parents_extended = ', '.join([ parent_clause[3] for parent_clause in entry_clause[1] ])

            print("Parents: %s\n" % (parents_extended))

            i += 1

    def register_entry(self, normalized_entry):

        logging.debug("We're registering entry with ID [%s] [%s]." % 
                      (normalized_entry.id, normalized_entry.title))

        with PathRelations.rlock:
            if not normalized_entry.is_visible:
                logging.info("We will not register entry with ID [%s] because it's not visible." % (normalized_entry.id))
                return None

            if normalized_entry.__class__ is not NormalEntry:
                raise Exception("PathRelations expects to register an object of "
                                "type NormalEntry, not [%s]." % 
                                (type(normalized_entry)))

            entry_id = normalized_entry.id

            logging.info("Registering entry with ID [%s] within path-relations." %
                         (entry_id))

            if self.is_cached(entry_id, include_placeholders=False):
                logging.debug("Entry to register with ID [%s] already exists "
                              "within path-relations, and will be removed in lieu "
                              "of update." % (entry_id))

                logging.debug("Removing existing entries.")

                try:
                    self.remove_entry_recursive(entry_id, True)
                except:
                    logging.exception("Could not remove existing entry with ID "
                                      "[%s] prior to its update." % 
                                      (entry_id))
                    raise

            logging.info("Doing add of entry with ID [%s]." % (entry_id))

            cache = EntryCache.get_instance().cache

            try:
                cache.set(normalized_entry.id, normalized_entry)
            except:
                logging.exception("Could not set entry with ID [%s] in "
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
                logging.debug("Placeholder exists for entry-to-register with ID [%s]." % (entry_id))

                entry_clause = self.entry_ll[entry_id]
                entry_clause[0] = normalized_entry
                entry_clause[1] = [ ]
            else:
                logging.debug("Entry does not yet exist in LL.")

                entry_clause = [normalized_entry, [ ], [ ], entry_id, False]
                self.entry_ll[entry_id] = entry_clause

            entry_parents = entry_clause[1]
            title_fs = normalized_entry.title_fs

            logging.debug("Registering entry with title [%s]." % (title_fs))
            parent_ids = [ parent_id for parent_id in normalized_entry.parents ]
            logging.debug("Parents are: %s" % (', '.join(parent_ids)))

            for parent_id in normalized_entry.parents:
                logging.debug("Processing parent with ID [%s] of entry with ID [%s]." % (parent_id, entry_id))

                # If the parent hasn't yet been loaded, install a placeholder.
                if self.is_cached(parent_id, include_placeholders=True):
                    logging.debug("Parent has an existing entry.")

                    parent_clause = self.entry_ll[parent_id]
                else:
                    logging.debug("Parent is not yet registered.")

                    parent_clause = [None, None, [ ], parent_id, False]
                    self.entry_ll[parent_id] = parent_clause

                if parent_clause not in entry_parents:
                    entry_parents.append(parent_clause)

                parent_children = parent_clause[2]

                filename_base = title_fs

                utility = get_utility()

#                if not normalized_entry.file_extension:
#                    # Append an extension to the bare filename, if available. If 
#                    # the file_extension property is available, the filename 
#                    # already has an extension.
#
#                    try:
#                        file_extension = utility.get_extension(normalized_entry)
#                    except:
#                        logging.exception("There was a problem trying to derive an "
#                                          "extension for entry with ID [%s]." %
#                                          (normalized_entry.id))
#                        raise
#
#                    if file_extension:
#                        filename_base = ("%s.%s" % (filename_base, file_extension))
#                        logging.debug("File will be given extension [%s]." % (file_extension))

                # Prepend a period if it's a hidden file.

                if u'hidden' in normalized_entry.labels \
                        and normalized_entry.labels[u'hidden']:
                    filename_base = utility. \
                        translate_filename_charset(".%s" % (filename_base))

                # Register among the children of this parent, but make sure we have 
                # a unique filename among siblings.

                i = 1
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
                    logging.error("Could not register entry with ID [%s]. There "
                                  "are too many duplicate names in that "
                                  "directory." % (entry_id))
                    return

                logging.debug("Final filename is [%s]." % (current_variation))

                # Register us in the list of children on this parent.
                parent_children.append((elected_variation, entry_clause))

        logging.debug("Entry registration complete.")

        return entry_clause

    def __load_all_children(self, parent_id):
        logging.info("Loading children under parent with ID [%s]." % 
                     (parent_id))

        with PathRelations.rlock:
            try:
                children = drive_proxy('list_files', parent_id=parent_id)
            except:
                logging.exception("Could not retrieve children for parent with"
                                  " ID [%s]." % (parent_id))
                raise

            child_ids = [ ]
            if children:
                logging.debug("(%d) children returned and will be "
                              "registered." % (len(children)))

                for child in children:
                    try:
                        self.register_entry(child)
                    except:
                        logging.exception("Could not register retrieved-entry for "
                                          "child with ID [%s] in path-cache." % 
                                          (child.id))
                        raise

                logging.debug("Looking up parent with ID [%s] for all-"
                              "children update." % (parent_id))

                try:
                    parent_clause = self.__get_entry_clause_by_id(parent_id)
                except:
                    logging.exception("Could not retrieve clause for parent-entry "
                                      "[%s] in load-all-children function." % 
                                      (parent_id))
                    raise

                parent_clause[4] = True

                logging.debug("All children have been loaded.")

        return children

    def get_child_filenames_from_entry_id(self, entry_id):
        """Return the filenames contained in the folder with the given 
        entry-ID.
        """

        logging.info("Getting children under entry with ID [%s]." % (entry_id))

        with PathRelations.rlock:
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

            else:
                logging.debug("All children for [%s] have already been loaded." % (entry_id))

            if not entry_clause[0].is_directory:
                message = ("Could not get child filenames for non-directory with "
                           "entry-ID [%s]." % (entry_id))

                logging.error(message)
                raise Exception(message)

            children_filenames = [ child_tuple[0] for child_tuple in entry_clause[2] ]

            logging.info("(%d) children found." % (len(children_filenames)))

        return children_filenames

    def get_clause_from_path(self, filepath):

        logging.info("Getting clause for path [%s]." % (filepath))

        with PathRelations.rlock:
            try:
                path_results = self.find_path_components_goandget(filepath)
            except:
                logging.exception("Could not resolve path [%s] to entry." % (filepath))
                raise

            (entry_ids, path_parts, success) = path_results

            if not success:
                return None

            entry_id = path_results[0][-1]
        
            logging.info("Found entry with ID [%s]." % (entry_id))

            # Make sure the entry is more than a placeholder.

            try:
                self.__get_entry_clause_by_id(entry_id)
            except:
                logging.exception("Clause was found for path, but entry could "
                                  "not be retrieved.")
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
                logging.info("Attempting to find path-components (go and get) for "
                             "path [%s].  CYCLE= (%d)" % (path, i))

                # See how many components can be found in our current cache.

                try:
                    result = self.__find_path_components(path)
                except:
                    logging.exception("There was a problem doing an iteration of "
                                      "find_path_components() on [%s]." % (path))
                    raise

                logging.debug("Path resolution cycle (%d) results: %s" % (i, result))

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
                                else AccountInfo.get_instance().root_id

                # The child will be the first part that was not found.
                child_name = result[1][num_results]

                logging.debug("Trying to reconcile child named [%s] under folder "
                              "with entry-ID [%s]." % (child_name, parent_id))

                try:
                    children = drive_proxy('list_files', parent_id=parent_id, query_is_string=child_name)
                except:
                    logging.exception("Could not retrieve children for parent with"
                                      " ID [%s]." % (parent_id))
                    raise
                
                for child in children:
                    try:
                        self.register_entry(child)
                    except:
                        logging.exception("Could not register child entry for "
                                          "entry with ID [%s] in path-cache." % 
                                          (child.id))
                        raise

                filenames_phrase = ', '.join([ candidate.id for candidate in children ])
                logging.debug("(%d) candidate children were found: %s" % (len(children), filenames_phrase))

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

        if path in self.path_cache:
            return self.path_cache[path]

        with PathRelations.rlock:
            logging.debug("Locating entry information for path [%s]." % (path))

            try:
                root_id = AccountInfo.get_instance().root_id
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

                logging.debug("Checking for part (%d) [%s] under parent with "
                              "ID [%s]." % (i, child_filename_to_search_fs, 
                                            entry_ptr))

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
                    logging.debug("Looking for child [%s] among (%d): %s" % 
                                  (child_filename_to_search_fs, len(children),
                                   [ child_tuple[0] for child_tuple 
                                     in children ]))

                    found = [ child_tuple[1][3] 
                              for child_tuple 
                              in children 
                              if child_tuple[0] == child_filename_to_search_fs ]

                if found:
                    logging.debug("Found matching child with ID [%s]." % (found[0]))
                    results.append(found[0])
                else:
                    logging.debug("Did not find matching child.")
                    return (results, path_parts, False)

                # Have we traveled far enough into the linked list?
                if (i + 1) >= num_parts:
                    logging.debug("Path has been completely resolved: %s" % (', '.join(results)))

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

    def is_cached(self, entry_id, include_placeholders=False):

        return (entry_id in self.entry_ll and (include_placeholders or \
                                               self.entry_ll[entry_id][0]))

PathRelations.rlock = RLock()

class EntryCache(CacheClient):
    """Manages our knowledge of file entries."""

    about = AccountInfo.get_instance()

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

        affected_entries = [ requested_entry_id ]
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

                affected_entries.append(child_id)

                if len(affected_entries) >= max_readahead_entries:
                    break

        return affected_entries

    def __do_update_for_missing_entry(self, requested_entry_id):

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

        # TODO: We have to determine when this is called, and either remove it 
        # (if it's not), or find another way to not have to load them 
        # individually.

        try:
            retrieved = drive_proxy('get_entries', entry_ids=affected_entries)
        except:
            logging.exception("Could not retrieve the (%d) entries." % (len(affected_entries)))
            raise

        # Update the cache.

        path_relations = PathRelations.get_instance()

        for entry_id, entry in retrieved.iteritems():
            try:
                path_relations.register_entry(entry)
            except:
                logging.exception("Could not register entry with ID [%s] with path-relations cache." % (entry_id))
                raise

        logging.debug("(%d) entries were loaded." % (len(retrieved)))

        return retrieved

    def fault_handler(self, resource_name, requested_entry_id):
        """A requested entry wasn't stored."""

        logging.info("EntryCache has faulted on entry with ID [%s]." % 
                      (requested_entry_id))

        try:
            retrieved = self.__do_update_for_missing_entry(requested_entry_id)
        except:
            logging.exception("Could not reconcile unknown entry with ID "
                              "[%s]." % (requested_entry_id))
            raise

        # Return the requested entry.

        try:
            return retrieved[requested_entry_id]
        except:
            logging.exception("We just updated as a result of a fault, but "
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
            logging.debug("Removing PathRelations entry for cleaned-up entry "
                          "with ID [%s]." % (entry_id))

            try:
                path_relations.remove_entry_recursive(entry_id)
            except:
                logging.exception("Could not remove PathRelations entry with "
                                  "ID [%s] on cleanup." % (entry_id))
                raise

    def get_max_cache_age_seconds(self):
        return Conf.get('cache_entries_max_age')

