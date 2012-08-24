import logging

from sys            import getfilesystemencoding
from collections    import OrderedDict
from threading      import Lock
from datetime       import datetime

from utility import get_utility

class CacheFault(Exception):
    pass

class _CacheRegistry(object):
    cache = { }

    def __init__(self):
        pass

    @staticmethod
    def get_instance(resource_name):
    
        logging.debug("CacheRegistry(%s)" % (resource_name))

        try:
            _CacheRegistry.instance;
        except:
            try:
                _CacheRegistry.instance = _CacheRegistry()
            except:
                logger.exception("Could not manufacture singleton "
                                 "CacheRegistry instance.")
                raise

        if resource_name not in _CacheRegistry.instance.cache:
            _CacheRegistry.instance.cache[resource_name] = { }

        return _CacheRegistry.instance

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
        
        logging.debug("CacheRegistry.get(%s,%s,%s,%s)" % (resource_name, key, 
                      max_age, cleanup_trigger))

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
    registry        = None
    resource_name   = None
    max_age         = None

    fault_handler   = None
    cleanup_trigger = None

    def __init__(self, resource_name, max_age, fault_handler=None, cleanup_trigger=None):
        logging.debug("CacheAgent(%s,%s,%s,%s)" % (resource_name, max_age, 
                                                   type(fault_handler), 
                                                   cleanup_trigger))

        self.registry = _CacheRegistry.get_instance(resource_name)
        self.resource_name = resource_name
        self.max_age = max_age

        self.fault_handler = fault_handler
        self.cleanup_trigger = cleanup_trigger

        instance = _CacheRegistry.instance

    def set(self, key, value):
        logging.debug("CacheAgent.set(%s,%s)" % (key, type(value)))

        return self.registry.set(self.resource_name, key, value)

    def remove(self, key):
        logging.debug("CacheAgent.remove(%s)" % (key))

        return self.registry.remove(self.resource_name, key, cleanup_trigger=self.cleanup_trigger)

    def get(self, key):
        logging.debug("CacheAgent.get(%s)" % (key))

        try:
            result = self.registry.get(self.resource_name, key, 
                                       max_age=self.max_age, 
                                       cleanup_trigger=self.cleanup_trigger)
        except (CacheFault):
            if self.fault_handler == None:
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
    
    cache = None

    def __init__(self):
        child_type = self.__class__.__bases__[0].__name__
        max_age = self.get_max_cache_age_seconds()
        
        logging.debug("CacheClient(%s,%s)" % (child_type, max_age))

        self.cache = _CacheAgent(child_type, max_age, 
                                 fault_handler=self.fault_handler, 
                                 cleanup_trigger=self.cleanup_trigger)

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

        try:
            CacheClient.instance
        except:
            CacheClient.instance = cls()

        return CacheClient.instance

class EntryCache(CacheClient):

    def fault_handler(self, resource_name, key):
        pass

    def cleanup_trigger(self, resource_name, key, force):
        pass

    def get_max_cache_age_seconds(self):
        return None

class _FileCache(object):
    """An in-memory buffer of the files that we're aware of."""

    entry_cache         = { }
    cleanup_index       = OrderedDict()
#    name_index          = { }
#    name_index_r        = { }
    filepath_index      = { }
    filepath_index_r    = { }
# TODO: The following includes duplicates of the above.
    paths           = { }
    paths_by_name   = { }
    root_entries    = [ ]
    entry_ll        = { }

    locker = Lock()
    latest_change_id = None
    local_character_set = getfilesystemencoding()

    def get_cached_entries(self):
        return self.entry_cache

    def cleanup_by_id(self, id):
        with self.locker:
            try:
                del self.cleanup_index[id]

            except:
                pass

#            try:
#                parent_id = self.name_index_r[id]
#                del self.name_index_r[id]
#                del self.name_index[parent_id][id]
#
#            except:
#                pass

            try:
                filepath = self.filepath_index_r[id]
                del self.filepath_index_r[id]
                del self.filepath_index[filepath]

            except:
                pass

            try:
                del self.entry_cache[id]

            except:
                pass

    def register_entry(self, parent_id, entry, filepath):
        """Register file in the cache. We assume that the file-path is unique 
        (no duplicates).
        """

        entry_id = entry[u'id']

        # TODO: Register in our granular entry cache. We don't do anything with
        #       this right now, and the current class/method will probably go 
        #       away.

        cache = EntryCache.get_instance().cache

        try:
            cache[entry_id] = entry
        except:
            logging.exception("Could not set entry with ID [%s] in entry-"
                              "cache." % (entry_id))
            raise

        self.cleanup_by_id(entry_id)

        with self.locker:
            # Store the entry.

            # Keep a forward and reverse index for the file-paths so that we 
            # can allow look-up and clean-up based on IDs while also allowing 
            # us to efficiently manage naming duplicity.

            if filepath in self.filepath_index:
                raise Exception("File-path [%s] is already recorded in the "
                                "cache with a different ID [%s]." % (filepath, 
                                                                    entry_id))

            self.filepath_index[filepath] = entry_id
            self.filepath_index_r[entry_id] = filepath

            # An ordered-dict to keep track of the tracked files by add order.
            self.entry_cache[entry_id] = entry
#            logging.info("ParentID: %s" % (parent_id))
#            # A hash for the heirarchical structure.
#            if parent_id not in self.name_index:
#                self.name_index[parent_id] = { entry_id: entry }
#
#            else:
#                self.name_index[parent_id][entry_id] = entry
#
#            self.name_index_r[entry_id] = parent_id

            # Delete it from the clean-up index.

            try:
                del self.cleanup_index[entry_id]
            except:
                pass

            # Now, add it to the end of the clean-up index.
            self.cleanup_index[entry_id] = entry

    def get_entry_by_filepath(self, filepath):
        logging.info("Retrieving entry for file-path [%s] from list of (%d) entries." % (filepath, len(self.filepath_index)))

        with self.locker:
            try:
                entry_id = self.filepath_index[filepath]
                entry = self.entry_cache[entry_id]

                logging.debug("Found as [%s]." % (entry_id))
            except:
                return None

            return entry

    def get_entry_by_id(self, id):
        with self.locker:
            if id in self.entry_cache:
                return entry_cache[id]

            else:
                return None

    def get_latest_change_id(self):
        return self.latest_change_id

    def apply_changes(self, changes):
        # Sort by change-ID (integer) in ascending order.

        logging.debug("Sorting changes to be applied.")

        sorted_changes = sorted(changes.items(), key=lambda t: t[0])
        updates = 0

        with self.locker:
            for change_id, change in sorted_changes:
                logging.debug("Applying change with ID (%d)." % (change_id))

                # If we've already processed updates, skip everything we've already 
                # processed.
                if self.latest_change_id != None and \
                        self.latest_change_id >= change_id:
                    logging.debug("The current change-ID (%d) is less than the"
                                  " last recorded change-ID (%d)." % 
                                  (change_id, self.latest_change_id))
                    continue

                (entry_id, was_deleted, entry) = change

                # Determine if we're already up-to-date.

                if entry_id in self.entry_cache:
                    logging.debug("We received a change item for entry-ID [%s]"
                                  " in our cache." % (entry_id))

                    local_entry = self.entry_cache['entry_id']

                    local_mtime = local_entry[u'modifiedDate']
                    date_obj = dateutil.parser.parse(local_mtime)
                    local_mtime_epoch = time.mktime(date_obj.timetuple())

                    remote_mtime = entry[u'modifiedDate']
                    date_obj = dateutil.parser.parse(remote_mtime)
                    remote_mtime_epoch = time.mktime(date_obj.timetuple())

                    # The local version is newer or equal-to this change.
                    if remote_mtime_epoch <= local_mtime_epoch:
                        logging.info("Change will be ignored because its mtime"
                                     " is [%s] and the one we have is [%s]." % 
                                     (remote_mtime, local_mtime))
                        continue

                # If we're here, our data for this file is old or non-existent.

                updates += 1

                if was_deleted:
                    logging.info("File [%s] will be deleted." % (entry_id))

                    try:
                        self.cleanup_by_id(entry_id)
                    except:
                        logging.exception("Could not cleanup deleted file with"
                                          " ID [%s]." % (entry_id))
                        raise

                else:
                    logging.info("File [%s] will be inserted/updated." % 
                                 (entry_id))

#                    try:
#                        self.register_entry(None, None, entry)
#                    except:
#                        logging.exception("Could not register changed file "
#                                          "with ID [%s].  WAS_DELETED= (%s)" % 
#                                          (entry_id, was_deleted))
#                        raise
        
                logging.info("Update successful.")

                # Update our tracker for which changes have been applied.
                self.latest_change_id = change_id

            logging.info("(%d) updates were performed." % (updates))

    def _is_invisible(self, entry):
        labels = entry[u'labels']
        if labels[u'hidden'] or labels[u'trashed']:
            return True

        return False

    def _build_ll(self, entry_list):
        """Build a linked list of directory-entries. We need it to determine 
        the heirarchy, as well as to calculate the full pathnames of the 
        constituents.
        """

        filtered_list = [ ]
        entry_ll = { }
        for entry in entry_list:
            # At this point, we'll filter any files that we want to hide.
            if self._is_invisible(entry):
                continue

            filtered_list.append(entry)

            entry_id = entry[u'id']
            entry_ll[entry_id] = [entry, None, []]

        root_entries = [ ]
        for entry in filtered_list:
            entry_id = entry[u'id']
            entry_record = entry_ll[entry_id]

            in_root = False
            for parent in entry[u'parents']:
                parent_id = parent[u'id']

                if parent[u'isRoot']:
                    in_root = True

                # If we're not in the root, link to the parent, and vice-versa. 
                # Only do this if the parent has a record, which won't happen 
                # if we've filtered it (above).
                elif parent_id in entry_ll:
                    parent_record = entry_ll[parent_id]

                    entry_record[1] = parent_record
                    parent_record[2].append(entry_record)

            if in_root:
                root_entries.append(entry_record)

        return (root_entries, entry_ll)

    def _translate_filename_charset(self, original_filename):
        """Make sure we're in the right character set."""
        
        return original_filename.encode(self.local_character_set)

    def _build_heirarchy(self, entry_list_raw):
        """Build a heirarchical model of the filesystem."""

        logging.info("Building file heirarchies.")

        # Build a list of relations (as a linked-list).

        try:
            (root_entries, entry_ll) = self._build_ll(entry_list_raw)
        except:
            logging.exception("Could not build heirarchy from files.")
            raise

        path_cache = { }
        def get_path(linked_entry, depth = 1):
            """A recursive path-name finder."""

            if depth > 8:
                raise Exception("Could not calculate paths for folder heirarchy"
                                " that's too deep.")

            if not linked_entry:
                return ''

            entry = linked_entry[0]
            entry_id = entry[u'id']

            if entry_id in path_cache:
                return path_cache[entry_id]

            parent_path = get_path(linked_entry[1], depth + 1)
            path = ("%s/%s" % (parent_path, entry[u'title']))

            # If it's not a folder, try to find an extension to attach to it.

            utility = get_utility()

            if not utility.is_folder(entry):
                try:
                    extension = utility.get_extension(entry)
                except:
                    logging.exception("Could not attempt to derive an extension "
                                      "for entry with ID [%s] and mime-type "
                                      "[%s]." % (entry_id, entry[u'mimeType']))
                    raise

                if extension != None:
                    path = ("%s.%s" % (path, extension))

            path = self._translate_filename_charset(path)
            path_cache[entry_id] = path

            return path

        # Produce a dictionary of entry-IDs and unique file-paths.

        paths = { }
        paths_by_name = { }
        for entry_id, linked_entry in entry_ll.iteritems():
            path = get_path(linked_entry)
            
            current_variation = path
            elected_variation = None
            i = 1
            while i < 256:
                if current_variation not in paths_by_name:
                    elected_variation = current_variation
                    break

                i += 1
                current_variation = self._translate_filename_charset("%s (%d)" % (path, i))
            
            if elected_variation == None:
                logging.error("There were too many duplicates of filename [%s]."
                              " We will have to hide all excess entries." % 
                              (base))
                continue

            paths[entry_id] = elected_variation
            paths_by_name[elected_variation] = entry_id

        return (paths, paths_by_name, root_entries, entry_ll)

    def get_children_by_path(self, path):
        if path == '/':
            entries = [ ]
            for linked_entry in self.root_entries:
                entry_id = linked_entry[0][u'id']
                entries.append(entry_id)

            return entries

        elif path not in self.paths_by_name:
            message = "Path [%s] not found in cache."

            logging.error(message)
            raise Exception(message)

        else:
            entry_id = self.paths_by_name[path]
            return [child[0][u'id'] for child in self.entry_ll[entry_id][2]]

    def get_filepaths_for_entries(self, entry_id_list):

        filepaths = { }
        for entry_id in entry_id_list:
            filepaths[entry_id] = self.filepath_index_r[entry_id]

        return filepaths

    def init_heirarchy(self, entry_list_raw):

        logging.info("Initializing file heirarchies.")

        try:
            heirarchy = self._build_heirarchy(entry_list_raw)
        except:
            logging.exception("Could not build heirarchy.")
            raise

        (paths, paths_by_name, root_entries, entry_ll) = heirarchy

        self.paths          = paths
        self.paths_by_name  = paths_by_name
        self.root_entries   = root_entries
        self.entry_ll       = entry_ll

        logging.info("Registering entries in cache.")

        for entry_id, linked_entry in self.entry_ll.iteritems():
            entry = linked_entry[0]
            parent = linked_entry[1]

            if parent:
                parent_id = parent[0][u'id']
            else:
                parent_id = None

            try:
                self.register_entry(parent_id, entry, self.paths[entry_id])
            except:
                logging.exception("Could not register entry with ID [%s] with "
                                  "the cache." % (entry_id))
                raise

        logging.info("All entries registered.")

        return self.paths

def get_cache():
    if get_cache.instance == None:
        try:
            get_cache.instance = _FileCache()
        except:
            logging.exception("Could not manufacture FileCache.")
            raise

    return get_cache.instance

get_cache.instance = None

# TODO: Start a cache clean-up thread to make sure that all old items at the 
# beginning of the cleanup_index are constantly pruned.

