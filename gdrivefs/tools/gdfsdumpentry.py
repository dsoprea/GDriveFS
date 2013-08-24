#!/usr/bin/env python2.7

from sys import exit
from argparse import ArgumentParser

from gdrivefs.cache.volume import PathRelations, EntryCache, path_resolver, \
                                  CLAUSE_ENTRY
from gdrivefs.gdfs.fsutility import split_path, build_filepath
from gdrivefs.gdfs.gdfuse import set_auth_cache_filepath
from gdrivefs.timer import Timers

def get_by_path(raw_path):
    try:
        result = split_path(raw_path, path_resolver)
        (parent_clause, path, filename, mime_type, is_hidden) = result
    except:
        print("Could not process file-path [%s]." % (raw_path))
        exit()

    filepath = build_filepath(path, filename)
    
    path_relations = PathRelations.get_instance()

    try:
        entry_clause = path_relations.get_clause_from_path(filepath)
    except GdNotFoundError:
        print("Could not retrieve clause for non-existent file-path [%s]." % 
              (filepath))
        exit()
    except:
        print("Could not retrieve clause for path [%s]. " % (filepath))
        exit()

    if not entry_clause:
        print("Path [%s] does not exist for stat()." % (filepath))
        exit()

    return entry_clause[CLAUSE_ENTRY]

def get_by_id(_id):
    cache = EntryCache.get_instance().cache

    return cache.get(_id)

if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('cred_filepath', help='Credentials file')

    subparsers = parser.add_subparsers(help='subcommand help')
    parser_bypath = subparsers.add_parser('bypath', help='Path-based lookups.')
    parser_bypath.add_argument('path', help='Path to identify')
    parser_byid = subparsers.add_parser('byid', help='Path-based lookups.')
    parser_byid.add_argument('id', help='Specific entry to identify')

    args = parser.parse_args()

    set_auth_cache_filepath(args.cred_filepath)
    Timers.get_instance().set_autostart_default(False)

    if 'id' in args:
        entry = get_by_id(args.id)
    
    if 'path' in args:
        entry = get_by_path(args.path)

    print(entry)
    print    

    data = entry.get_data()

    for _type, _dict in data.iteritems():
        print("[%s]\n" % (_type))

        for key, value in _dict.iteritems():
            print("%s: %s" % (key, value))

        print

