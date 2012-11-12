#!/usr/bin/python

from os import symlink
from os.path import dirname

import tools

tool_path = dirname(tools.__file__)

gdfs_filepath = ('%s/%s' % (tool_path, 'gdfs'))
gdfs_symlink_filepath = '/usr/sbin/gdfs'

gdfstool_filepath = ('%s/%s' % (tool_path, 'gdfstool'))
gdfstool_symlink_filepath = '/usr/sbin/gdfstool'

print("Writing gdfs symlink.")
symlink(gdfs_symlink_filepath, gdfs_symlink_filepath)

print("Writing gdfstool symlink.")
symlink(gdfstool_filepath, gdfstool_symlink_filepath)


# Ensure FUSE.

