###############################################################################
# Copyright (C) 2013 Dustin Oprea                                             #
# License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher       #
#                                                                             #
# See https://github.com/dsoprea/RandomUtility for the full collection of     #
# tools.                                                                      #
###############################################################################

from os import environ, symlink, chmod, unlink
from os.path import join, basename, dirname, exists, abspath
from stat import S_IEXEC

_default_prefix = environ.get('PREFIX', '/usr/local')

def _get_physical_path(fq_module_spec):
    (fq_package, ignore_, module_name) = fq_module_spec.rpartition('.')

    package = __import__(fq_package, fromlist=[module_name])
    module = getattr(package, module_name)

    module_filepath = abspath(module.__file__)
    module_path = dirname(module_filepath)
    module_filename = basename(module_filepath)
    
    (filename_root, ignore_, extension) = module_filename.rpartition('.')

    # We assume that there's always a "py" version of the filename available.
    if extension == 'pyc':
        module_filename = ('%s.py' % (filename_root))
        module_filepath = join(module_path, module_filename)

    print("Module [%s] refers to: %s" % (fq_module_spec, module_filepath))
    return module_filepath

def _install_tool_symlink(fq_module_spec, deposit_path_rel):
    module_filepath = _get_physical_path(fq_module_spec)
    module_filename = basename(module_filepath)

    (filename_noext, ignore_, ignore_) = module_filename.rpartition('.')

    deposit_path = join(_default_prefix, deposit_path_rel)
    deposit_filepath = join(deposit_path, filename_noext)

    # If it already exists, kill it.
    if exists(deposit_filepath) is True:
        print("Removing existing symlink: %s" % (deposit_filepath))
        unlink(deposit_filepath)
    
    print("Creating executable symlink at [%s] with target [%s]." % 
          (deposit_filepath, module_filepath))

    symlink(module_filepath, deposit_filepath)

    # Permissions should already be set correctly.

    print("")

def install_su_tool_symlink(fq_module_spec):
    _install_tool_symlink(fq_module_spec, 'sbin')

def install_user_tool_symlink(fq_module_spec):
    _install_tool_symlink(fq_module_spec, 'bin')

