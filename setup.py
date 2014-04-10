#!/usr/bin/env python2.7

from setuptools import find_packages, setup

import versioneer

versioneer.VCS = 'git'
versioneer.versionfile_source = 'gdrivefs/_version.py'
versioneer.versionfile_build = 'gdrivefs/_version.py'
versioneer.tag_prefix = ''
versioneer.parentdir_prefix = 'gdrivefs-'

long_description = "A complete FUSE adapter for Google Drive. See Github for "\
                   "more information."

setup(name='gdrivefs',
      version=versioneer.get_version(),
      description="A complete FUSE adapter for Google Drive.",
      long_description=long_description,
      classifiers=['Topic :: System :: Filesystems',
                   'Development Status :: 4 - Beta',
                   'Environment :: Console',
                   'Intended Audience :: End Users/Desktop',
                   'Intended Audience :: System Administrators',
                   'License :: OSI Approved :: BSD License',
                   'Natural Language :: English',
                   'Operating System :: POSIX',
                   'Programming Language :: Python',
                   'Topic :: Internet',
                   'Topic :: Utilities'],
      keywords='google-drive google drive fuse filesystem',
      author='Dustin Oprea',
      author_email='myselfasunder@gmail.com',
      url='https://github.com/dsoprea/GDriveFS',
      license='GPL 2',
      packages=find_packages(exclude=['dev','tests']),
      include_package_data=True,
      zip_safe=False,
      install_requires=[
        'google_appengine',
# TODO: There's an issue when this is listed as a requirement here. It can be 
#       installed separately, easily. The repository version is old, anyways.
#       We advise that people download and install it manually.
#        'google_api_python_client',
        'httplib2',
        'python-dateutil',
        'fusepy',
      ],
      cmdclass=versioneer.get_cmdclass(),
      scripts=['tools/gdfs',
               'tools/gdfstool',
               'tools/gdfsuninstall'],
      )

