#!/usr/bin/env python2.7

import os.path
import setuptools

import gdrivefs

_APP_PATH = os.path.dirname(gdrivefs.__file__)

with open(os.path.join(_APP_PATH, 'resources', 'README.rst')) as f:
      long_description = f.read()

with open(os.path.join(_APP_PATH, 'resources', 'requirements.txt')) as f:
      install_requires = [s.strip() for s in f.readlines()]

setuptools.setup(
    name='gdrivefs',
    version=gdrivefs.__version__,
    description="A complete FUSE adapter for Google Drive.",
    long_description=long_description,
    classifiers=[
        'Topic :: System :: Filesystems',
        'Development Status :: 4 - Beta',
        'Environment :: Console',
        'Intended Audience :: End Users/Desktop',
        'Intended Audience :: System Administrators',
        'License :: OSI Approved :: BSD License',
        'Natural Language :: English',
        'Operating System :: POSIX',
        'Programming Language :: Python',
        'Topic :: Internet',
        'Topic :: Utilities'
    ],
    keywords='google-drive google drive fuse filesystem',
    author='Dustin Oprea',
    author_email='myselfasunder@gmail.com',
    url='https://github.com/dsoprea/GDriveFS',
    license='GPL 2',
    packages=setuptools.find_packages(exclude=['dev', 'tests']),
    include_package_data=True,
    package_data={
        'gdrivefs': [
            'resources/README.rst',
            'resources/requirements.txt',
        ],
    },
    zip_safe=False,
    install_requires=install_requires,
    scripts=[
        'gdrivefs/resources/scripts/gdfs',
        'gdrivefs/resources/scripts/gdfstool',
        'gdrivefs/resources/scripts/gdfsdumpentry',
    ],
)
