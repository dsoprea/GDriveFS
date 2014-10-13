#!/usr/bin/env bash

# Setup the package-cache.

sudo mount --bind /vagrant/.packages /var/cache/apt/archives
sudo apt-get install -y python-pip build-essential python-dev

# Install dependencies.

sudo pip install ez_setup
sudo pip install -r /gdrivefs/requirements.txt

# Add "gdrivefs" to PYTHONPATH and install executables into path.

cd /gdrivefs
sudo python setup.py develop

# Create a mountpoint.

sudo mkdir /mnt/gdrivefs

# Get the user going with an authorization URL.

echo
sudo gdfstool auth -u

# Mention follow-up steps.
# *For some reason, we can't print empty strings, so we print single-spaces.

echo " "
echo "Once you have retrieved your authorization string, run:"
echo " "
echo "sudo gdfstool auth -a /var/cache/gdfs.creds <auth string>"
echo " "
