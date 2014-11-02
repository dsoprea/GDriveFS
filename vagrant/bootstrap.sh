#!/usr/bin/env bash

echo "PYTHONDONTWRITEBYTECODE=1" >> /etc/environment

# Setup the package-cache.

mount --bind /vagrant/.packages /var/cache/apt/archives
apt-get install -y python-pip build-essential python-dev

# Install dependencies.

pip install ez_setup
pip install -r /gdrivefs/requirements.txt

# Add "gdrivefs" to PYTHONPATH and install executables into path.

cd /gdrivefs
python setup.py develop

# Create a mountpoint.

mkdir /mnt/gdrivefs

# Get the user going with an authorization URL.

echo
gdfstool auth -u

# Mention follow-up steps.
# *For some reason, we can't print empty strings, so we print single-spaces.

echo " "
echo "Once you have retrieved your authorization string, run:"
echo " "
echo "sudo gdfstool auth -a /var/cache/gdfs.creds <auth string>"
echo " "
