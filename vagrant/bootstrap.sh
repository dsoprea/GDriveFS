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
#
# We can't use /mnt/gdrivefs, because, for some reason, when we unmount that in 
# the Vagrant machine, it unmounts the shared-folder the the same name, too.

mkdir /mnt/g

# Get the user going with an authorization URL.

echo
gdfstool auth -u

# Mention follow-up steps.
# *For some reason, we can't print empty strings, so we print single-spaces.

echo " "
echo "Once you have retrieved your authorization string, run:"
echo " "
echo "  sudo gdfstool auth -a /var/cache/gdfs.creds <auth string>"
echo " "
echo " "
echo "Once authorized, you can mount by calling (as root):"
echo " "
echo "  gdfs /var/cache/gdfs.creds /mnt/g"
echo " "
echo "Or, to debug (and disable change-monitoring, to simplify things):"
echo " "
echo "  GD_DEBUG=1 GD_MONITOR_CHANGES=0 gdfs /var/cache/gdfs.creds /mnt/g"
echo " "
echo " "
