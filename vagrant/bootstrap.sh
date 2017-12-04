#!/usr/bin/env bash

echo "PYTHONDONTWRITEBYTECODE=1" >> /etc/environment

# Setup the package-cache.

mount --bind /vagrant/.packages /var/cache/apt/archives
apt-get update

apt-get install -y python-pip build-essential python-dev

# Install dependencies.

pip install -U pip
pip install -r /gdrivefs/requirements.txt

# Add "gdrivefs" to PYTHONPATH and install executables into path.

pip install -e /gdrivefs

# Create a mountpoint.
#
# We can't use /mnt/gdrivefs, because, for some reason, when we unmount that in
# the Vagrant machine, it unmounts the shared-folder the the same name, too.

mkdir /mnt/g
sudo chown vagrant:vagrant /mnt/g

# Configure us to run as non-root.

sudo chmod 755 /etc/fuse.conf
sudo adduser vagrant fuse

# Get the user going with an authorization URL.

echo
gdfstool auth_get_url

# Mention follow-up steps.
# *For some reason, we can't print empty strings, so we print single-spaces.

echo " "
echo "Once you have retrieved your authorization string, run:"
echo " "
echo "  gdfstool auth_write <authcode>"
echo " "
echo " "
echo "Once authorized, you can mount by calling (as root):"
echo " "
echo "  gdfs default /mnt/g"
echo " "
echo "Or, to debug (and disable change-monitoring, to simplify things):"
echo " "
echo "  GD_DEBUG=1 GD_MONITOR_CHANGES=0 gdfs default /mnt/g"
echo " "
echo " "
