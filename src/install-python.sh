#!/bin/bash


# give the exact python version as first and only the command line input for this script

sudo apt update;

sudo apt full-upgrade < "y";

sudo apt autoremove < "y";

sudo apt install wget;

python_version=$1;

python_tar_file_name=Python-$python_version.tar.xz;

python_dest_folder=Python-$python_version;

wget -c https://www.python.org/ftp/python/$python_version/$python_tar_file_name;

tar --extract --verbose --file=$python_tar_file_name $python_dest_folder;

cd $python_dest_folder;

# look into this to reduce installation time / installation dir
./configure --enable-optimizations --prefix="$(dirname $(pwd))/python-$python_version"

# mention the number of threads
sudo make -j32 && sudo make altinstall;

rm -r $python_dest_folder;
rm $python_tar_file_name;


# to uninstall go to the installation folder and run 'sudo make uninstall'

# lookinto readme for more configs. build dir can be deleted


