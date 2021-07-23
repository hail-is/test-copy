#!/bin/bash
set -ex

sudo mkfs.ext4 -F /dev/nvme0n1
sudo mkdir -p /data
sudo mount /dev/nvme0n1 /data
sudo chown $(whoami) /data

sudo apt -y update
sudo apt -y install python3-pip
git clone https://github.com/cseed/test-copy.git
git clone https://github.com/cseed/hail.git

pushd hail
git remote add is https://github.com/hail-is/hail.git
git fetch --all
git checkout origin/main
make -C hail python-version-info
popd

python3 -m pip install -r hail/docker/requirements.txt
python3 -m pip install numpy
