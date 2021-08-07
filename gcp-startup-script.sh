#!/bin/bash

mkfs.ext4 -F /dev/nvme0n1
mkdir /data
mount /dev/nvme0n1 /data
chown ubuntu /data

apt -y update
apt -y upgrade
apt -y install python3-pip

sudo -u ubuntu bash -c '
cd /home/ubuntu
git clone https://github.com/hail-is/hail.git
cd hail
git checkout is/main
make -C /home/ubuntu/hail/hail python-version-info
'

python3 -m pip install -r /home/ubuntu/hail/hail/python/requirements.txt
