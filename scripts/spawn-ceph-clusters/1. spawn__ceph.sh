#!/bin/sh
mkdir vagrant_box/
cd vagrant_box
vagrant plugin install vagrant-cachier
vagrant plugin install vagrant-hostmanager
ssh-add -K ~/.vagrant.d/insecure_private_key
vagrant up
vagrant ssh ceph-admin
mkdir test-cluster && cd test-cluster

## Add in ./ceph.conf: osd pool default size = 2

ceph-deploy install --release=octopus ceph-admin ceph-server-1 ceph-server-2 ceph-server-3 ceph-client

ceph-deploy mon create-initial
ssh ceph-server-2 "sudo mkdir /var/local/osd0 && sudo chown ceph:ceph /var/local/osd0"
ssh ceph-server-3 "sudo mkdir /var/local/osd1 && sudo chown ceph:ceph /var/local/osd1"

ceph-deploy osd prepare ceph-server-2:/var/local/osd0 ceph-server-3:/var/local/osd1
ceph-deploy osd activate ceph-server-2:/var/local/osd0 ceph-server-3:/var/local/osd1

ceph-deploy admin ceph-admin ceph-server-1 ceph-server-2 ceph-server-3 ceph-client
sudo chmod +r /etc/ceph/ceph.client.admin.keyring
ssh ceph-server-1 sudo chmod +r /etc/ceph/ceph.client.admin.keyring
ssh ceph-server-2 sudo chmod +r /etc/ceph/ceph.client.admin.keyring
ssh ceph-server-3 sudo chmod +r /etc/ceph/ceph.client.admin.keyring

ceph-deploy mgr create ceph-admin:mon_mgr

ceph-deploy mds create ceph-server-1

ceph osd pool create rbd 150 150


sudo apt-get install ceph-fuse
sudo mkdir /mnt/vfroot
sudo mkdir /mnt/vfroot/ceph-fuse

sudo ceph-fuse -n client.admin --keyring=/etc/ceph/ceph.client.admin.keyring  -m ceph-server-1 /mnt/vfroot/ceph-fuse/

sudo apt-get install attr
