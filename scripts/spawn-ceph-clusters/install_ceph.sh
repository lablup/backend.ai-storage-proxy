#!/bin/sh
sudo chmod 755 /
echo "StrictHostKeyChecking no" >> .ssh/config
mkdir test-cluster && cd test-cluster/

ssh-keyscan 172.21.12.10 172.21.12.11 172.21.12.12 172.21.12.13
ssh-keyscan ceph-server-1 ceph-server-2 ceph-server-3 ceph-server-client
yes | ceph-deploy new ceph-server-1 ceph-server-2 ceph-server-3

yes | ceph-deploy install --release=octopus ceph-admin ceph-server-1 ceph-server-2 ceph-server-3 ceph-client
ceph-deploy mon create-initial
ssh ceph-server-2 "sudo mkdir /var/local/osd0 && sudo chown ceph:ceph /var/local/osd0"
ssh ceph-server-3 "sudo mkdir /var/local/osd1 && sudo chown ceph:ceph /var/local/osd1"

ceph-deploy osd prepare ceph-server-2:/var/local/osd0 ceph-server-3:/var/local/osd1
ceph-deploy osd activate ceph-server-2:/var/local/osd0 ceph-server-3:/var/local/osd1

yes | ceph-deploy admin ceph-admin ceph-server-1 ceph-server-2 ceph-server-3 ceph-client
sudo chmod +r /etc/ceph/ceph.client.admin.keyring
ssh ceph-server-1 sudo chmod +r /etc/ceph/ceph.client.admin.keyring
ssh ceph-server-2 sudo chmod +r /etc/ceph/ceph.client.admin.keyring
ssh ceph-server-3 sudo chmod +r /etc/ceph/ceph.client.admin.keyring
ssh ceph-client   sudo chmod +r /etc/ceph/ceph.client.admin.keyring
ceph-deploy mgr create ceph-admin:mon_mgr

ceph-deploy mds create ceph-server-1

ceph osd pool create rbd 150 150

sudo apt-get -y install ceph-fuse
echo "Done"
exit
