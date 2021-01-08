# Readme


# Installation
1. Setup vagrant environment and vagrant machine nodes
 ```
 mkdir vagrant_box/
 cd vagrant_box
 vagrant plugin install vagrant-cachier
 vagrant plugin install vagrant-hostmanager
 ssh-add -K ~/.vagrant.d/insecure_private_key
 vagrant up
```
2. Login to ceph-admin node and execute ceph-deployment script

```
vagrant ssh ceph-admin
sh /vagrant/install_ceph.sh
exit
```

3. Login to ceph-client and install ceph-fuse and Storage Proxy.
```
vagrant ssh ceph-client
sh /vagrant/install_storage_proxy.sh
```

4. Storage proxy settings
The Storage proxy is installed at root /vagrant directory which is also shared with Host machine.
The ceph-fuse mounted path is /mnt/vfroot/ceph-fuse
The storage-proxy.toml will contain updated path 
```
[volume.myceph]
backend="cephfs"
path="/mnt/vfroot/ceph-fuse"
```
Etcd IP address should be updated to proper one based on Host machine.

5. Manager settings
In volume .json set the vagrant network ip address of ceph-client node.
```
backend.ai mgr etcd put-json volumes volume.json
update domains set allowed_vfolder_hosts = '{local:myceph}' ;
update groups set allowed_vfolder_hosts = '{local:myceph}' ;
update keypair_resource_policies set allowed_vfolder_hosts = '{local:myceph}' ;
```
