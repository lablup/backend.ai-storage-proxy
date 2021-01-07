#!/bin/sh
sudo chmod 755 /
sudo chmod 755 -R /var/cache/
sudo apt install python3.8

sudo apt-get install -y \
build-essential git-core                                     # for generic C/C++ builds
libreadline-dev libsqlite3-dev libssl-dev libbz2-dev tk-dev \ # for Python builds
libzmq3-dev libsnappy-dev libffi-dev

git clone https://github.com/pyenv/pyenv.git ~/.pyenv
echo 'export PYENV_ROOT="$HOME/.pyenv"' >> ~/.profile
$ echo 'export PATH="$PYENV_ROOT/bin:$PATH"' >> ~/.profile
$ echo 'eval "$(pyenv init -)"' >> ~/.profile

git clone https://github.com/pyenv/pyenv-virtualenv.git ~/.pyenv/plugins/pyenv-virtualenv
echo 'eval "$(pyenv virtualenv-init -)"' >> ~/.profile
$ exec $SHELL -l

pyenv install 3.8.6
pyenv virtualenv 3.8.6 storage-proxy
pyenv global storage-proxy
mkdir /home/vagrant/storage_proxy/
cd /home/vagrant/storage_proxy/
pyenv local storage-proxy
git clone https://github.com/lablup/backend.ai-storage-agent storage_proxy
pip install backend.ai-common
# python3.8 -m pip install --upgrade pip
pip install -U -r requirements/dev.txt
cp config/sample.toml storage-proxy.toml

# At storage-proxy.toml
# change proxy ip address to vagrant ceph-client ip addres. Set manager ip addres of the Host machine.
# set ceph path to /mnt/vfroot/ceph-fuse under local.myceph
# update mgr allowed_vfolder_hosts = '{local:myceph}'
# on manager side, backend.ai mgr etcd put-json volumes volume.json
