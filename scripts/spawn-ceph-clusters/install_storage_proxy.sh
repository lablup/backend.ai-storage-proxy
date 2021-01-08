
sudo chmod 755 /
cd $HOME/

sudo apt install -y python3.8

sudo apt-get install -y build-essential git-core libreadline-dev libsqlite3-dev libssl-dev libbz2-dev tk-dev libzmq3-dev libsnappy-dev libffi-dev

sudo git clone https://github.com/pyenv/pyenv.git "$HOME/.pyenv"
echo 'export PYENV_ROOT="$HOME/.pyenv"' >> ~/.profile
echo 'export PATH="$PYENV_ROOT/bin:$PATH"' >> ~/.profile
echo 'eval "$(pyenv init -)"' >> ~/.profile
echo "1======"
echo 'export PYENV_ROOT="$HOME/.pyenv"' >> ~/.bashrc
echo 'export PATH="$PYENV_ROOT/bin:$PATH"' >> ~/.bashrc
echo 'eval "$(pyenv init -)"' >> ~/.bashrc
echo "2======"

sudo git clone https://github.com/pyenv/pyenv-virtualenv.git "$HOME/.pyenv/plugins/pyenv-virtualenv"
echo 'eval "$(pyenv virtualenv-init -)"' >> ~/.profile
echo 'eval "$(pyenv virtualenv-init -)"' >> ~/.bashers
sudo chmod 777 -R .pyenv/
echo "3======"

eval "$(pyenv init -)"
eval "$(pyenv virtualenv-init -)"
echo "4======"

pyenv install 3.8.6
pyenv virtualenv 3.8.6 storage-proxy
pyenv global storage-proxy
sudo git clone https://github.com/lablup/backend.ai-storage-agent /vagrant/storage-proxy/
cd /vagrant/storage-proxy/
pyenv local storage-proxy

pip install backend.ai-common
# python3.8 -m pip install --upgrade pip
pip install -U -r requirements/dev.txt
cp /vagrant/storage-proxy.toml storage-proxy.toml
sudo apt-get -y install ceph-fuse
sudo apt-get -y install attr

sudo mkdir /mnt/vfroot/
sudo mkdir /mnt/vfroot/ceph-fuse/
sudo ceph-fuse -n client.admin --keyring=/etc/ceph/ceph.client.admin.keyring  -m ceph-server-1 /mnt/vfroot/ceph-fuse/ &
python -m ai.backend.storage.server &
exit