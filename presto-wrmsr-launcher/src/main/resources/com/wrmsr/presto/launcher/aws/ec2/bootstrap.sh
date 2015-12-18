#!/usr/bin/env bash
set -e

echo 'export TERM=screen' >> ~/.bashrc
echo 'export PATH="~/.local/bin:$PATH"' >> ~/.bashrc
source ~/.bashrc

sudo bash -c 'echo 127.0.0.1 `hostname` >> /etc/hosts'

ISSUE=$(cat /etc/issue | head -n 1)

if [[ $ISSUE == "Ubuntu"* ]] ; then
    sudo apt-get update
    # sudo DEBIAN_FRONTEND=noninteractive apt-get -y -o Dpkg::Options::="--force-confdef" -o Dpkg::Options::="--force-confold" dist-upgrade
    sudo DEBIAN_FRONTEND=noninteractive apt-get -y upgrade
    sudo apt-get install -y htop tcpdump iotop tmux mtr mosh iftop ack-grep python-pip msr-tools silversearcher-ag

elif [[ $ISSUE == "Amazon Linux"* ]] ; then
    sudo yum update -y
    sudo yum install -y htop tcpdump iotop tmux mtr mosh ack
    wget http://pkgs.repoforge.org/iftop/iftop-0.17-1.el6.rf.x86_64.rpm
    sudo rpm -ivh iftop-0.17-1.el6.rf.x86_64.rpm

fi

wget -c --no-cookies --no-check-certificate --header "Cookie: gpw_e24=http%3A%2F%2Fwww.oracle.com%2F; oraclelicense=accept-securebackup-cookie" "https://download.oracle.com/otn-pub/java/jdk/8u60-b27/jdk-8u60-linux-x64.tar.gz" --output-document="jdk-8u60-linux-x64.tar.gz"
tar xvzf jdk-8u60-linux-x64.tar.gz

pip install --user ipython

if [ ! -f /dev/mem ]; then
    sudo mknod -m 660 /dev/mem c 1 1
    sudo chown root:kmem /dev/mem
fi
if [ ! -f /dev/kmem ]; then
    sudo mknod -m 640 /dev/kmem c 1 2
    sudo chown root:kmem /dev/kmem
fi
if [ ! -f /dev/port ]; then
    sudo mknod -m 660 /dev/port c 1 4
    sudo chown root:mem /dev/port
fi
