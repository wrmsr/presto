#!/usr/bin/env bash
set -e

export TERM=screen
echo 'export TERM=screen' >> ~/.bashrc
sudo bash -c 'echo 127.0.0.1 `hostname` >> /etc/hosts'

ISSUE=$(cat /etc/issue | head -n 1)
if [[ $ISSUE == "Ubuntu"* ]] ; then
    sudo apt-get update
    sudo DEBIAN_FRONTEND=noninteractive apt-get -y -o Dpkg::Options::="--force-confdef" -o Dpkg::Options::="--force-confold" dist-upgrade
    sudo apt-get install htop tcpdump iotop tmux mtr mosh iftop ack-grep python-pip msr-tools

elif [[ $ISSUE == "Amazon Linux"* ]] ; then
    sudo yum update -y
    sudo yum install -y htop tcpdump iotop tmux mtr mosh ack
    wget http://pkgs.repoforge.org/iftop/iftop-0.17-1.el6.rf.x86_64.rpm
    sudo rpm -ivh iftop-0.17-1.el6.rf.x86_64.rpm

fi

wget -c --no-cookies --no-check-certificate --header "Cookie: gpw_e24=http%3A%2F%2Fwww.oracle.com%2F; oraclelicense=accept-securebackup-cookie" "https://download.oracle.com/otn-pub/java/jdk/8u60-b27/jdk-8u60-linux-x64.tar.gz" --output-document="jdk-8u60-linux-x64.tar.gz"
tar xvzf jdk-8u60-linux-x64.tar.gz

pip install --user ipython

