from __future__ import with_statement

import os

from fabric.context_managers import lcd, cd
from fabric.api import local, run

import time

def restart_services():
    local('sudo service nginx restart')
    local('sudo service uwsgi restart')

def deploy_local(dir="/var/www/product"):
    local('sudo cp -r cta %s/' % dir)
    restart_services()

def first_deploy(version="product"):
    run('sudo apt-get -y update')
    run('sudo apt-get -y install git')
    run('sudo apt-get -y install nginx')
    run('sudo /etc/init.d/nginx start')
    run('sudo apt-get -y install python-pip')
    run('sudo apt-get -y install python-dev')
    run('sudo rm -rf /etc/nginx/sites-enabled/default')
    run('sudo apt-get -y install memcached')

    deploy(version)

def deploy(version="product"):

    run('rm -rf %s' % version)
    run('mkdir %s' % version)
    with cd(version):
        run('git clone -b %s git@github.com:LeoHung/amazonbies.git' % version)

        with cd('amazonbies/amazonbies_server'):
            run('sudo pip install -r amazonbies/requirements.txt')
            run('sudo mkdir -p /var/www')
            run('sudo mkdir -p /var/www/%s/' % version)
            run('sudo cp -r amazonbies /var/www/%s/' % version)
            run('sudo /etc/init.d/nginx restart')

            run('sudo mkdir -p /var/log/uwsgi')
            run('sudo chown -R ubuntu:ubuntu /var/log/uwsgi')

            run('sudo ln -f -s /home/ubuntu/product/amazonbies/amazonbies_server/Settings/nginx/product.conf  /etc/nginx/conf.d/')
            run('sudo /etc/init.d/nginx restart')

            run('sudo rm -rf /tmp/amazonbies_uwsgi.sock')
            run('uwsgi --ini Settings/uwsgi/product.ini')

            run('sudo chown www-data:www-data /tmp/amazonbies_uwsgi.sock')

def keygen():
    with cd('~/.ssh/'):
        run('ssh-keygen')
        run('cat id_rsa.pub')

