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
    run('sudo apt-get update')
    run('sudo apt-get install git')
    run('sudo apt-get install nginx')
    run('sudo /etc/init.d/nginx start')
    run('sudo apt-get install python-pip')
    run('sudo apt-get install python-dev')
    run('sudo rm /etc/nginx/sites-enabled/default')

    deploy(version)

def deploy(version="product"):

    run('rm -rf %s' % version)
    run('mkdir %s' % version)
    with cd(version):
        run('git clone -b %s git@github.com:LeoHung/amazonbies.git' % version)

        with cd('amazonbies/amazonbies_server'):
            run('sudo pip install -r amazonbies/requirements.txt')
            run('sudo mkdir -p /var/www')
            run('sudo mkdir -p /var/www/amazonbies')
            run('sudo cp -r amazonbies/ /var/www/%s/' % version)
            run('sudo ln -f -s Settings/nginx/product.conf /etc/nginx/conf.d/amazonbies.conf')
            run('sudo /etc/init.d/nginx restart')

            run('sudo mkdir -p /var/log/uwsgi')
            run('sudo chown -R ubuntu:ubuntu /var/log/uwsgi')

            run('sudo ln -f -s /home/ubuntu/amazonbies/amazonbies_server/Settings/nginx/product.conf  /etc/nginx/conf.d/')
            run('sudo /etc/init.d/nginx restart')

            run('sudo rm -rf /tmp/amazonbies_uwsgi.sock')
            run('uwsgi --ini Settings/uwsgi/product.ini')

            run('sudo chown www-data:www-data /tmp/amazonbies_uwsgi.sock')

