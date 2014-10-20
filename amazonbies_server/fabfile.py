from __future__ import with_statement

import os

from fabric.context_managers import lcd, cd
from fabric.api import local, run

def restart_services():
    local('sudo service nginx restart')
    local('sudo service uwsgi restart')


def deploy_local(dir="/var/www/product"):
    local('sudo cp -r cta %s/' % dir)
    restart_services()

def deploy(version="develop"):
    run('rm -rf %s' % version)
    run('mkdir %s' % version)
    with cd(version):
        run('git clone -b %s git@github.com:Gogolook-Inc/cta.git' % version)
    with cd("%s/cta" % version):
        run('fab deploy_local:dir=/var/www/%s' % version)
