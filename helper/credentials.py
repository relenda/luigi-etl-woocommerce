# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from envparse import env

env.read_envfile()


class PostgresCredentialsMixin(object):
    host = env('PGSQL_HOST_AND_PORT')
    database = env('PGSQL_DATABASE')
    user = env('PGSQL_USER')
    password = env('PGSQL_PASSWORD')


class MySQLCredentialsMixin(object):
    host = env('MYSQL_HOST')
    port = env('MYSQL_PORT')
    database = env('MYSQL_DATABASE')
    user = env('MYSQL_USER')
    password = env('MYSQL_PASSWORD')
