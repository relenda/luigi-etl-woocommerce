# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import logging
import re

import luigi
from envparse import env
from luigi.contrib.mysqldb import MySqlTarget
from luigi.contrib.rdbms import Query
from mysql.connector.errors import OperationalError, ProgrammingError

from helper.helper import load_content_from_file
from .credentials import MySQLCredentialsMixin

logger = logging.getLogger('luigi-interface')
env.read_envfile()

try:
    import mysql.connector
    from mysql.connector import errorcode
except ImportError as e:
    logger.warning('Loading MySQL module without the python package mysql-connector-python. \
        This will crash at runtime if MySQL functionality is used.')


class MySqlQuery(MySQLCredentialsMixin, Query):
    """
    Code lend from PostgresQuery

    Template task for querying a MySQL compatible database

    Usage:
    Subclass and override the required `host`, `database`, `user`, `password`, `table`, and `query` attributes.

    Override the `run` method if your use case requires some action with the query result.

    Task instances require a dynamic `update_id`, e.g. via parameter(s), otherwise the query will only execute once

    To customize the query signature as recorded in the database marker table, override the `update_id` property.
    """

    date = luigi.DateParameter()

    def run(self):
        self.database = 'woocommerce_{date:%Y_%m_%d}'.format(date=self.date)

        self._maybe_create_database()

        connection = mysql.connector.connect(
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port,
            database=self.database
        )

        cur = connection.cursor()

        self.inner_run(connection, cur)

        # commit and close connection
        connection.commit()
        connection.close()

    def inner_run(self, connection, cursor):
        cursor.execute(self.query)
        # Update marker table
        self.output().touch(connection)

    def _maybe_create_database(self):
        connection_db_creation = mysql.connector.connect(
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port
        )
        cursor_db_creation = connection_db_creation.cursor()
        cursor_db_creation.execute('CREATE DATABASE IF NOT EXISTS {};'.format(self.database))
        connection_db_creation.commit()
        connection_db_creation.close()

    def output(self):
        """
        Returns a MySqlTarget representing the executed query.

        Normally you don't override this.
        """

        self.database = 'woocommerce_{date:%Y_%m_%d}'.format(date=self.date)

        return MySqlTarget(
            host=self.host,
            database=self.database,
            user=self.user,
            password=self.password,
            table=self.table,
            update_id=self.update_id
        )


class MySqlQueryMultiple(MySqlQuery):
    def inner_run(self, connection, cursor):

        if self.query and self.query != '_load_from_input_':
            sqlFileContent = self.query
        else:
            sqlFileContent = load_content_from_file(self.input().path)

        # all SQL commands (split on ';\n' (newline))
        sqlCommands = re.split(";\n", sqlFileContent)

        # Execute every command from the input file
        for command in sqlCommands:
            if command.isspace():
                continue

            # This will skip and report errors
            # For example, if the tables do not yet exist, this will skip over
            # the DROP TABLE commands
            try:
                cursor.execute(command)
            except OperationalError, msg:
                print "Command skipped: ", msg
            except ProgrammingError, msg:
                raise Exception(
                    "ProgrammingError '{}' while executing following sql command: {}".format(msg,command)
                )

        self.output().touch()
