# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import base64
import logging
import shutil
import subprocess

import luigi
import os
import stat
from envparse import env
from luigi.contrib.mysqldb import MySqlTarget
from luigi.contrib.postgres import PostgresTarget
from luigi.task import MixinNaiveBulkComplete

from helper.credentials import MySQLCredentialsMixin
from helper.csv_format import CSVFormat
from helper.helper import get_mysql_dump_filepath, load_content_from_file
from helper.mysql import MySqlQuery, MySqlQueryMultiple
from helper.postgres import CopyToMetabaseTable, UpsertDataToTable, UpsertDataToManyTables

logger = logging.getLogger('luigi-interface')
env.read_envfile()


class PrepareSSHKey(luigi.Task):
    def output(self):
        return luigi.LocalTarget('.ssh/id')

    def run(self):
        keyfile_content = base64.b64decode(env('BACKUP_SERVER_KEYFILE_CONTENT'))

        with self.output().open('w') as output:
            output.write(keyfile_content)

        os.chmod(os.path.join(os.getcwd(), self.output().path), stat.S_IREAD | stat.S_IWRITE)


class DownloadWoocommerceDump(luigi.Task):
    date = luigi.DateParameter()

    def requires(self):
        return PrepareSSHKey()

    def output(self):
        return luigi.LocalTarget(get_mysql_dump_filepath(self.date))

    def run(self):
        file_name = env('BACKUP_DUMP_FILENAMEFORMAT_REMOTE').format(date=self.date)
        remote_file = '{user}@{server}:{dir}{file}'.format(user=env('BACKUP_SERVER_USER'),
                                                              server=env('BACKUP_SERVER'),
                                                              dir=env('BACKUP_SERVER_DUMP_DIR'),
                                                              file=file_name)

        local_file = '{file}.gz'.format(file=get_mysql_dump_filepath(self.date))

        scp_cmd = ['scp', '-q', '-C', '-i', '.ssh/id_waihona', '-o', 'StrictHostKeyChecking=no']
        scp_cmd.extend([remote_file, local_file])
        scp = subprocess.Popen(scp_cmd)
        output, _ = scp.communicate()
        if scp.returncode != 0:
            raise subprocess.CalledProcessError(scp.returncode, scp_cmd, output=output)

        gunzip_cmd = ['gunzip', local_file]
        gunzip = subprocess.Popen(gunzip_cmd)
        output, _ = gunzip.communicate()
        if gunzip.returncode != 0:
            raise subprocess.CalledProcessError(gunzip.returncode, gunzip_cmd, output=output)


class ImportWoocommerceDump(MySqlQueryMultiple):
    table = '_mysql_import_'
    query = '_load_from_input_'

    def requires(self):
        return DownloadWoocommerceDump(self.date)

    def output(self):
        """
        We are marking the import as completed in our Postgres DB
        """

        return PostgresTarget(
            host=env('PGSQL_HOST_AND_PORT'),
            database=env('PGSQL_DATABASE'),
            user=env('PGSQL_USER'),
            password=env('PGSQL_PASSWORD'),
            table=self.table,
            update_id=self.update_id
        )


class MySQLTransformStepAfterImport(MySqlQueryMultiple):
    def requires(self):
        return ImportWoocommerceDump(self.date)

    def output(self):
        """
        We are marking the completion of the transform step as
        completed in our Postgres DB because the MariaDB is not present
        at the moment of checking completeness of the task
        """

        return PostgresTarget(
            host=env('PGSQL_HOST_AND_PORT'),
            database=env('PGSQL_DATABASE'),
            user=env('PGSQL_USER'),
            password=env('PGSQL_PASSWORD'),
            table=self.table,
            update_id=self.update_id
        )


class CreateProductCategoryTable(MySQLTransformStepAfterImport):
    table = 'category_tree'
    query = load_content_from_file('./sql/products__category_tree.sql')


class CreateProductLastOrderTable(MySQLTransformStepAfterImport):
    table = 'products__last_order_date'
    query = load_content_from_file('./sql/products__last_order_date.sql')


class ExtractWoocommerceDataToCSV(MySqlQuery):
    """
    MySqlQuery needs a table attribute to work, but its not necessary for us
    You only need it if your output is a MySqlTarget!
    """

    table = '_data_extraction_'

    def requires(self):
        return ImportWoocommerceDump(self.date)

    def inner_run(self, connection, cursor):
        cursor.execute(self.query)
        rows = cursor.fetchall()

        with self.output().open('w') as output:
            for row in rows:
                self.write_row(output, row)

    def write_row(self, output, row):
        """
        Override this to change default behaviour
        of writing the wohle data set unaltered to the CSV

        :param output: The output to which the row should be written
        :param row: The content of the row
        """
        output.write(*row)

    def _get_filepath(self):
        return './intermediates/{}/{}'.format(self.date, self.filename)

    def output(self):
        return luigi.LocalTarget(self._get_filepath(), format=CSVFormat())


class ExtractWoocommerceAddresses(ExtractWoocommerceDataToCSV):
    filename = 'addresses.csv'
    query = load_content_from_file('./sql/addresses.sql')


class ExtractWoocommerceCustomers(ExtractWoocommerceDataToCSV):
    filename = 'customers.csv'
    query = load_content_from_file('./sql/customers.sql')


class ExtractWoocommerceOrderItems(ExtractWoocommerceDataToCSV):
    filename = 'orderitems.csv'
    query = load_content_from_file('./sql/orderitems.sql')


class ExtractWoocommerceOrders(ExtractWoocommerceDataToCSV):
    filename = 'orders.csv'
    query = load_content_from_file('./sql/orders.sql')


class ExtractWoocommerceProducts(ExtractWoocommerceDataToCSV):
    filename = 'products.csv'
    query = load_content_from_file('./sql/products.sql')

    def requires(self):
        yield CreateProductCategoryTable(self.date)
        yield CreateProductLastOrderTable(self.date)


class ExtractWoocommerceStock(ExtractWoocommerceDataToCSV):
    filename = 'stock.csv'
    query = load_content_from_file('./sql/stock.sql')

    def write_row(self, output, row):
        l = list(row)
        l.append(str(self.date))
        output.write(*tuple(l))

class ExtractCampaignTrackings(ExtractWoocommerceDataToCSV):
    filename = 'campaign_trackings.csv'
    query = load_content_from_file('./sql/campaign_trackings.sql')


class RunETL(MySQLCredentialsMixin, MixinNaiveBulkComplete, luigi.Task):
    date = luigi.DateParameter()
    legacy = luigi.BoolParameter(default=False)

    def requires(self):
        if not self.legacy:
            yield UpsertOrdersAddressesCustomers(self.date)
            yield UpsertCampaignTrackings(self.date)

        yield InsertStock(self.date)

    def drop_woocommerce_database(self):
        database = 'woocommerce_{date:%Y_%m_%d}'.format(date=self.date)
        query = 'DROP DATABASE IF EXISTS {db};'.format(db=database)

        mysql_db = MySqlTarget(
            host='{}:{}'.format(self.host, self.port),
            database=database,
            user=self.user,
            password=self.password,
            table='_removing_whole_database_',
            update_id=self.task_id
        )

        connection = mysql_db.connect()
        cur = connection.cursor()
        cur.execute(query)
        # we don't touch this connection here - we are marking the completeness in run method via PostgresTarget
        # commit and close connection
        connection.commit()
        connection.close()

    def remove_files(self):
        # intermediates
        intermediates_path = './intermediates/{date}'.format(date=self.date)
        if os.path.isdir(intermediates_path):
            shutil.rmtree(intermediates_path)

        # db dump
        db_dump_file = get_mysql_dump_filepath(self.date)
        if os.path.exists(db_dump_file):
            os.remove(db_dump_file)

    def run(self):
        self.drop_woocommerce_database()
        self.remove_files()

        pg_connection = self.output().connect()
        self.output().touch(pg_connection)
        pg_connection.commit()
        pg_connection.close()

    def output(self):
        """
        We are marking the cleanup as completed in our Postgres DB
        because the MySQL DB will be destroyed
        """

        return PostgresTarget(
            host=env('PGSQL_HOST_AND_PORT'),
            database=env('PGSQL_DATABASE'),
            user=env('PGSQL_USER'),
            password=env('PGSQL_PASSWORD'),
            table='_removed_whole_database_',
            update_id=self.task_id
        )


class UpsertOrdersAddressesCustomers(UpsertDataToManyTables):
    date = luigi.DateParameter()

    def requires(self):
        all_requirements = super(UpsertOrdersAddressesCustomers, self).requires()
        all_requirements.update({
            'orders': ExtractWoocommerceOrders(self.date),
            'addresses': ExtractWoocommerceAddresses(self.date),
            'customers': ExtractWoocommerceCustomers(self.date),
            'orders_products': ExtractWoocommerceOrderItems(self.date),
            'upsert_products_first': UpsertProducts(self.date),
            'upsert_campaign_trackings_first': UpsertCampaignTrackings(self.date),
        })

        return all_requirements

    def after_upsert(self, cursor):
        cursor.execute(
            """
            UPDATE customers c
            SET last_billing_address_id = (
               SELECT o.billig_address_id
               FROM orders o
               WHERE o.id = (
                  SELECT MAX(mo.id) FROM orders mo WHERE mo.customer_id = c.id
               )
            )
            """
        )

    table = 'many_tables'

    tables = {
        'orders': {
            'columns': (
                'id',
                'customer_id',
                'order_date',
                'order_completed_date',
                'payment_method',
                'order_total',
                'order_tax',
                'order_net',
                'discount_net',
                'discount_tax',
                'subtotal',
                'subtotal_tax',
                'subtotal_net',
                'billig_address_id',
                'shipping_address_id',
                'last_contact_campaign_id',
                'first_contact_campaign_id',
                'last_contact_campaign_recorded_at',
                'first_contact_campaign_recorded_at',
            ),
            'match_on': 'id'
        },

        'addresses': {
            'columns': (
                'type',
                'customer_id',
                'address1',
                'address2',
                'company',
                'firstname',
                'lastname',
                'zipcode',
                'city',
                'state',
                'country',
                'id',
            ),
            'match_on': 'id'
        },

        'customers': {
            'columns': (
                'id',
                'firstname',
                'lastname',
                'phone',
                'email',
                'registered_date',
                'first_order_id',
                'first_order_date',
                'last_order_id',
                'last_order_date',
                'origin',
                'last_billing_address_id',
            ),
            'match_on': 'id'
        },

        'orders_products': {
            'columns': (
                'order_item_id',
                'order_id',
                'subqty_id',
                'delivered_on',
                'returned_on',
                'is_retoure',
                'product_id',
                'product_name',
                'price_net',
                'price_tax',
                'discount_net',
                'discount_tax',
                'payment_net',
                'payment_tax',
            ),
            'match_on': ('order_item_id', 'subqty_id')
        }
    }


class UpsertCampaignTrackings(UpsertDataToTable):
    date = luigi.DateParameter()

    def requires(self):
        all_requirements = super(UpsertCampaignTrackings, self).requires()
        all_requirements.update({
            'campaign_trackings': ExtractCampaignTrackings(self.date)
        })

        return all_requirements

    table = 'campaign_trackings'
    match_on = 'id'

    columns = (
        'id',
        'utm_medium',
        'utm_source',
        'utm_campaign',
        'utm_term',
        'utm_content',
    )


class UpsertProducts(UpsertDataToTable):
    date = luigi.DateParameter()

    def requires(self):
        all_requirements = super(UpsertProducts, self).requires()
        all_requirements.update({
            'products': ExtractWoocommerceProducts(self.date)
        })

        return all_requirements

    table = 'products'
    match_on = 'id'

    columns = (
        'id',
        'name',
        'parent_id',
        'thumbnail_id',
        'thumbnail_url',
        'category',
        'primary_category',
        'super_category',
        'uvp',
        'cost_of_goods',
        'color',
        'brand',
        'order_season',
        'season_fruehling',
        'season_sommer',
        'season_herbst',
        'season_winter',
        'sex',
        'size',
        'size_shopfilter',
        'item_condition',
        'created_at',
        'updated_at',
        'last_order_at',
    )


class InsertStock(CopyToMetabaseTable):
    date = luigi.DateParameter()

    def requires(self):
        all_requirements = super(InsertStock, self).requires()

        all_requirements.update({
            'data': ExtractWoocommerceStock(self.date),
            'upsert_products_first': UpsertProducts(self.date)
        })

        return all_requirements

    table = 'stock'
    columns = ('product_id', 'stock', 'purchased_stock', 'record_timestamp')


if __name__ == '__main__':
    luigi.run(['RangeDaily',
               '--of', 'RunETL',
               '--start', '2017-01-22',
               '--param-name', 'date',
               '--reverse',
               '--workers', '8'])
