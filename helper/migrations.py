# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import luigi
from luigi.contrib import postgres

from .credentials import PostgresCredentialsMixin
from .helper import load_content_from_file


class MigrateMetabaseSchema(luigi.WrapperTask):
    """
    We could use this WrapperTask to register even more migrations for the future!
    """

    def requires(self):
        yield MigrateMetabaseSchemaInitial()
        yield MigrateMetabaseSchemaNewProductFields()
        yield MigrateMetabaseSchemaCustomerLatestOrderAndProductLatestOrder()
        yield MigrateMetabaseSchemaCampaignTrackings()


class MigrateMetabaseSchemaInitial(PostgresCredentialsMixin, postgres.PostgresQuery):
    update_id = 'initial_migration'
    table = '_initial_migration_'
    query = load_content_from_file('./migrations/001_initial.sql')


class MigrateMetabaseSchemaNewProductFields(PostgresCredentialsMixin, postgres.PostgresQuery):
    update_id = '002_add_product_thumbnailurl_and_sex'
    table = '_migration_'
    query = load_content_from_file('./migrations/002_product_thumbnailurl_category_sex.sql')

    def requires(self):
        return MigrateMetabaseSchemaInitial()


class MigrateMetabaseSchemaCustomerLatestOrderAndProductLatestOrder(PostgresCredentialsMixin, postgres.PostgresQuery):
    update_id = '003_customer_last_order_and_product_last_order_date'
    table = '_migration_'
    query = load_content_from_file('./migrations/003_customer_last_order_and_product_last_order_date.sql')

    def requires(self):
        return MigrateMetabaseSchemaInitial()

class MigrateMetabaseSchemaCampaignTrackings(PostgresCredentialsMixin, postgres.PostgresQuery):
    update_id = '004_campaign_trackings'
    table = '_migration_'
    query = load_content_from_file('./migrations/004_campaign_trackings.sql')

    def requires(self):
        return MigrateMetabaseSchemaInitial()
