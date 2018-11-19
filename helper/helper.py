# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import codecs

from envparse import env


def get_mysql_dump_filepath(date):
    file_name = env('BACKUP_DUMP_FILENAMEFORMAT_LOCAL').format(date=date)
    return './dumps/{}'.format(file_name)


def load_content_from_file(filename):
    with codecs.open(filename, 'r', encoding='utf8') as f:
        return f.read()
