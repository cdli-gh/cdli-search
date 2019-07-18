#!/usr/bin/env python3

' Upload data from the the CDLI catalogue to Elasticsearch for indexing.'

import io
import os
import csv
import fileinput

from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk

files = [
    'cdli_catalogue_1of2.csv',
    'cdli_catalogue_2of2.csv',
]


def as_utf8(filename, mode='r'):
    '''Return a file opened as UTF-8 text.

    The gives correct unicode strings on systems where the
    default text encoding is different.'''
    return io.open(filename, mode, encoding='utf-8')


def read_catalogue(filenames):
    '''Concatenate and read the catalog file data.

    The catalogue data is split into multiple smaller files
    to fit better in a git repository. Open these in sequence
    and yield a series of dictionaries representing each row.

    The keys in the dictionary are taken from the column labels
    on the first row.'''

    with fileinput.input(files=filenames, openhook=as_utf8) as csvfile:
        for row in csv.DictReader(csvfile):
            yield row


def print_entries(filenames):
    'Dump each row in the catalogue for debugging.'
    for row in read_catalogue(filenames):
        print('P' + row['id_text'], row['designation'])


def index_entries(filenames):
    'Upload each row in the catalogue data for indexing.'

    host = os.environ.get('ELASTICSEARCH_URL', 'localhost')
    es = Elasticsearch(host)

    index_name = f'cdli-catalogue-{datetime.utcnow().date()}'

    print(f'Indexing under {index_name}...')
    for ok, result in streaming_bulk(
            es,
            read_catalogue(filenames),
            index=index_name,
    ):
        action, result = result.popitem()
        if not ok:
            print(f"Failed to index {result['_id']}!")
            continue
        print(index_name, result['_seq_no'], result['_id'])



if __name__ == '__main__':
    filenames = [os.path.join('../cdli-data', fn) for fn in files]
    index_entries(filenames)
