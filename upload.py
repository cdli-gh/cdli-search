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


def index_bodies(rows):
    '''Construct a sequence of Elasticsearch index actions.

    Pass in a sequence or iterable of dictionaries representing
    the entries in a metadata catalogue row.'''
    for row in rows:
        # Just add metadata keys. The indexer will treat the
        # rest of the keys as part of the document.
        row['_id'] = row['id_text']
        row['_type'] = 'metadata'
        yield row


def index_entries(filenames):
    'Upload each row in the catalogue data for indexing.'

    host = os.environ.get('ELASTICSEARCH_URL', 'localhost')
    es = Elasticsearch(host)

    index_base = 'cdli-catalogue'
    index_name = f'{index_base}-{datetime.utcnow().date()}'

    print(f'Indexing under {index_name}...')
    failures = 0
    successes = 0
    for ok, result in streaming_bulk(
            es,
            index_bodies(read_catalogue(filenames)),
            index=index_name,
    ):
        action, result = result.popitem()
        id = result['_id']
        try:
            # Convert numeric ids to P-numbers.
            cdli_no = f'P{int(id):06d}'
        except ValueError:
            # Use others as-is.
            cdli_no = id
        if not ok:
            failures += 1
            print(f"Failed to index {cdli_no}!")
            continue
        successes += 1
        print(index_name, result['_seq_no'], cdli_no)

    print(f'Successfully indexed {successes} catalogue entries.')
    if failures:
        print(f'FAILED to index {failures} entries.')


if __name__ == '__main__':
    filenames = [os.path.join('../cdli-data', fn) for fn in files]
    index_entries(filenames)
