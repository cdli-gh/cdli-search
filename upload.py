#!/usr/bin/env python3

' Upload data from the the CDLI catalogue to Elasticsearch for indexing.'

import os

from datetime import datetime
from logging import info, warn, error
from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk

from cdli import read_catalogue


def index_bodies(rows):
    '''Construct a sequence of Elasticsearch index actions.

    Pass in a sequence or iterable of dictionaries representing
    the entries in a metadata catalogue row.'''
    for row in rows:
        # Decorate the row dictionary with metadata.
        # First, make sure we're not clobbering any native keys.
        assert '_id' not in row
        assert '_type' not in row

        # Now add keys for the properties we want to specify.
        # The indexer will treat the rest as the document body.
        row['_id'] = row['id_text']
        row['_type'] = 'metadata'
        yield row


def index_entries(data_path):
    'Upload each row in the catalogue data for indexing.'

    host = os.environ.get('ELASTICSEARCH_URL', 'localhost')
    es = Elasticsearch(host)

    index_base = 'cdli-catalogue'
    index_name = f'{index_base}-{datetime.utcnow().date()}'

    info(f'Indexing under {index_name}...')
    failures = 0
    successes = 0
    for ok, result in streaming_bulk(
            es,
            index_bodies(read_catalogue(data_path)),
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
            warn(f"Failed to index {cdli_no}!")
            continue
        successes += 1
        info(f"{index_name} {result['_seq_no']} {cdli_no}")

    info(f'Successfully indexed {successes} catalogue entries.')
    if failures:
        error(f'FAILED to index {failures} entries.')

    # Update the main index alias.
    es.indices.put_alias(index=index_name, name=index_base)
    info(f'Updated index alias {index_base}')


def index_clear():
    host = os.environ.get('ELASTICSEARCH_URL', 'localhost')
    es = Elasticsearch(host)
    es.indices.delete('cdli-catalog*')


if __name__ == '__main__':
    import argparse
    import logging

    p = argparse.ArgumentParser(
            description='Upload catalogue data to an Elasticsearch instance.')
    p.add_argument('data_path',
                   help='directory containing the catalogue data files')
    p.add_argument('-q', '--quiet', action='store_true',
                   help='only print warnings and errors')
    args = p.parse_args()

    level = logging.INFO
    if args.quiet:
        level = logging.WARNING
    logging.basicConfig(level=level)

    info(f'Loading catalogue from {args.data_path}')
    index_entries(args.data_path)
