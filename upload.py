#!/usr/bin/env python3

' Upload data from the the CDLI catalogue to Elasticsearch for indexing.'

import io

import os
import csv
import fileinput

files = [
    'cdli_catalogue_1of2.csv',
    'cdli_catalogue_2of2.csv',
]


def as_utf8(filename, mode):
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
    'Dump each entry for debugging.'
    for row in read_catalogue(filenames):
        print('P' + row['id_text'], row['designation'])


if __name__ == '__main__':
    filenames = [os.path.join('../cdli-data', fn) for fn in files]
    print_entries(filenames)
