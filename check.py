#!/usr/bin/env python3

'Print info from the CDLI catalogue.'

import csv
import fileinput
import os.path
import pprint

import cdli


def print_composites(data_path):
    'Dump each row in the catalogue for debugging.'
    rows = 0
    composites = 0
    for row in cdli.read_catalogue(data_path):
        rows += 1
        if row['composite'] and row['composite'] != 'needed':
            composites += 1
            print(cdli.id_from_row(row), '>>', row['composite'])
    print(rows, 'rows')
    print(composites, 'with composite entries')


def check_values(data_path):
    'Check for problems in the entry data.'
    for row in cdli.read_catalogue(data_path):
        id = cdli.id_from_row(row)
        for key, value in row.items():
            if not isinstance(value, str):
                print(id, 'skipping non-string value for column', key)
                continue
            if value.isspace():
                print(id, key, 'is whitespace-only.')
            if not value.isprintable():
                print(id, key, 'contains non-printable characters.')


def check_columns(data_path):
    reader = cdli._catalogue_reader(data_path)
    count = len(reader.fieldnames)
    print(f'{count} columns in header')
    for row in reader:
        extras = [column for column in row.keys()
                  if column not in reader.fieldnames]
        for extra in extras:
            print('Warning: Extra column not in the header'
                  f' on row {reader.line_num}!'
                  f'\n\t{extra}: {row[extra]}')


def check_empties(data_path):
    reader = cdli._catalogue_reader(data_path)
    empties = set(reader.fieldnames)
    for row in reader:
        has_content = [key for key in row.keys() if row[key]]
        for column in has_content:
            empties.discard(column)
    if empties:
        print(f'{len(empties)} unused columns:')
        pprint.pprint(empties)
    else:
        print(f'All {len(reader.fieldnames)} columns are used.')


if __name__ == '__main__':
    import sys

    data_path = '../cdli-data'
    if len(sys.argv) > 1:
        data_path = sys.argv[1]

    print('Checking catalogue data in', data_path)

    check_columns(data_path)
    check_empties(data_path)
    check_values(data_path)
