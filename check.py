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
            if value.isspace():
                print(id, key, 'is whitespace-only.')
            if not value.isprintable():
                print(id, key, 'contains non-printable characters.')


def check_columns(filenames):
    with fileinput.input(files=filenames, openhook=cdli.as_utf8) as csvfile:
        reader = csv.DictReader(csvfile)
        count = len(reader.fieldnames)
        print(f'{count} columns in header')
        for row in reader:
            for column in row.keys():
                if column not in reader.fieldnames:
                    print(f'Warning: Extra column not in the header on row {reader.line_num}!\n\t{column}: {row[column]}')


def check_empties(filenames):
    with fileinput.input(files=filenames, openhook=cdli.as_utf8) as csvfile:
        reader = csv.DictReader(csvfile)
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

    filenames = [os.path.join(data_path, fn) for fn in cdli.files]
    check_columns(filenames)
    check_empties(filenames)

    check_values(data_path)
