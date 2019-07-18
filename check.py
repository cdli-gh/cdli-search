#!/usr/bin/env python3

'Print info from the CDLI catalogue.'

import csv
import fileinput
import os.path
import pprint

import upload


def print_composites(filenames):
    'Dump each row in the catalogue for debugging.'
    rows = 0
    composites = 0
    for row in upload.read_catalogue(filenames):
        rows += 1
        if row['composite'] and row['composite'] != 'needed':
            composites += 1
            print('P' + row['id_text'], '>>', row['composite'])
    print(rows, 'rows')
    print(composites, 'with composite entries')


def check_columns(filenames):
    with fileinput.input(files=filenames, openhook=upload.as_utf8) as csvfile:
        reader = csv.DictReader(csvfile)
        count = len(reader.fieldnames)
        print(f'{count} columns in header')
        for row in reader:
            for column in row.keys():
                if not column in reader.fieldnames:
                    print(f'Warning: Extra column not in the header on row {reader.line_num}!\n\t{column}: {row[column]}')


def check_empties(filenames):
    with fileinput.input(files=filenames, openhook=upload.as_utf8) as csvfile:
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
    filenames = [os.path.join('../cdli-data', fn) for fn in upload.files]
    check_columns(filenames)
    check_empties(filenames)
