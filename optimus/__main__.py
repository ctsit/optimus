docstr = """
Optimus

Usage: optimus.py [-hd] (<file> <config>) [-o <output.json>]

Options:
  -h --help                                     show this message and exit
  -d --debug                                    json is indented for debugging
  -o <output.json> --output=<output.json>       optional output file for results

"""
import csv
import json

from docopt import docopt
import dateutil.parser as date_parser
import yaml

import optimus.project_specific as ps

_file = '<file>'
_config = '<config>'
_output = '--output'

# config magic strings
_delim = 'delimiter' # how the csv is delimited
_qc = 'quotechar' # how things are quoted

def main(args):
    with open(args[_config], 'r') as config_file:
        global config
        config = yaml.load(config_file.read())

    csv_file = open(args[_file], 'r')
    data = csv.DictReader(csv_file, delimiter=config[_delim], quotechar=config[_qc])

    csv_output_data = []
    for row in data:
        row_data = get_row_data(row)
        for item in row_data:
            csv_output_data.append(item)

    project_pipeline = getattr(ps, config['project']).pipeline
    transformed = project_pipeline(config, csv_output_data)

    csv_file.close()

    if args.get(_output):
        with open(args[_output], 'w') as outfile:
            if args.get('--debug'):
                outfile.write(json.dumps(transformed, indent=4))
            else:
                outfile.write(json.dumps(transformed))
    else:
        if args.get('--debug'):
            print(json.dumps(transformed, indent=4))
        else:
            print(json.dumps(transformed))

def get_row_data(row):
    data_for_row = []
    for row_map in config['rows']:
        if row[config['key_header']] == row_map['row_key']:
            outputs = row_map['outputs']
            for out in outputs:
                datum = {
                    'datum': row[out['datum']],
                    'field': out['field'],
                    'date': row[out['date']],
                    'subj': row[out['subj']],
                }
                data_for_row.append(datum)
    return data_for_row or []

def cli_run():
    args = docopt(docstr)
    main(args)

if __name__ == '__main__':
    cli_run()
    exit()

