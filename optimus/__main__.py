docstr = """
Optimus

Usage: optimus.py [-h] (<file> <config>) [-o <output.json>]

Options:
  -h --help                                     show this message and exit
  -o <output.json> --output=<output.json>       optional output file for results

"""
import csv

from docopt import docopt
import dateutil.parser as date_parser
import yaml

from StagingArea import StagingArea

_file = '<file>'
_config = '<config>'
_output = '--output'

# config magic strings
_subj = 'subject' # this refers to the column that holds the subject id
_ev = 'event' # this refers to the column that holds the event decider
_id = 'identifier' # identifies the row as the results of a particular test
_delim = 'delimiter' # how the csv is delimited
_qc = 'quotechar' # how things are quoted
_map = 'mappings'

# config mapping magic strings
_rk = 'row_key'
_hdr = 'headers'
_hk = 'header_key'
_fld = 'field'
_v = 'value'
_t = 'transform'

transforms = {
    'format_date': lambda d : date_parser.parse(d).strftime('%Y-%m-%d')
}

def main(args=docopt(docstr)):
    with open(args[_config], 'r') as config_file:
        global config
        config = yaml.load(config_file.read())

    csv_file = open(args[_file], 'r')
    data = csv.DictReader(csv_file, delimiter=config[_delim], quotechar=config[_qc])

    autobots = StagingArea()

    for row in data:
        subject_key = row[config[_subj]]
        event_key = row[config[_ev]]
        mapping = get_mapping_for_row(row)
        if mapping:
            mapped_data = use_mapping(row, mapping)
            autobots.add_data(subject_key, event_key, **mapped_data)

    csv_file.close()

    if args.get(_output):
        with open(args[_output], 'w') as outfile:
            outfile.write(autobots.transform_and_roll_out())
    else:
        print(autobots.transform_and_roll_out())

def get_mapping_for_row(row):
    row_key = row[config[_id]]
    for mapping in config[_map]:
        if mapping[_rk] == row_key:
            return mapping

def use_mapping(row, mapping):
    data = {}
    for col in mapping[_hdr]:
        if col.get(_v):
            data[col[_fld]] = col[_v]
        elif col.get(_t):
            data[col[_fld]] = transforms[col[_t]](row[col[_hk]])
        else:
            data[col[_fld]] = row[col[_hk]]
    return data

if __name__ == '__main__':
    args = docopt(docstr)
    main(args)
    exit()

