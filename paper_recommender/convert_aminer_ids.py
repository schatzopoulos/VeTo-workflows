import csv
import argparse

from pathlib import Path

AMINER_ID_FILEPATH = Path(__file__).parent.resolve() / 'data' / 'aminer_ids.csv'


def _parse_user_args():
    """
    Parses the user arguments

    :returns: user arguments
    """
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('-it', '--id_type', choices=['veto', 'aminer'], default='aminer',
                            help='Convert the ids given their type. Can be "aminer" or "veto"')
    arg_parser.add_argument('-p', '--path', required=True, help='path to the newline separated file containing the ids')
    arg_parser.add_argument('-ap', '--aminer_path', required=True, help='path to the veto to aminer ids file',
                            default=AMINER_ID_FILEPATH)
    return arg_parser.parse_args()


def to_aminer(id_file, id_type='veto', aminer_path=AMINER_ID_FILEPATH):
    ids = []
    with open(id_file, newline='') as outf:
        reader = csv.reader(outf, delimiter='\n')
        for line in reader:
            ids.append(line[0].split(' ')[0])

    id_set = set(ids)

    search_key = 'id' if id_type == 'veto' else 'aminer_id'
    result_key = 'aminer_id' if id_type == 'veto' else 'id'

    with open(aminer_path, newline='') as csvfile:
        reader = csv.DictReader(csvfile, delimiter='\t')
        res = {}
        for row in reader:
            if (row_id := row[search_key]) in id_set:
                res[row_id] = row[result_key]
    for _id in ids:
        print(res[_id])
    for _id in ids:
        print(f'"{res[_id]}",')


if __name__ == '__main__':
    args = _parse_user_args()
    to_aminer(args.path, args.id_type, args.aminer_path)
