import csv
import sys
from pprint import pprint


def to_aminer(out_file):
    ids = set()
    with open(out_file, newline='') as outf:
        reader = csv.reader(outf, delimiter='\n')
        for line in reader:
            ids.add(line[0].split(' ')[0])
    with open('data/aminer_ids.csv', newline='') as csvfile:
        reader = csv.DictReader(csvfile, delimiter='\t')
        res = {}
        for row in reader:
            if (row_id := row['id']) in ids:
                res[row_id] = row['aminer_id']
        pprint(res)


if __name__ == '__main__':
    to_aminer(sys.argv[1])
