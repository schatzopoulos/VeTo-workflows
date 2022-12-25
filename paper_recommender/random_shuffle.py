import csv
import random

csv.register_dialect(
    'foo',
    delimiter='\t'
)


def foo():
    columns = ['abstract', 'aminer_id', 'id', 'title', 'year']
    res = []
    with open('data/random_shuffle_in.csv', newline='') as outf:
        reader = csv.DictReader(outf, fieldnames=columns, delimiter='\t')
        for line in reader:
            res.append({'abstract': line['abstract'], 'aminer_id': line['aminer_id'], 'id': line['id'],
                        'title': line['title'], 'year': line['year']})
    for _ in range(3):
        random.shuffle(res)
    with open('data/random_shuffle_out.csv', 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=columns, dialect='foo')
        for item in res:
            writer.writerow(item)


if __name__ == '__main__':
    foo()
