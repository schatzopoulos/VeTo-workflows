import csv

csv.register_dialect(
    'foo',
    delimiter='\t'
)

s = '''
1801747
2165377
1978826
1919624
2327043
924122
1919630
1906896
1847858
1819310
407254
603867
2618702
535920
1805455
316188
3058607
68488
3046963
1341287
'''


def foo():
    columns = ['abstract', 'aminer_id', 'id', 'title', 'year']
    l = [item for item in s.strip().split('\n')]
    print(l)
    res = {}
    with open('data/input_sort.csv', newline='') as outf:
        reader = csv.DictReader(outf, fieldnames=columns, delimiter='\t')
        for line in reader:
            k = line['id']
            res[k] = {'abstract': line['abstract'], 'aminer_id': line['aminer_id'], 'id': line['id'],
                      'title': line['title'], 'year': line['year']}

    with open('data/output_sort.csv', 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=columns, dialect='foo')
        for item in l:
            writer.writerow(res[item])


if __name__ == '__main__':
    foo()
