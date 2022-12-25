from random import sample

s = '''
2690053
1190654
640491
1700030
716666
867672
1190963
1390347
1764389
2356344
90411
2314563
2504035
'''

l = {item for item in s.strip().split('\n')}
rand_selected = set(sample(list(l), len(l) // 3))
print('\nRandom Selection\n')
for item in rand_selected:
    print(item)
nl = l.difference(rand_selected)
print('\nNew Ids\n')
for item in nl:
    print(item)
