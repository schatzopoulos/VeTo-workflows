import sys
import csv
import pandas as pd 

names = pd.read_csv(sys.argv[1], sep='\t', usecols=["id", "name"])
result = pd.read_csv(sys.argv[2], sep='\t', header=None, names=["id", "Score", 'apt', 'apv'])

result['apt'] /= result['Score']
result['apv'] /= result['Score']

max_score = result["Score"].max()
result["Score"] /= max_score

result = result.merge(names, on="id", how='inner')
del result['id']

cols = result.columns.tolist()

cols = cols[-1:] + cols[:-1]

result[cols].to_csv(sys.argv[3], index = False, sep='\t')