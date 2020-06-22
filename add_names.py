import sys
import csv
import json
import os  
import pandas as pd 
 
def parse_entities(entity_file, select_field):
    with open(entity_file) as fp:
    
        # read header file and determine selected column
        line = fp.readline()

        fields = line.rstrip().split('\t')
        for i in range(len(fields)):

            if select_field in fields[i]: 
                break

        select_field_idx = i

        names_df = pd.read_csv(entity_file, sep='\t', usecols=["id", select_field])

    return names_df

def write_output(names, analysis, fin, fout):

    # read community detection result
    if analysis == "Ranking":
        result = pd.read_csv(fin + "/part-00000", sep='\t', header=None, names=["id", "Ranking Score"])

    elif analysis == "Community Detection":
        df = pd.read_csv(fin + "/part-00000", sep='\t', header=None, names=["id", "Community"])
        result = df.sort_values(by=["Community"])


    # combine ranking and community detection results
    # if cdf is not None and rdf is not None:
    #     result = cdf.merge(rdf, on="id", how='inner')
    #     result.sort_values(by=["Community", "Ranking Score"], ascending=[True, False], inplace=True)

    
    result = result.merge(names, on="id", how='inner')
    del result['id']
    result.rename(columns={'name': 'Entity'}, inplace=True)
    cols = result.columns.tolist()
    cols = cols[-1:] + cols[:-1]        # move name first

    result[cols].to_csv(fout, index = False, sep='\t')


with open(sys.argv[2]) as config_file:
    analysis = sys.argv[3]
    fin = sys.argv[4]
    fout = sys.argv[5]
    config = json.load(config_file)
    
    entity_file = config["indir"] + config["query"]["metapath"][:1] + ".csv"

    names = parse_entities(entity_file, config["select_field"])
    write_output(names, analysis, fin, fout)
    print(analysis + "\t3\tCompleted")
