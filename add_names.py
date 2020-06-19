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

def write_output(names, cfd, rfd, fout):

    # read community detection result
    cdf = None
    if os.path.isdir(cfd):
        cdf = pd.read_csv(cfd + "/part-00000", sep='\t', header=None, names=["id", "Community"])
        # print(cdf.head())

    # read ranking result
    rdf = None
    if os.path.isdir(rfd):
        rdf = pd.read_csv(rfd + "/part-00000", sep='\t', header=None, names=["id", "Ranking Score"])
        # print(rdf.head())

    # combine ranking and community detection results
    if cdf is not None and rdf is not None:
        result = cdf.merge(rdf, on="id", how='inner')
        result.sort_values(by=["Community", "Ranking Score"], ascending=[True, False], inplace=True)

    # only community detection results, sort appropriately
    elif cdf is not None:
        result = cdf.sort_values(by=["Community"])
    
    # ranking result is already sorted
    elif rdf is not None:
        result = rdf
    
    result = result.merge(names, on="id", how='inner')
    result.to_csv(fout, index = False, sep='\t')


with open(sys.argv[2]) as config_file:
    config = json.load(config_file)
    
    entity_file = config["indir"] + config["query"]["metapath"][:1] + ".csv"

    names = parse_entities(entity_file, config["select_field"])
    write_output(names, config["communities_out"], config["ranking_out"], config["final_out"])
