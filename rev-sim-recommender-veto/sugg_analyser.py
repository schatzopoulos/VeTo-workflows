import sys
import csv
import math
import os 
import random 
import datetime

# Define CSV dialect to be used.
csv.register_dialect(
    'pc_dialect',
    delimiter = '\t'
)

# Read command-line input.
basic_path = sys.argv[1] #path to the main folder of current dataset
kfold = int(sys.argv[2]) #how many different train/test sets will be used
fold = int(sys.argv[3]) #the fold for which I want to see the results

experiment_path = basic_path+"experiments/"+str(kfold)+"-fold/"
suggestion_path = experiment_path+"suggestions/"

try:
    # Make a dictionary of all DBLP authors.
    all_authors = dict() 
    with open("data/all_authors_dblp.csv",'r') as authors_file:
        authors_data = csv.reader(authors_file,dialect='pc_dialect')
        for line in authors_data:
            all_authors[line[0]] = line[1]

    # Read suggestions for the given fold from the corresponding folders. 
    print("\n=== FOLD "+str(fold)+" ===")
    if fold == (kfold-1):
        fold_path = experiment_path+"folds/fold"+str(fold)+"+/" 
    else:
        fold_path = experiment_path+"folds/fold"+str(fold)+"/" 

    # Create a list with all items in the test set
    test_set = dict()
    with open(fold_path+"test.csv",'r') as test_file:
        test_entries_csv = csv.reader(test_file,dialect='pc_dialect')
        for entry in test_entries_csv:
            test_set[entry[0]] = 1
    test_file.close()

    if fold == (kfold-1):
        sugg_path = suggestion_path+"fold"+str(fold)+"+/" 
    else:
        sugg_path = suggestion_path+"fold"+str(fold)+"/" 

    bl_suggs = dict()
    with open(sugg_path+"bl.csv",'r') as bl_sugg_file:
        bl_sugg_reader = csv.reader(bl_sugg_file,dialect='pc_dialect')
        for sugg in bl_sugg_reader:
            bl_suggs[sugg[0]] = 1
        print("\n--- BASELINE: ---")
        true_positives = []
        false_positives = []
        false_negatives = []
        for sugg in bl_suggs:
            print(all_authors[sugg])
            if sugg in test_set:
                true_positives.append(all_authors[sugg])
            else:
                false_positives.append(all_authors[sugg])
        for test in test_set:
            if test not in bl_suggs:
                false_negatives.append(all_authors[test])
        print("- True positives:")
        print(true_positives)
        print("- False positives:")
        print(false_positives)
        print("- False negatives:")
        print(false_negatives)
    bl_sugg_file.close()

    adt_suggs = dict()
    with open(sugg_path+"adt.csv",'r') as adt_sugg_file:
        adt_sugg_reader = csv.reader(adt_sugg_file,dialect='pc_dialect')
        for sugg in adt_sugg_reader:
            adt_suggs[sugg[0]] = 1
        print("\n--- ADT: ---")
        true_positives = []
        false_positives = []
        false_negatives = []
        for sugg in adt_suggs:
            print(all_authors[sugg])
            if sugg in test_set:
                true_positives.append(all_authors[sugg])
            else:
                false_positives.append(all_authors[sugg])
        for test in test_set:
            if test not in adt_suggs:
                false_negatives.append(all_authors[test])
        print("- True positives:")
        print(true_positives)
        print("- False positives:")
        print(false_positives)
        print("- False negatives:")
        print(false_negatives)
    adt_sugg_file.close()

    wg_suggs = dict()
    with open(sugg_path+"wg.csv",'r') as wg_sugg_file:
        wg_sugg_reader = csv.reader(wg_sugg_file,dialect='pc_dialect')
        for sugg in wg_sugg_reader:
            wg_suggs[sugg[0]] = 1
        print("\n--- WG: ---")
        true_positives = []
        false_positives = []
        false_negatives = []
        for sugg in wg_suggs:
            print(all_authors[sugg])
            if sugg in test_set:
                true_positives.append(all_authors[sugg])
            else:
                false_positives.append(all_authors[sugg])
        for test in test_set:
            if test not in wg_suggs:
                false_negatives.append(all_authors[test])
        print("- True positives:")
        print(true_positives)
        print("- False positives:")
        print(false_positives)
        print("- False negatives:")
        print(false_negatives)
    wg_sugg_file.close()

    doc_suggs = dict()
    with open(sugg_path+"doc.csv",'r') as doc_sugg_file:
        doc_sugg_reader = csv.reader(doc_sugg_file,dialect='pc_dialect')
        for sugg in doc_sugg_reader:
            doc_suggs[sugg[0]] = 1
        print("\n--- DOC: ---")
        true_positives = []
        false_positives = []
        false_negatives = []
        for sugg in doc_suggs:
            print(all_authors[sugg])
            if sugg in test_set:
                true_positives.append(all_authors[sugg])
            else:
                false_positives.append(all_authors[sugg])
        for test in test_set:
            if test not in doc_suggs:
                false_negatives.append(all_authors[test])
        print("- True positives:")
        print(true_positives)
        print("- False positives:")
        print(false_positives)
        print("- False negatives:")
        print(false_negatives)
    doc_sugg_file.close()

    hin_apv_suggs = dict()
    with open(sugg_path+"hin_apv.csv",'r') as hin_apv_sugg_file:
        hin_apv_sugg_reader = csv.reader(hin_apv_sugg_file,dialect='pc_dialect')
        for sugg in hin_apv_sugg_reader:
            hin_apv_suggs[sugg[0]] = 1
        print("\n--- HIN-APV: ---")
        true_positives = []
        false_positives = []
        false_negatives = []
        for sugg in hin_apv_suggs:
            print(all_authors[sugg])
            if sugg in test_set:
                true_positives.append(all_authors[sugg])
            else:
                false_positives.append(all_authors[sugg])
        for test in test_set:
            if test not in hin_apv_suggs:
                false_negatives.append(all_authors[test])
        print("- True positives:")
        print(true_positives)
        print("- False positives:")
        print(false_positives)
        print("- False negatives:")
        print(false_negatives)
    hin_apv_sugg_file.close()

    hin_apt_suggs = dict()
    with open(sugg_path+"hin_apt.csv",'r') as hin_apt_sugg_file:
        hin_apt_sugg_reader = csv.reader(hin_apt_sugg_file,dialect='pc_dialect')
        for sugg in hin_apt_sugg_reader:
            hin_apt_suggs[sugg[0]] = 1
        print("\n--- HIN-APT: ---")
        true_positives = []
        false_positives = []
        false_negatives = []
        for sugg in hin_apt_suggs:
            print(all_authors[sugg])
            if sugg in test_set:
                true_positives.append(all_authors[sugg])
            else:
                false_positives.append(all_authors[sugg])
        for test in test_set:
            if test not in hin_apt_suggs:
                false_negatives.append(all_authors[test])
        print("- True positives:")
        print(true_positives)
        print("- False positives:")
        print(false_positives)
        print("- False negatives:")
        print(false_negatives)
    hin_apt_sugg_file.close()

    hin_suggs = dict()
    with open(sugg_path+"hin.csv",'r') as hin_sugg_file:
        hin_sugg_reader = csv.reader(hin_sugg_file,dialect='pc_dialect')
        for sugg in hin_sugg_reader:
            hin_suggs[sugg[0]] = 1
        print("\n--- HIN: ---")
        true_positives = []
        false_positives = []
        false_negatives = []
        for sugg in hin_suggs:
            print(all_authors[sugg])
            if sugg in test_set:
                true_positives.append(all_authors[sugg])
            else:
                false_positives.append(all_authors[sugg])
        for test in test_set:
            if test not in hin_suggs:
                false_negatives.append(all_authors[test])
        print("- True positives:")
        print(true_positives)
        print("- False positives:")
        print(false_positives)
        print("- False negatives:")
        print(false_negatives)
    hin_sugg_file.close()

except IOError as e:
    print("=> ERROR: Cannot open file...")
    print(e)