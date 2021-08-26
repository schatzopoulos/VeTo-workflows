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

experiment_path = basic_path+"experiments/"+str(kfold)+"-fold/"
suggestion_path = experiment_path+"suggestions/"

try:
    bl_sum = 0
    adt_sum = 0
    wg_sum = 0
    doc_sum = 0
    hin_apt_sum = 0
    hin_apv_sum = 0
    hin_sum = 0

    for ifold in range(0,int(kfold)):
        # Read suggestions for the given fold from the corresponding folders. 
        print("\n=== FOLD "+str(ifold)+" ===")
        if ifold == (kfold-1):
            fold_path = experiment_path+"folds/fold"+str(ifold)+"+/" 
        else:
            fold_path = experiment_path+"folds/fold"+str(ifold)+"/" 

        # Create a list with all items in the test set
        test_set = dict()
        with open(fold_path+"test.csv",'r') as test_file:
            test_entries_csv = csv.reader(test_file,dialect='pc_dialect')
            for entry in test_entries_csv:
                test_set[entry[0]] = 1
        test_file.close()

        if ifold == (kfold-1):
            sugg_path = suggestion_path+"fold"+str(ifold)+"+/" 
        else:
            sugg_path = suggestion_path+"fold"+str(ifold)+"/" 

        bl_suggs = dict()
        with open(sugg_path+"bl.csv",'r') as bl_sugg_file:
            bl_sugg_reader = csv.reader(bl_sugg_file,dialect='pc_dialect')
            for sugg in bl_sugg_reader:
                bl_suggs[sugg[0]] = 1
            print("\n--- BASELINE: ---")
            true_positives = []
            false_positives = []
            false_negatives = []
            rank = 1
            for sugg in bl_suggs:
                
                if sugg in test_set:
                    print(rank)
                    bl_sum += 1 / rank
                    break
                rank += 1
            
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
            rank = 1
            for sugg in adt_suggs:
                if sugg in test_set:
                    print(rank)
                    adt_sum += 1 / rank
                    break
                rank += 1
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
            rank = 1
            for sugg in wg_suggs:
                if sugg in test_set:
                    print(rank)
                    wg_sum += 1 / rank
                    break
                rank += 1
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
            rank = 1
            for sugg in doc_suggs:
                if sugg in test_set:
                    print(rank)
                    doc_sum += 1 / rank
                    break
                rank += 1
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
            rank = 1
            for sugg in hin_apv_suggs:
                if sugg in test_set:
                    print(rank)
                    hin_apv_sum += 1 / rank
                    break
                rank += 1
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
            rank = 1
            for sugg in hin_apt_suggs:
                if sugg in test_set:
                    print(rank)
                    hin_apt_sum += 1 / rank
                    break
                rank += 1
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
            rank = 1
            for sugg in hin_suggs:
                if sugg in test_set:
                    print(rank)
                    hin_sum += 1 / rank
                    break
                rank += 1
        hin_sugg_file.close()
    print ("##### MRR SCORES #####")
    print("Baseline: " + str(1 / int(kfold) *  bl_sum))
    print("ΑDT: " + str(1 / int(kfold) * adt_sum))
    print("WG: " + str(1 / int(kfold) *  wg_sum))
    print("DOC: " + str(1 / int(kfold) *  doc_sum))
    print("HIN-APT: " + str(1 / int(kfold) *  hin_apt_sum))
    print("HIN-APV: " + str(1 / int(kfold) *  hin_apv_sum))
    print("HIN: " + str(1 / int(kfold) * hin_sum))
except IOError as e:
    print("=> ERROR: Cannot open file...")
    print(e)