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

researchers = []
researchers_num = 0
try:
    # Read PC-members CSV file.
    with open(basic_path+"input/authors/valid-ids.csv",'r') as pc_file:
        researchers_data = csv.reader(pc_file,dialect='pc_dialect')
        for line in researchers_data:
            researchers.append(line[0])
    pc_file.close()
    researchers_num = len(researchers)
    print("- Found "+str(researchers_num)+" researchers in file...")

    # Create k folds
    fold_size = math.floor(researchers_num/kfold)
    print("- Creating "+str(kfold)+" folds (each of size "+str(fold_size)+")")
    train_set_size = researchers_num - fold_size 

    # Build test/train sets. 
    print("- Building "+str(kfold)+" train/test sets (train: "+str(train_set_size)+", test: "+str(fold_size)+")")  
    # Shuffle the list of researchers.
    random.shuffle(researchers)
    # Create the folds for each experiment.
    for exp in range(0,kfold):
        fold_dir = basic_path+"experiments/"+str(kfold)+"-fold/folds/"
        print("\t- Building train/test set #"+str(exp))
        # Create the corresponding folder (if not there).
        if exp == (kfold-1):
            sets_dir = fold_dir+"fold"+str(exp)+"+"
        else:
            sets_dir = fold_dir+"fold"+str(exp)
        if not os.path.exists(sets_dir):
            os.makedirs(sets_dir)
        train_file = open(sets_dir+"/train.csv",'w')
        test_file = open(sets_dir+"/test.csv",'w')
        # Create the test set.
        if exp == (kfold-1): 
            cur_test = researchers[(exp*fold_size):]
        else:
            cur_test = researchers[(exp*fold_size):((exp+1)*fold_size)]
        for res in cur_test:
            test_file.write(res+"\n")
        # Create the train set.
        train_set = set(researchers).difference(cur_test)
        for item in train_set:
            train_file.write(item+"\n")
        train_file.close()   
        test_file.close()   

except IOError as e:
    print("=> ERROR: Cannot open file...")
    print(e)