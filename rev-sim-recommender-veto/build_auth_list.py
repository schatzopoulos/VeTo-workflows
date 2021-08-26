import sys
import csv
import os

# Define CSV dialect to be used.
csv.register_dialect(
    'exp_dialect',
    delimiter = '\t'
)

basic_path = sys.argv[1] #path to the main folder of current dataset
auth_sim_dirs = os.listdir("./data/TPDL/input/author_sim/") #get author similiarity folders 

#get the intersection of all authors that are valid for all similarity folders
#print("Distinct authors for each similarity method: ")
sim_methods = 0
dist_auth_files = []
for sim_dir in auth_sim_dirs:
    if sim_dir[0]!= ".": #do not consider any OS hidden files
        sim_methods += 1
        files = os.listdir("./data/TPDL/input/author_sim/"+sim_dir)
        #print("\t-"+sim_dir+":\t"+str(len(files)))
        if sim_methods == 1:
            dist_auth_files = files
        else:
            dist_auth_files = set(files).intersection(dist_auth_files)

#print("\t-Intersection:\t"+str(len(dist_auth_files)))
#print(dist_auth_files)

#create the corresponding file of author ids 
try: 
    with open(basic_path+'/input/authors/authors_cleansed.csv', 'r') as auth_ids_file:
        authors = csv.reader(auth_ids_file,dialect='exp_dialect')

        for auth in authors:
            #print (auth)
            if auth[0] + '.csv' in dist_auth_files:
                print(auth[0])
    auth_ids_file.close()

except IOError as e:
    print("=> ERROR: Cannot open file...")
    print(e)
