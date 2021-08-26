import sys
import csv
import os 
from operator import itemgetter

# Define CSV dialect to be used.
csv.register_dialect(
    'exp_dialect',
    delimiter = '\t'
)

# Read command-line input.
expert_file = sys.argv[1] 
output_file = sys.argv[2]
apv_dir = sys.argv[3]
apt_dir = sys.argv[4]
topk_thr = int(sys.argv[5]) #how many of the topk most similar authors will be considered
alpha = float(sys.argv[6])
beta = float(sys.argv[7])
rrf_k = int(sys.argv[8])
method = sys.argv[9]
output_size = int(sys.argv[10])

def score(coeff, method, rrf_k, topk_thr, lines_to_read, sim_score):
    if method == 'borda':
        return coeff * lines_to_read
    elif method == 'rrf':
        return  coeff * (1.0 /  (rrf_k + (topk_thr - lines_to_read)))
    elif method == 'sum':
        return  float(sim_score)

test_size_strict = 0
try:
        # Create a dictionary with all items in train set
        train_set = dict() 
        with open(expert_file, 'r') as train_file:
            train_entries = csv.reader(train_file,dialect='exp_dialect')
            for entry in train_entries: 
                train_set[entry[0]] = 1
        train_file.close()

        # Get suggestions based on HIN
        hin_sugg = dict() 
        for entry in train_set:
            try:
                with open(apt_dir + "/" + entry, 'r') as auth_sim1_file:
                    sim1_authors = csv.reader(auth_sim1_file,dialect='exp_dialect')

                    lines_to_read = topk_thr
                    for auth in sim1_authors:
                        if auth[1] in train_set: #do not consider anyone in the training set
                            continue
                        lines_to_read -= 1
                        if lines_to_read == -1:
                            break
                        if auth[1] in hin_sugg:
                            hin_sugg[auth[1]]['apt'] += score(alpha, method, rrf_k, topk_thr, lines_to_read, auth[2])
                        else:
                            hin_sugg[auth[1]] = dict()
                            hin_sugg[auth[1]]['apv'] = 0
                            hin_sugg[auth[1]]['apt'] = score(alpha, method, rrf_k, topk_thr, lines_to_read, auth[2])

                    auth_sim1_file.close()
            except FileNotFoundError:
                pass

            try:
                with open(apv_dir + "/" + entry, 'r') as auth_sim2_file:
                    sim2_authors = csv.reader(auth_sim2_file,dialect='exp_dialect')
                    lines_to_read = topk_thr

                    for auth in sim2_authors:
                        if auth[1] in train_set: #do not consider anyone in the training set
                            continue
                        lines_to_read -= 1
                        if lines_to_read == -1:
                            break
                        if auth[1] in hin_sugg:
                            hin_sugg[auth[1]]['apv'] += score(beta, method, rrf_k, topk_thr, lines_to_read, auth[2])
                        else:
                            hin_sugg[auth[1]] = dict() 
                            hin_sugg[auth[1]]['apv'] = score(beta, method, rrf_k, topk_thr, lines_to_read, auth[2])
                            hin_sugg[auth[1]]['apt'] = 0

                    auth_sim2_file.close()
            except FileNotFoundError:
                pass
        
        for sugg in hin_sugg:
            hin_sugg[sugg]['overall'] = hin_sugg[sugg]['apt'] + hin_sugg[sugg]['apv']

        hin_sugg_list = sorted(hin_sugg,key=lambda k: hin_sugg[k]['overall'], reverse=True) #sort suggestions based on borda count
        hin_sugg_list = hin_sugg_list[0:output_size] #keep as many as in the test size

        with open(output_file, 'w') as hin_sugg_file:
            hin_sugg_writer = csv.writer(hin_sugg_file,dialect='exp_dialect')
            for sugg in hin_sugg_list:
                hin_sugg_writer.writerow([sugg, hin_sugg[sugg]['overall'], hin_sugg[sugg]['apt'], hin_sugg[sugg]['apv']])
        hin_sugg_file.close()

except IOError as e:
    print("=> ERROR: Cannot open file...")
    print(e)
