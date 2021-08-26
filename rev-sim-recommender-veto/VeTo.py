import sys
import csv
import os 

# Define CSV dialect to be used.
csv.register_dialect(
    'exp_dialect',
    delimiter = '\t'
)

class VeTo:

    def score(self, coeff, method, rrf_k, topk_thr, lines_to_read, sim_score):
        if method == 'borda':
            return coeff * lines_to_read
        elif method == 'rrf':
            return  coeff * (1.0 /  (rrf_k + (topk_thr - lines_to_read)))
        elif method == 'sum':
            return  coeff * float(sim_score)

    def run(self, method, basic_path, kfold, topk_thr, alpha, beta, rrf_k):

        experiment_path = basic_path+"experiments/"+str(kfold)+"-fold/"

        suggestion_path = experiment_path+"suggestions/"

        hin_wp_avg_precision = []
        hin_wp_avg_recall = []

        test_size_strict = 0
        try:
            # Run once for each pair of train/test sets.
            for fold in range(0,kfold):
                if fold == (kfold-1):
                    fold_path = experiment_path+"folds/fold"+str(fold)+"+/" 
                else:
                    fold_path = experiment_path+"folds/fold"+str(fold)+"/" 

                # Create a dictionary with all items in train set
                train_set = dict() 
                with open(fold_path+"train.csv",'r') as train_file:
                    train_entries = csv.reader(train_file,dialect='exp_dialect')
                    for entry in train_entries: 
                        train_set[entry[0]] = 1
                train_file.close()

                # Create a list with all items in the test set
                test_set = []
                with open(fold_path+"test.csv",'r') as test_file:
                    test_entries_csv = csv.reader(test_file,dialect='exp_dialect')
                    test_size = 0
                    for entry in test_entries_csv:
                        test_set.append(entry[0])
                        test_size += 1
                test_file.close()
                if fold == 0:
                    test_size_strict = test_size #to be used in case the size of last partition is larger
                    hin_wp_avg_precision = [0]*test_size_strict
                    hin_wp_avg_recall = [0]*test_size_strict

                # Get suggestions based on HIN
                hin_wp_sugg = dict() 
                for entry in train_set:
                    try:
                        with open(basic_path+"input/author_sim/HIN-APT/"+entry+".csv",'r') as auth_sim1_file:
                            sim1_authors = csv.reader(auth_sim1_file,dialect='exp_dialect')

                            lines_to_read = topk_thr
                            for auth in sim1_authors:
                                if auth[1] in train_set: #do not consider anyone in the training set
                                    continue
                                lines_to_read -= 1

                                if lines_to_read == -1:
                                    break
                                if auth[1] in hin_wp_sugg:
                                    hin_wp_sugg[auth[1]] +=  self.score(alpha, method, rrf_k, topk_thr, lines_to_read, auth[2]) #pow(lines_to_read,3) #* float(auth[2]) #get borda-count score 
                                else:
                                    hin_wp_sugg[auth[1]] =   self.score(alpha, method, rrf_k, topk_thr, lines_to_read, auth[2]) # #* float(auth[2])  #get borda-count score

                        auth_sim1_file.close()
                    except FileNotFoundError:
#                        print("NOT FOUND: " + basic_path+"input/author_sim/HIN-APT/"+entry+".csv")
                        pass

                    try:
                        with open(basic_path+"input/author_sim/HIN-APV/"+entry+".csv",'r') as auth_sim2_file:
                            sim2_authors = csv.reader(auth_sim2_file,dialect='exp_dialect')
                            lines_to_read = topk_thr
                            for auth in sim2_authors:
                                if auth[1] in train_set: #do not consider anyone in the training set
                                    continue
                                lines_to_read -= 1
                                if lines_to_read == -1:
                                    break
                                if auth[1] in hin_wp_sugg:
                                    hin_wp_sugg[auth[1]] +=   self.score(beta, method, rrf_k, topk_thr, lines_to_read, auth[2]) #pow(lines_to_read,3) #* float(auth[2]) #get borda-count score 
                                else:
                                    hin_wp_sugg[auth[1]] =   self.score(beta, method, rrf_k, topk_thr, lines_to_read, auth[2]) #pow(lines_to_read,3) #* float(auth[2])  #get borda-count score

                        auth_sim2_file.close()
                    except FileNotFoundError:
#                        print("NOT FOUND: " + basic_path+"input/author_sim/HIN-APV/"+entry+".csv")
                        pass

                hin_wp_sugg_list = sorted(hin_wp_sugg,key=hin_wp_sugg.get, reverse=True) #sort suggestions based on borda count
                hin_wp_sugg_list = hin_wp_sugg_list[0:test_size] #keep as many as in the test size

                # Calculate top-k precision & recall for different k values
                for k in range(1,test_size_strict):
                    #print("- Calculating precision & recall for fold #"+str(fold)+" at top-"+str(k)+":") #debug
                    #consider first k elements for each method
                    hin_wp_sugg_list_topk = hin_wp_sugg_list[0:k]

                    hin_wp_found = set(test_set).intersection(hin_wp_sugg_list_topk)
                    hin_wp_found_cnt = len(hin_wp_found)

                    hin_wp_precision = hin_wp_found_cnt/k
                    hin_wp_recall = hin_wp_found_cnt/test_size_strict
                    hin_wp_avg_precision[k] += hin_wp_precision
                    hin_wp_avg_recall[k] += hin_wp_recall
                    
            value = []
            hin_wp_f1_measures = [0]*test_size_strict
            for k in range(1,test_size_strict):
                #            
                hin_wp_avg_precision[k] = hin_wp_avg_precision[k]/kfold
                hin_wp_avg_recall[k] = hin_wp_avg_recall[k]/kfold
                if (hin_wp_avg_precision[k]+hin_wp_avg_recall[k]) !=0:
                    hin_wp_f1_measures[k] = 2*hin_wp_avg_precision[k]*hin_wp_avg_recall[k]/(hin_wp_avg_precision[k]+hin_wp_avg_recall[k])
                else:
                    hin_wp_f1_measures[k] = 0
                    
                # print([k,hin_wp_avg_precision[k]])
                # print([k,hin_wp_avg_recall[k]])
                # print([hin_wp_avg_recall[k],hin_wp_avg_precision[k]])
#                print([k,hin_wp_f1_measures[k]])
                value.append(hin_wp_f1_measures[k])
                
            return value;
        except IOError as e:
            print("=> ERROR: Cannot open file...")
            print(e)

