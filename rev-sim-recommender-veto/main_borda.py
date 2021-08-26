import sys
import numpy as np
from VeTo import VeTo

veto = VeTo()

for method in [ "borda"]:
    for dataset in ["data/VLDB/", "data/SIGMOD/", "data/CIKM/"]:
        for k in [100, 500, 1000, 2000, 5000]:
            for alpha in np.linspace(0, 1.0, 21):
                for beta in np.linspace(0, 1.0, 21):

                    alpha = round(alpha, 2)
                    beta = round(beta, 2)
                    sum = alpha + beta
                    if (round(sum, 2) != 1.0):
                        continue

                    for rrf_k in [0, 25, 50, 75, 100]:
                        f1_values = veto.run(method, dataset, 5, k, alpha, beta, rrf_k)
                        f1_values = f1_values[0:100]
                        avg_f1 = np.mean(f1_values)
                        print (method + "\t" + str(dataset) + "\t" + str(k) + "\t" + str(alpha) + "\t" + str(beta) + "\t" + str(rrf_k) + "\t" + str(avg_f1))

                        if method == 'borda' or method == 'sum':
                            break
