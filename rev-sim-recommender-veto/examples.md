# compute WG
python3 prepare_wg-doc.py ../metapaths/APA.csv data/author-ids.csv > data/SIGMOD/input/author_sim/WG/doc-sim.csv

# compute DOC 
python3 prepare_wg-doc.py ../metapaths/AP.csv data/author-ids.csv > data/SIGMOD/input/author_sim/DOC/doc-sim.csv

# split single similarities file into multiple file for each author
awk -F $'\t' '{print>$1".csv"}' [similarities_file]

# sort files based on similarity score (3rd column)
find . -maxdepth 1 -type f -exec sort -k3 -nr -o {} {} \;

# determine author valid ids; those that have similarities for all considered metapths
# python build_auth_list.py ./data/
python3 build_auth_list2.py data/JCDL/ > data/JCDL/input/authors/valid-ids.csv

# build folds
python3 build_folds.py data/SIGMOD/ 5

# prepare files for WG and DOC (runs jaccard similarity)
# WG needs APA metapath associations
# DOC needs AP metapath associations
python prepare_wg-doc.py [author_metapath_output] [valid_ids_file]

python run_exp.py data/SIGMOD/ 5 100
python sugg_analyser.py data/VLDB/ 5 0


# run average f1-score configuration
python3 main_borda.py
python3 main_sum.py
python3 main_rrf.py

# run experiment for specific configuration
python3 run_exp.py
