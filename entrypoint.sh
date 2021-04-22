#!/bin/bash
cd "$(dirname "$0")"
config="$1"

function clean_exit() {
	exit $1
}


expert_set=`cat "$config" | jq -r .expert_set`
sim_min_values=`cat "$config" | jq -r .sim_min_values`
sim_threshold=`cat "$config" | jq -r .sim_threshold`

this_dir=`pwd`

# echo -e "1\tCalculating expert similarities based on venues"
# apv=`cat "$config" | jq -r .apv_hin`
# apv_sims=`cat "$config" | jq -r .apv_sims_dir`
# mkdir "$apv_sims"

# java -jar EntitySimilarity.jar "$apv" "$expert_set" "$sim_min_values" "$sim_threshold" > "$apv_sims"/similarities.csv

# echo -e "2\tSorting similarities based on venues for each expert"
# cd "$apv_sims"
# awk -F $'\t' '{print>$1.csv}' similarities.csv
# rm similarities.csv
# find . -maxdepth 1 -type f -exec sort -k3 -nr -o {} {} \;

# cd "$this_dir"

# echo -e "3\tCalculating expert similarities based on topics"
# apt=`cat "$config" | jq -r .apt_hin`
# apt_sims=`cat "$config" | jq -r .apt_sims_dir`
# mkdir "$apt_sims"

# java -jar EntitySimilarity.jar "$apt" "$expert_set" "$sim_min_values" "$sim_threshold" > "$apt_sims"/similarities.csv

# echo -e "4\tSorting similarities based on topics for each expert"
# cd "$apt_sims"
# awk -F $'\t' '{print>$1.csv}' similarities.csv
# rm similarities.csv
# find . -maxdepth 1 -type f -exec sort -k3 -nr -o {} {} \;

cd "$this_dir"

echo -e "5\tCalculating expert set expansions"

