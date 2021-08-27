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

echo -e "1\tCalculating expert similarities based on venues"
apv=`cat "$config" | jq -r .apv_hin`
apv_sims=`cat "$config" | jq -r .apv_sims_dir`
mkdir "$apv_sims"

if ! java -jar ./heysim-veto/target/EntitySimilarity-1.0-SNAPSHOT.jar "$apv" "$expert_set" "$sim_min_values" "$sim_threshold" > "$apv_sims"/similarities.csv; then 
	echo "Error: Calculating expert similarities based on venues"
 	clean_exit 2
fi

echo -e "2\tSorting similarities based on venues for each expert"
cd "$apv_sims"
awk -F $'\t' '{print>$1.csv}' similarities.csv
rm similarities.csv
find . -maxdepth 1 -type f -exec sort -k3 -nr -o {} {} \;

cd "$this_dir"

echo -e "3\tCalculating expert similarities based on topics"
apt=`cat "$config" | jq -r .apt_hin`
apt_sims=`cat "$config" | jq -r .apt_sims_dir`
mkdir "$apt_sims"

if ! java -jar ./heysim-veto/target/EntitySimilarity-1.0-SNAPSHOT.jar "$apt" "$expert_set" "$sim_min_values" "$sim_threshold" > "$apt_sims"/similarities.csv; then 
	echo "Error: Calculating expert similarities based on topics"
 	clean_exit 3
fi

echo -e "4\tSorting similarities based on topics for each expert"
cd "$apt_sims"
awk -F $'\t' '{print>$1.csv}' similarities.csv
rm similarities.csv
find . -maxdepth 1 -type f -exec sort -k3 -nr -o {} {} \;

cd "$this_dir"

echo -e "5\tCalculating expert set expansions"

apt_weight=`cat "$config" | jq -r .apt_weight`
apv_weight=`cat "$config" | jq -r .apv_weight`
output_size=`cat "$config" | jq -r .output_size`
sims_per_expert=`cat "$config" | jq -r .sims_per_expert`
veto_output=`cat "$config" | jq -r .veto_output`

# echo "$expert_set" "$veto_output" "$apv_sims" "$apt_sims" "$sims_per_expert" "$apt_weight" "$apv_weight" 0 borda "$output_size"

if ! python3 rev-sim-recommender-veto/run_exp.py "$expert_set" "$veto_output" "$apv_sims" "$apt_sims" "$sims_per_expert" "$apt_weight" "$apv_weight" borda 0 "$output_size"; then 
	echo "Error: Calculating expert set expansions"
 	clean_exit 3
fi

echo -e "6\tMapping expert ids to names"
author_names=`cat "$config" | jq -r .author_names`
final_output=`cat "$config" | jq -r .final_output`

if ! python add_names.py "$author_names" "$veto_output" "$final_output" ; then 
         echo "Error: Finding expert set names"
         clean_exit 4
fi
