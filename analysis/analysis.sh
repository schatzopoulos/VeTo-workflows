#!/bin/bash
cd "$(dirname "$0")"

config="$1"

# performs HIN transformation and ranking (if needed)
if ! spark-submit --master local[*] --conf spark.sql.shuffle.partitions=32 --py-files=../hminer/sources.zip ../hminer/Hminer.py "$config"; then
       echo "Error: HIN Transformation"
       exit 1
fi

analyses=`cat "$config" | jq -r .analyses`

# format ranking ouput
if [[ " ${analyses[@]} " =~ "Ranking" ]]; then
	ranking_out=`cat "$config" | jq -r .ranking_out`
	ranking_final=`cat "$config" | jq -r .final_ranking_out`

	if ! python3 ../add_names.py -c "$config" "Ranking" "$ranking_out" "$ranking_final"; then 
         echo "Error: Finding node names in Ranking output"
         exit 2
	fi
fi



if [[ " ${analyses[@]} " =~ "Similarity Join" ]]; then

	# find hin folder from json config 
	join_hin=`cat "$config" | jq -r .join_hin_out`

	if ! java -jar ../similarity/EntitySimilarity-1.0-SNAPSHOT.jar -c "$config" "Similarity Join" "$join_hin/part-"*; then
	        echo "Error: Similarity Join"
	        exit 2
	fi

	if ! python3 ../similarity/add_names_sim.py -c "$config" "Similarity Join"; then 
         echo "Error: Finding node names in Similarity Join output"
         exit 2
	fi
fi

if [[ " ${analyses[@]} " =~ "Similarity Search" ]]; then

	# find hin folder from json config 
	join_hin=`cat "$config" | jq -r .join_hin_out`

	if ! java -jar ../similarity/EntitySimilarity-1.0-SNAPSHOT.jar -c "$config" "Similarity Search" "$join_hin/part-"*; then
	        echo "Error: Similarity Search"
	        exit 2
	fi

	if ! python3 ../similarity/add_names_sim.py -c "$config" "Similarity Search"; then 
         echo "Error: Finding node names in Similarity Search output"
         exit 2
	fi
fi
	
# perform Community Detection
if [[ " ${analyses[@]} " =~ "Community Detection" ]]; then

	# find hin folder from json config 
	hin=`cat "$config" | jq -r .hin_out`
	communities_out=`cat "$config" | jq -r .communities_out`
	final_communities_out=`cat "$config" | jq -r .final_communities_out`

	current_dir=`pwd`

	# cat $out/*.csv > $out/part-00000

	# call community detection algorithm
	cd ../louvain/

	if ! bash ./bin/louvain -m "local[8]" -p 8 -i "$hin/part-*" -o "$communities_out" 2>/dev/null; then
		echo "Error: Community Detection"
		exit 3
	fi

	cd $current_dir

	if ! python3 ../add_names.py -c "$config" "Community Detection" "$communities_out" "$final_communities_out"; then 
         echo "Error: Finding node names in Community Detection output"
         exit 2
	fi
fi