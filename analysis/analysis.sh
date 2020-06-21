#!/bin/bash
cd "$(dirname "$0")"

config="$1"

# performs HIN transformation and ranking (if needed)
if ! spark-submit --master local[*] --conf spark.sql.shuffle.partitions=32 --py-files=../hminer/sources.zip ../hminer/Hminer.py "$config"; then
       echo "Error: HIN Transformation"
       exit 1
fi

# if operation contains "community", executes Louvain method for community detection
operation=`cat "$config" | jq -r .operation`
if [[ $operation == *"community"* ]]; then

	# find hin folder from json config 
	hin=`cat "$config" | jq -r .hin_out`
	out=`cat "$config" | jq -r .communities_out`
	current_dir=`pwd`

	cat $out/*.csv > $out/part-00000

	# call community detection algorithm
	cd ../louvain/

	if ! bash ./bin/louvain -m "local[8]" -p 8 -i "$hin/part-*" -o "$out" 2>/dev/null; then
		echo "Error: Community Detection"
		exit 2
	fi

	cd $current_dir
fi
	
# merges available results and appends entity names
if ! python3 ../add_names.py -c "$config"; then 
         echo "Error: Finding node names"
         exit 3
fi