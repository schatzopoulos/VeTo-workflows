#!/bin/bash
cd "$(dirname "$0")"

config="$1"

if ! spark-submit --master local[*] --conf spark.sql.shuffle.partitions=32 --py-files=../hminer/sources.zip ../hminer/Hminer.py "$config"; then
        echo "Error: Ranking"
        exit 1
fi

# find hin folder from json config 
hin=`cat "$config" | jq -r .hin_out`
out=`cat "$config" | jq -r .analysis_out`

# call community detection algorithm
cd ./dga-graphx/
bash ./bin/louvain -i "$hin/part-00000*" -o "$out"

# if ! python3 ../add_names.py -c "$config"; then 
#         echo "Error: Finding node names"
#         exit 3
# fi
