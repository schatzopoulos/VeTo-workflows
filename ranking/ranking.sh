#!/bin/bash

config="$1"

if ! spark-submit --master local[*] --conf spark.sql.shuffle.partitions=32 --py-files=../hminer/sources.zip ../hminer/Hminer.py "$config"; then
        echo "Error: Ranking"
        exit 1
fi

cd "$(dirname "$0")"

if ! python3 ../add_names.py -c "$config"; then 
        echo "Error: Finding node names"
        exit 3
fi
