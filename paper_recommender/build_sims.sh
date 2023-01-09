#!/bin/bash
cd "$(dirname "$0")"
config="$1"

function clean_exit() {
	exit $1
}


pap_sims_file=`cat "$config" | jq -r .pap_hin`
pap_sims_dir=`cat "$config" | jq -r .pap_sims_dir`

ptp_sims_file=`cat "$config" | jq -r .ptp_hin`
ptp_sims_dir=`cat "$config" | jq -r .ptp_sims_dir`

echo -e "1\tSorting similarities based on authors for each paper"
cd "$pap_sims_dir"
awk -F $'\t' '{print>$1.csv}' "$pap_sims_file"
rm "$pap_sims_file"
find . -maxdepth 1 -type f -exec sort -k3 -nr -o {} {} \;

echo -e "2\tSorting similarities based on topics for each paper"
cd "$ptp_sims_dir"
awk -F $'\t' '{print>$1.csv}' "$ptp_sims_file"
rm "$ptp_sims_file"
find . -maxdepth 1 -type f -exec sort -k3 -nr -o {} {} \;