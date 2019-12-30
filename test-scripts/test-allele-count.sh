#!/bin/bash
set -x
set -e
baseurl='http://localhost:8080'

function do_rsid_query {
  if [[ -z "$1" ]]; then
    echo "Must provide an rsid parameter"
  fi
  rsid=$1
  url="$baseurl/variants?return_results=true&rsid=$rsid"
  time curl "$url" | tee "allele-count-${rsid}.json"
}
function do_gene_query {
  if [[ -z "$1" ]]; then
    echo "Must provide an gene parameter"
  fi
  gene=$1
  url="$baseurl/variants?return_results=true&gene=$gene"
  time curl "$url" | tee "allele-count-${gene}.json"
}

# Validate rsid counts with the following, look at AC in info:
#   SELECT reference_name, pos, ref, alt, info
#   FROM `gbsc-gcp-project-annohive-dev.1000.1000Orig_half1_partitioned_clustered`
#   WHERE id='rs671'



echo "Doing rsid variant requests"
do_rsid_query rs671
do_rsid_query rs12913832
do_rsid_query rs1333049
do_rsid_query rs4988235

echo "Doing gene variant requests"
do_gene_query TP53
do_gene_query APC
do_gene_query ALDH2
