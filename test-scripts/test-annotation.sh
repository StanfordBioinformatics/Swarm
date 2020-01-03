#!/bin/bash
set -x
set -e
baseurl='http://localhost:8080'

function gene_query_variant_athena_annotation_bigquery_dest_athena {
  gene=$1
  if [[ -z $gene ]]; then
    echo "Must provide a gene name as first parameter"
    exit 1
  fi
  set +x
  url="http://localhost:8080/annotate?variantsDatabaseType=athena"
  url="$url&variantsDatabaseName=swarm"
  url="$url&variantsTable=thousandorig_half2_partitioned_bucketed"
  url="$url&annotationDatabaseType=bigquery"
  url="$url&annotationDatabaseName=swarm"
  url="$url&annotationTable=hg19_Variant_9B_Table"
  url="$url&destinationDatabaseType=athena"
  url="$url&return_results=true"
  url="$url&gene=$gene"
  set -x

  time curl "$url" | tee "annotation-${gene}.json"
}

echo "Doing gene-based annotation requests"
gene_query_variant_athena_annotation_bigquery_dest_athena TP53
gene_query_variant_athena_annotation_bigquery_dest_athena APC
gene_query_variant_athena_annotation_bigquery_dest_athena ALDH2

#time curl "$baseurl/variants?gene=TP53&return_results=true" | tee annotation-TP53.json
#time curl "$baseurl/variants?gene=APC&return_results=true" | tee annotation-APC.json
#ime curl "$baseurl/variants?gene=ALDH2&return_results=true" | tee annotation-ALDH2.json

