#!/bin/bash
set -x
set -e
baseurl='http://localhost:8080'

echo "Doing gene-based annotation requests"
time curl "$baseurl/variants?gene=TP53" | tee annotation-TP53.json
time curl "$baseurl/variants?gene=APC" | tee annotation-APC.json
time curl "$baseurl/variants?gene=ALDH2" | tee annotation-ALDH2.json