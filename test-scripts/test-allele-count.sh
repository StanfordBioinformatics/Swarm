#!/bin/bash
set -x
set -e
baseurl='http://localhost:8080'

echo "Doing RSID variant requests"
time curl "$baseurl/variants?rsid=rs671&return_results=true" | tee allele-count-rs671.json
time curl "$baseurl/variants?rsid=rs12913832&return_results=true" | tee allele-count-rs12913832.json
time curl "$baseurl/variants?rsid=rs1333049&return_results=true" | tee allele-count-rs1333049.json
time curl "$baseurl/variants?rsid=rs4988235&return_results=true" | tee allele-count-rs4988235.json

echo "Doing gene requests"
