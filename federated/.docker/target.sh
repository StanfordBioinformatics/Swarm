#!/bin/bash
set -e

DATA_DIR=${INPUT:-data}
echo "Using ${DATA_DIR} as the data directory..."

plink \
    --bfile ${DATA_DIR}/EUR.QC \
    --clump-p1 1 \
    --clump-r2 0.1 \
    --clump-kb 250 \
    --clump ${DATA_DIR}/Height.QC.Transformed \
    --clump-snp-field SNP \
    --clump-field P \
    --out EUR

awk 'NR!=1{print $3}' EUR.clumped > EUR.valid.snp
awk '{print $3,$8}' ${DATA_DIR}/Height.QC.Transformed > SNP.pvalue
echo "0.001 0 0.001" > range_list
echo "0.05 0 0.05" >> range_list
echo "0.1 0 0.1" >> range_list
echo "0.2 0 0.2" >> range_list
echo "0.3 0 0.3" >> range_list
echo "0.4 0 0.4" >> range_list
echo "0.5 0 0.5" >> range_list

plink \
    --bfile ${DATA_DIR}/EUR.QC \
    --score ${DATA_DIR}/Height.QC.Transformed 3 4 12 header \
    --q-score-range range_list SNP.pvalue \
    --extract EUR.valid.snp \
    --out EUR

# First, we need to perform prunning
plink \
    --bfile ${DATA_DIR}/EUR.QC \
    --indep-pairwise 200 50 0.25 \
    --out EUR

# Then we calculate the first 6 PCs
plink \
    --bfile ${DATA_DIR}/EUR.QC \
    --extract EUR.prune.in \
    --pca 6 \
    --out EUR

Rscript /usr/src/code/PRS.R --wait

# finally, the best-fit script.
Rscript /usr/src/code/best-fit.R --wait

exit 0
