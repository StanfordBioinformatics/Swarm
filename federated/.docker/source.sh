#!/bin/bash
set -ex

echo "Running script /usr/src/code/EffectSize.R with pwd: $(pwd)"
Rscript /usr/src/code/EffectSize.R --wait

exit 0
