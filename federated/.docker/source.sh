#!/bin/bash
set -e

echo "Running script /usr/src/code/EffectSize.R with pwd: $(pwd)"
Rscript /usr/src/code/EffectSize.R --wait

exit 0
