#!/bin/bash

for yr in {1950..2014}; do
    job="process_"$yr".job"
    sed 's/ystr/'$yr'/g' process_cpl_hist.csh > $job
    qsub $job
done
