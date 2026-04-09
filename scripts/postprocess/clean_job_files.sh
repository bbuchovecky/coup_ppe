#!/bin/bash

for yr in {1950..2014}; do
    rm proc_cplhist_$yr.o*
    rm process_$yr.job
done

