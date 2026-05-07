#!/bin/bash

set -e  # exit on first error
set -u  # exit if undefined variable is used

SDIR="/glade/u/home/bbuchovecky/projects/coup_ppe/scripts"
CASEROOTDIR="/glade/u/home/bbuchovecky/projects/coup_ppe/sims"
MEMBERS=(006)  # 005 006 - only perturbations of maximum_leaf_wetted_fraction

for m in "${MEMBERS[@]}"; do

    mem="coupPPE.${m}"
    echo "MEMBER = ${mem}"

    wdir="${CASEROOTDIR}/${mem}"
    cd $wdir

    echo "${SDIR}/setupPAD.sh ${mem}" > "${wdir}/commands.txt"

    cd "$wdir" || exit 1
    qsub -v MEM="$mem" segment001.job
    
done