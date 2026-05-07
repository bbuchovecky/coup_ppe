#!/bin/bash

SDIR="/glade/u/home/bbuchovecky/projects/coup_ppe/scripts"
MEMBERS=(000 001 002 003 004 005 006 007 008 009 010 011 012 013 014 015 016 017 018 019 020 021 022 023 024 025 026 027 028)

for m in "${MEMBERS[@]}"; do

    mem="coupPPE.${m}"
    echo "MEMBER = ${mem}"

    cd $SDIR

    cp run.job "runFSSP370.${m}.job"
    echo -e "\n./runFSSP370.sh ${mem}" >> "runFSSP370.${m}.job"
    sed -i "s/name/runFSSP370.${m}/g" "runFSSP370.${m}.job"

    qsub "runFSSP370.${m}.job"

done
