#!/bin/bash

ARCHDIR="/glade/derecho/scratch/bbuchovecky/archive"
CASE="f.e21.FHIST_BGC.f19_f19_mg17.historical"
PATTERN="*/proc/*"
MEMBERS=(000 001 002 003 004 005 006 007 008 009 010 011 012 013 014 015 016 017 018 019 020 021 022 023 024 025 026 027 028)

for m in "${MEMBERS[@]}"; do

    mem="coupPPE.${m}"
    echo "MEMBER = ${mem}"

    to_rm="${ARCHDIR}/${CASE}.${mem}"

    echo "BEFORE DELETION"
    du -sh $to_rm/$PATTERN

    rm -r $to_rm/$PATTERN

    echo ""
    
done
