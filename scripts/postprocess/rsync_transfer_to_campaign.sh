#!/bin/bash

# rsync utility for transferring PPE output from scratch to campaign

# MEMBERS=(000 001 002 003 004 005 006 007 008 009 010 011 012 013 014 015 016 017 018 019 020 021 022 023 024 025 026 027 028)
MEMBERS=(004)

DO="OFFLHIST"

if [ "$DO" == "FHIST" ]; then
    echo "Transferring FHIST"

    for MEM in "${MEMBERS[@]}"; do

        echo "coupPPE.${MEM}"
        mkdir -p /glade/campaign/univ/uwas0155/ppe/historical/coupled_simulations/f.e21.FHIST_BGC.f19_f19_mg17.historical.coupPPE.${MEM}

        rsync -a --info=progress2 \
            --relative \
            --ignore-existing \
            --exclude='*/atm/hist/***' \
            --exclude='*/lnd/hist/***' \
            --exclude='*/cpl/***' \
            /glade/derecho/scratch/bbuchovecky/archive/./f.e21.FHIST_BGC.f19_f19_mg17.historical.coupPPE.${MEM}/ \
            /glade/campaign/univ/uwas0155/ppe/historical/coupled_simulations
    done


elif [ "$DO" == "OFFLHIST" ]; then
    echo "Transferring OFFLHIST"

    for MEM in "${MEMBERS[@]}"; do

        echo "coupPPE.${MEM}"
        mkdir -p /glade/campaign/univ/uwas0155/ppe/historical/offline_simulations/i.e21.CPLHIST_BGC.f19_f19_mg17.historical.coupPPE.${MEM}

        rsync -a --info=progress2 \
            --relative \
            --ignore-existing \
            --exclude='*/atm/hist/***' \
            --exclude='*/lnd/hist/***' \
            --exclude='*/cpl/***' \
            /glade/derecho/scratch/bbuchovecky/archive/./i.e21.CPLHIST_BGC.f19_f19_mg17.historical.coupPPE.${MEM}/ \
            /glade/campaign/univ/uwas0155/ppe/historical/offline_simulations/
    done


elif [ "$DO" == "IHIST" ]; then
    echo "Transferring IHIST"

    for MEM in "${MEMBERS[@]}"; do

        echo "coupPPE.${MEM}"
        mkdir -p /glade/campaign/univ/uwas0155/ppe/historical/spinup_simulations/IHistClm50Bgc.CPLHIST.historical.coupPPE.${MEM}

        rsync -a --info=progress2 \
            --relative \
            --ignore-existing \
            /glade/derecho/scratch/bbuchovecky/archive/./IHistClm50Bgc.CPLHIST.historical.coupPPE.${MEM}.IHIST/ \
            /glade/campaign/univ/uwas0155/ppe/historical/spinup_simulations/
    done


else
    echo "Unknown option: $DO"


fi

