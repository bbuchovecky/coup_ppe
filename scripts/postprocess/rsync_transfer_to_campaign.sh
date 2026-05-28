#!/bin/bash

# rsync utility for transferring PPE output from scratch to campaign

# Parse command-line arguments
DO=$1
DRY_RUN="false"
NO_NLMOD="false"
for arg in "$@"; do
    if [ "$arg" = "--dry-run" ]; then
        DRY_RUN="true"
    elif [ "$arg" = "--no-nlmod" ]; then
        NO_NLMOD="true"
    fi
done

# Check if DO is provided
if [ -z "$DO" ]; then
    echo "Usage: $0 <FHIST|OFFLHIST|IHIST> [--dry-run] [--no-nlmod]"
    echo ""
    echo "Required argument:"
    echo "  FHIST      - Transfer FHIST data"
    echo "  OFFLHIST   - Transfer OFFLHIST data"
    echo "  IHIST      - Transfer IHIST data"
    echo ""
    echo "Optional flags:"
    echo "  --dry-run  - Show what would be done without executing"
    echo "  --no-nlmod - Apply to members 005 and 006 only"
    exit 1
fi


# MEMBERS=(000)
# MEMBERS=(001 002 003 004 005 006 007 008 009 010 011 012 013 014 015 016 017 018 019 020 021 022 023 024 025 026 027 028)

MEMBERS=(005 006)



if [ "$DO" == "FHIST" ]; then
    echo "Transferring FHIST"

    for MEM in "${MEMBERS[@]}"; do

        if [[ "$NO_NLMOD" == "true" && ($MEM == 005 || $MEM == 006) ]]; then
            MEM="${MEM}.no_nlmod"
        fi

        echo "coupPPE.${MEM}"
        
        if [ "$DRY_RUN" = "true" ]; then
            echo "[DRY RUN] mkdir -p /glade/campaign/univ/uwas0155/ppe/historical/coupled_simulations/f.e21.FHIST_BGC.f19_f19_mg17.historical.coupPPE.${MEM}"
            echo "[DRY RUN] rsync FHIST data from: /glade/derecho/scratch/bbuchovecky/archive/./f.e21.FHIST_BGC.f19_f19_mg17.historical.coupPPE.${MEM}"
            echo "[DRY RUN] rsync FHIST data to: /glade/campaign/univ/uwas0155/ppe/historical/coupled_simulations"
        else
            mkdir -p /glade/campaign/univ/uwas0155/ppe/historical/coupled_simulations/f.e21.FHIST_BGC.f19_f19_mg17.historical.coupPPE.${MEM}

            rsync -a --info=progress2 \
                --relative \
                --ignore-existing \
                --exclude='*/atm/hist/***' \
                --exclude='*/lnd/hist/***' \
                --exclude='*/cpl/***' \
                /glade/derecho/scratch/bbuchovecky/archive/./f.e21.FHIST_BGC.f19_f19_mg17.historical.coupPPE.${MEM}/ \
                /glade/campaign/univ/uwas0155/ppe/historical/coupled_simulations
        fi
    done


elif [ "$DO" == "OFFLHIST" ]; then
    echo "Transferring OFFLHIST"

    for MEM in "${MEMBERS[@]}"; do

        if [[ "$NO_NLMOD" == "true" && ($MEM == 005 || $MEM == 006) ]]; then
            MEM="${MEM}.no_nlmod"
        fi

        echo "coupPPE.${MEM}"
        
        if [ "$DRY_RUN" = "true" ]; then
            echo "[DRY RUN] mkdir -p /glade/campaign/univ/uwas0155/ppe/historical/offline_simulations/i.e21.CPLHIST_BGC.f19_f19_mg17.historical.coupPPE.${MEM}"
            echo "[DRY RUN] rsync OFFLHIST data from: /glade/derecho/scratch/bbuchovecky/archive/./i.e21.CPLHIST_BGC.f19_f19_mg17.historical.coupPPE.${MEM}"
            echo "[DRY RUN] rsync OFFLHIST data to: /glade/campaign/univ/uwas0155/ppe/historical/offline_simulations/"
        else
            mkdir -p /glade/campaign/univ/uwas0155/ppe/historical/offline_simulations/i.e21.CPLHIST_BGC.f19_f19_mg17.historical.coupPPE.${MEM}

            rsync -a --info=progress2 \
                --relative \
                --ignore-existing \
                --exclude='*/atm/hist/***' \
                --exclude='*/lnd/hist/***' \
                --exclude='*/cpl/***' \
                /glade/derecho/scratch/bbuchovecky/archive/./i.e21.CPLHIST_BGC.f19_f19_mg17.historical.coupPPE.${MEM}/ \
                /glade/campaign/univ/uwas0155/ppe/historical/offline_simulations/
        fi
    done


elif [ "$DO" == "IHIST" ]; then
    echo "Transferring IHIST"

    for MEM in "${MEMBERS[@]}"; do

        if [[ "$NO_NLMOD" == "true" && ($MEM == 005 || $MEM == 006) ]]; then
            MEM="${MEM}.no_nlmod"
        fi

        echo "coupPPE.${MEM}"
        
        if [ "$DRY_RUN" = "true" ]; then
            echo "[DRY RUN] mkdir -p /glade/campaign/univ/uwas0155/ppe/historical/spinup_simulations/IHistClm50Bgc.CPLHIST.historical.coupPPE.${MEM}.IHIST"
            echo "[DRY RUN] rsync IHIST data from: /glade/derecho/scratch/bbuchovecky/archive/./IHistClm50Bgc.CPLHIST.historical.coupPPE.${MEM}.IHIST/"
            echo "[DRY RUN] rsync IHIST data to: /glade/campaign/univ/uwas0155/ppe/historical/spinup_simulations/"
        else
            mkdir -p /glade/campaign/univ/uwas0155/ppe/historical/spinup_simulations/IHistClm50Bgc.CPLHIST.historical.coupPPE.${MEM}.IHIST

            rsync -a --info=progress2 \
                --relative \
                --ignore-existing \
                /glade/derecho/scratch/bbuchovecky/archive/./IHistClm50Bgc.CPLHIST.historical.coupPPE.${MEM}.IHIST/ \
                /glade/campaign/univ/uwas0155/ppe/historical/spinup_simulations/
        fi
    done


else
    echo "Unknown option: $DO"


fi

