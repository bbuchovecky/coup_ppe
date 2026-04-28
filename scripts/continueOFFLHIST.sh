#!/bin/bash

SIMDIR="/glade/u/home/bbuchovecky/projects/coup_ppe/sims"
ARCHDIR="/glade/derecho/scratch/bbuchovecky/archive"
CASE="i.e21.CPLHIST_BGC.f19_f19_mg17.historical"
END_YEAR=2015
MEMBERS=(005 006 007 008 009 010 011 012 013 014 015 016 017 018 019 020 021 022 023 024 025 026 027 028)
# MEMBERS=(004)


for m in "${MEMBERS[@]}"; do

    mem="coupPPE.${m}"

    rundir="/glade/derecho/scratch/bbuchovecky/${CASE}.${mem}/run"
    latest_year=$(printf '%s\n' $rundir/*clm2.r.* | sed -E 's/.*\.clm2\.r\.([0-9]{4})-.*/\1/' | sort -n | tail -1)
    run_length=$((END_YEAR - latest_year))
    echo $m : 2015 - $latest_year = $run_length

    # mkdir $rundir/rest_pre$latest_year
    # mv $rundir/*.r*.* $rundir/rest_pre$latest_year/
    # mv $rundir/rest_pre$latest_year/*.r*.$latest_year* $rundir/

    cd $SIMDIR/$mem/$CASE.$mem
    pwd

    ./xmlchange STOP_OPTION="nyears"
    ./xmlchange STOP_N=$run_length
    ./xmlchange REST_OPTION="nyears"
    ./xmlchange REST_N=5
    ./xmlchange CONTINUE_RUN=TRUE

    ./xmlchange JOB_WALLCLOCK_TIME="02:30:00" --subgroup case.run

    ./case.submit

done
