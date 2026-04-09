#!/bin/bash

module load conda
conda activate ppe-py

MEM="coupPPE.cplhist"
CASENAME="f.e21.FHIST_BGC.f19_f19_mg17.historical.${MEM}"
PROJECT=UWAS0155

WDIR="/glade/u/home/bbuchovecky/projects/coup_ppe/sims/${MEM}"
NAMELISTS="/glade/u/home/bbuchovecky/projects/coup_ppe/scripts/namelists"
PARAMFILES="/glade/u/home/bbuchovecky/projects/coup_ppe/pert/paramfiles"
SOURCEMODS="/glade/u/home/bbuchovecky/projects/coup_ppe/pert/srcmods/perturbed"

COMPSET=HIST_CAM60_CLM50%BGC-CROP_CICE%PRES_DOCN%DOM_MOSART_CISM2%NOEVOLVE_SWAV
GRID=f19_f19_mg17
CESMROOT="/glade/u/home/bbuchovecky/cesm_source/cesm2.1.5"

REFCASE="b.e21.B1850.f19_g17.CMIP6-piControl-2deg.001"
REFDIR="/glade/campaign/cesm/cesmdata/inputdata/cesm2_init/b.e21.B1850.f19_g17.CMIP6-piControl-2deg.001/0321-01-01"
REFDATE="0321-01-01"
STARTDATE="1850-01-01"


# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++


# redirect output from this script to a log file
FILENAME="$(pwd)/$(basename "${0%.*}")"
exec > >(tee -a "${FILENAME}.log") 2>&1
echo $FILENAME
echo $USER
date +%y-%m-%dT%H:%M:%S


CASEROOTBASE=$WDIR
caseroot="${CASEROOTBASE}/${CASENAME}"
echo "caseroot: ${caseroot}"


cd "${CESMROOT}/cime/scripts"
./create_newcase --case $caseroot --compset $COMPSET --res $GRID --project $PROJECT --mach derecho --run-unsupported
cd $caseroot
./case.setup

# apply sourcemods for history fields
cp "${SOURCEMODS}/all/clm/"* ./SourceMods/src.clm/
cp "${SOURCEMODS}/all/cam/"* ./SourceMods/src.cam/


# no sourcemods for parameter change


# apply namelist mods for history fields
cp "${NAMELISTS}/FHIST_CPL_EXT/"* ./


# no namelist mods for parameter change


# apply parameter file for default (with FUN bug fix)
echo -e "paramfile = \"${PARAMFILES}/coupPPE.000.nc\"" >> user_nl_clm


./xmlchange RUN_TYPE=hybrid
./xmlchange PROJECT=$PROJECT
./xmlchange JOB_PRIORITY="regular"
./xmlchange RUN_STARTDATE=$STARTDATE
./xmlchange RUN_REFCASE=$REFCASE
./xmlchange RUN_REFDIR=$REFDIR
./xmlchange RUN_REFDATE=$REFDATE
./xmlchange GET_REFCASE="True"


./xmlchange STOP_OPTION="nyears"
./xmlchange STOP_N=5
./xmlchange REST_OPTION="nyears"
./xmlchange REST_N=5
./xmlchange RESUBMIT=32
./xmlchange JOB_WALLCLOCK_TIME=09:00:00 --subgroup case.run


./case.build
./case.submit

mv "${FILENAME}.log" .
