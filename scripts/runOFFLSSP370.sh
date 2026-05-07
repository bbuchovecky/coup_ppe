#!/bin/bash

set -u  # exit if undefined variable is used

MEM=$1
CASENAME="i.e21.CPLSSP370_BGC.f19_f19_mg17.ssp370.${MEM}"
PROJECT=UWAS0155

WDIR="/glade/u/home/bbuchovecky/projects/coup_ppe/sims/${MEM}"
NAMELISTS="/glade/u/home/bbuchovecky/projects/coup_ppe/scripts/namelists"
NAMELISTMODS="/glade/u/home/bbuchovecky/projects/coup_ppe/pert/nlmods"  # BGB: fixed 04/30/26, only maximum_leaf_wetted_fraction is affected
PARAMFILES="/glade/u/home/bbuchovecky/projects/coup_ppe/pert/paramfiles"
SOURCEMODS="/glade/u/home/bbuchovecky/projects/coup_ppe/pert/srcmods/perturbed"

COMPSET=SSP370_DATM%CPLHIST_CLM50%BGC-CROP_SICE_SOCN_MOSART_CISM2%NOEVOLVE_SWAV
GRID=f19_f19_mg17
CESMROOT="/glade/u/home/bbuchovecky/cesm_source/cesm2.1.5"

SSTICE="/glade/campaign/univ/uwas0155/ppe/docn_sst/..."

REFCASE="f.e21.FHIST_BGC.f19_f19_mg17.historical.${MEM}"

CPLHIST_CASE="f.e21.FHIST_BGC.f19_f19_mg17.historical.coupPPE.cplhist"
CPLHIST_DIR="/glade/derecho/scratch/bbuchovecky/archive/${CPLHIST_CASE}/cpl/proc/"
CPLHIST_YR_ALIGN="2015"
CPLHIST_YR_START="2015"
CPLHIST_YR_END="2100"


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
cp "${SOURCEMODS}/all/"* ./SourceMods/src.clm/


# apply sourcemods for parameter change
cp "${SOURCEMODS}/${MEM}/"* ./SourceMods/src.clm/


# apply namelist mods for history fields
cp "${NAMELISTS}/FHIST/user_nl_clm" ./


# apply namelist mods for parameter change
cat "${NAMELISTMODS}/${MEM}.txt" >> user_nl_clm


# apply parameter file for parameter change
echo -e "\nparamfile = \"${PARAMFILES}/${MEM}.nc\"" >> user_nl_clm


./xmlchange JOB_PRIORITY="regular"
./xmlchange RUN_TYPE=hybrid
./xmlchange PROJECT=$PROJECT
./xmlchange RUN_STARTDATE="1950-01-01"
./xmlchange STOP_OPTION="nyears"
./xmlchange STOP_N=65
./xmlchange REST_OPTION="nyears"
./xmlchange REST_N=65
./xmlchange RESUBMIT=1
./xmlchange JOB_WALLCLOCK_TIME="04:30:00" --subgroup case.run


# finding the latest restart from IHIST
# this code is likely very brittle
./xmlchange RUN_REFCASE=$REFCASE
./xmlchange GET_REFCASE="True"
./xmlchange RUN_REFDIR="${ARCHIVE}/${REFCASE}/rest/1950-01-01-00000"
./xmlchange RUN_REFDATE="1950-01-01"


./xmlchange DATM_MODE="CPLHIST"
./xmlchange DATM_PRESAERO="cplhist"
./xmlchange DATM_TOPO="cplhist"
./xmlchange DATM_CPLHIST_CASE=$CPLHIST_CASE
./xmlchange DATM_CPLHIST_DIR=$CPLHIST_DIR
./xmlchange DATM_CPLHIST_YR_ALIGN=$CPLHIST_YR_ALIGN
./xmlchange DATM_CPLHIST_YR_START=$CPLHIST_YR_START
./xmlchange DATM_CPLHIST_YR_END=$CPLHIST_YR_END


./case.build
mv "${FILENAME}.log" .


cd $WDIR
echo $CASENAME>case.txt
rm commands.txt
echo "${SDIR}/setupFHIST.sh ${MEM}"> commands.txt
