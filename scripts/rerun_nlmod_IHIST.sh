#!/bin/bash

# set -e  # exit on first error

MEM="coupPPE.006"
CASENAME="IHistClm50Bgc.CPLHIST.historical.${MEM}.IHIST"
PROJECT=UWAS0155

SDIR="/glade/u/home/bbuchovecky/projects/coup_ppe/scripts"
WDIR="/glade/u/home/bbuchovecky/projects/coup_ppe/sims/${MEM}"
ARCHIVE="/glade/derecho/scratch/bbuchovecky/archive"
NAMELISTS="/glade/u/home/bbuchovecky/projects/coup_ppe/scripts/namelists"
NAMELISTMODS="/glade/u/home/bbuchovecky/projects/coup_ppe/pert/nlmods"
PARAMFILES="/glade/u/home/bbuchovecky/projects/coup_ppe/pert/paramfiles"
SOURCEMODS="/glade/u/home/bbuchovecky/projects/coup_ppe/pert/srcmods/perturbed"

COMPSET=HIST_DATM%CPLHIST_CLM50%BGC-CROP_SICE_SOCN_MOSART_CISM2%NOEVOLVE_SWAV
GRID=f19_f19_mg17
CESMROOT="/glade/u/home/bbuchovecky/cesm_source/cesm2.1.5"

REFCASE="I1850Clm50Bgc.CPLHIST.historical.${MEM}.pAD"

CPLHIST_CASE="f.e21.FHIST_BGC.f19_f19_mg17.historical.coupPPE.cplhist"
CPLHIST_DIR="/glade/derecho/scratch/bbuchovecky/archive/${CPLHIST_CASE}/cpl/proc/"
CPLHIST_YR_ALIGN="1850"
CPLHIST_YR_START="1850"
CPLHIST_YR_END="1949"


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
cp "${NAMELISTS}/IHIST/"* ./


# apply namelist mods for parameter change
cat "${NAMELISTMODS}/${MEM}.txt" >> user_nl_clm


# apply parameter file for parameter change
echo -e "\nparamfile = \"${PARAMFILES}/${MEM}.nc\"" >> user_nl_clm


./xmlchange JOB_PRIORITY="regular"
./xmlchange RUN_TYPE=hybrid
./xmlchange PROJECT=$PROJECT
./xmlchange RUN_STARTDATE="1850-01-01"
./xmlchange STOP_OPTION="nyears"
./xmlchange STOP_N=100
./xmlchange REST_OPTION="nyears"
./xmlchange REST_N=25
./xmlchange RESUBMIT=0
./xmlchange JOB_WALLCLOCK_TIME="09:00:00" --subgroup case.run


# finding the latest restart from PAD
# this code is likely very brittle
./xmlchange RUN_REFCASE=$REFCASE
./xmlchange GET_REFCASE="True"
REFREST="${ARCHIVE}/${REFCASE}/rest"
last_date=$(ls $REFREST | tail -n1)
REFDIR="${REFREST}/${last_date}"
REFDATE=${last_date%-*}
./xmlchange RUN_REFDIR=$REFDIR
./xmlchange RUN_REFDATE=$REFDATE


./xmlchange DATM_MODE="CPLHIST"
./xmlchange DATM_PRESAERO="cplhist"
./xmlchange DATM_TOPO="cplhist"
./xmlchange DATM_CPLHIST_CASE=$CPLHIST_CASE
./xmlchange DATM_CPLHIST_DIR=$CPLHIST_DIR
./xmlchange DATM_CPLHIST_YR_ALIGN=$CPLHIST_YR_ALIGN
./xmlchange DATM_CPLHIST_YR_START=$CPLHIST_YR_START
./xmlchange DATM_CPLHIST_YR_END=$CPLHIST_YR_END


./case.build
./case.submit
mv "${FILENAME}.log" .