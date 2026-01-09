#!/bin/bash

module load conda/latest
conda activate npl

GLADE_COLLECTION="d33b3614-6d04-11e5-ba46-22000b92c6ec"

SRC_PATH="/glade/derecho/scratch/bbuchovecky/archive"
DST_PATH="/glade/campaign/univ/uwas0155/ppe/historical"

PATTERN="f.e21.FHIST_BGC.f19_f19_mg17.historical.coupPPE.*/*/proc"

globus login --force
globus whoami --verbose

globus session consent \
    "urn:globus:auth:scope:transfer.api.globus.org:all[*https://auth.globus.org/scopes/${GLADE_COLLECTION}/data_access]"

globus transfer "${GLADE_COLLECTION}:${SRC_PATH}/${PATTERN}" "${GLADE_COLLECTION}:${DST_PATH}/${PATTERN}" \
    --recursive \
    --label "ppe proc files" \
    # --dry-run

