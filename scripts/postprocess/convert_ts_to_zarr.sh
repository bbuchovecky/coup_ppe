#!/bin/bash

GCOMP=(atm lnd)
STREAM=(h0 h1 h2)
FREQ=(month_1 day_1)

# atm day_1 h2
# atm month_1 h0
# atm month_1 h1

# atm day_1 h2
# atm month_1 h0
# atm month_1 h1

for gc in "${GCOMP[@]}"; do

    python3 ppe_ts_to_zarr.py \
        --case f.e21.FHIST_BGC.f19_f19_mg17.historical.coupPPE \
        --ts-root /glade/campaign/univ/uwas0155/ppe/historical/coupled_simulations \
        --zarr-root /glade/derecho/scratch/bbuchovecky/zarr/ppe/historical/coupled_simulations \
        --gcomp $gc \
        --freq month_1 \
        --stream h0 \
        --all-vars \
        # --skip-existing \
        # --dry-run

    python3 ppe_ts_to_zarr.py \
        --case f.e21.FHIST_BGC.f19_f19_mg17.historical.coupPPE \
        --ts-root /glade/campaign/univ/uwas0155/ppe/historical/coupled_simulations \
        --zarr-root /glade/derecho/scratch/bbuchovecky/zarr/ppe/historical/coupled_simulations \
        --gcomp $gc \
        --freq month_1 \
        --stream h1 \
        --all-vars \
        # --skip-existing \
        # --dry-run

    python3 ppe_ts_to_zarr.py \
        --case f.e21.FHIST_BGC.f19_f19_mg17.historical.coupPPE \
        --ts-root /glade/campaign/univ/uwas0155/ppe/historical/coupled_simulations \
        --zarr-root /glade/derecho/scratch/bbuchovecky/zarr/ppe/historical/coupled_simulations \
        --gcomp $gc \
        --freq day_1 \
        --stream h2 \
        --all-vars \
        # --skip-existing \
        # --dry-run

done
