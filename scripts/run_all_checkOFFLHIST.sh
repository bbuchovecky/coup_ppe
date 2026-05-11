#!/bin/bash

# MEMBERS=(000 001 002 003 004 005 006 007 008 009 010 011 012 013 014 015 016 017 018 019 020 021 022 023 024 025 026 027 028)
MEMBERS=(005 006)

for MEM in "${MEMBERS[@]}"; do

    ./checkOFFLHIST.py coupPPE.$MEM --domain lnd
    ./checkOFFLHIST.py coupPPE.$MEM --domain lnd --no-nlmod

done
