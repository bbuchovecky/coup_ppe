#!/bin/bash

START=925300
END=925314

for ((i=START; i<=END; i++)); do
    qdel $i.casper-pbs
done