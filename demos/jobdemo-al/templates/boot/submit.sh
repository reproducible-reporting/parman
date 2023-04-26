#!/usr/bin/env bash
set -e
[ -e run.jobid ] || (sbatch --parsable run | sed -e 's/:.*//' > run.jobid)
[ -s run.jobid ] || (echo "submission failed"; exit 1) 
JOBID=$(cat run.jobid)
while (squeue --job $JOBID -h -o '%t' | egrep -q '(CF|PD|R)'); do echo sleep; sleep 10; done
