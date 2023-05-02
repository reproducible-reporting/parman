#!/usr/bin/env bash

### Comments

# This script will submit a job the first time it is executed, after which it waits for the job to
# complete. When the workflow script is interrupted and restarted, this script does not resubmit
# the job. It is assumed that the job just kept on running while the workflow script died.
# Later calls just wait for the (running) job to complete.

# This script is a temporary solution and needs more testing.
# (Testing is postponed until there is a nicer implementation.)

# The functionality below line 25 would make more sense in a helper script installed along
# with Sweet Future. It is mostly making up for a missing feature in Slurm, i.e. something
# similar to the bwait command in the LFS scheduler:
# https://www.ibm.com/docs/en/spectrum-lsf/10.1.0?topic=reference-bwait

### Things to modify

# Optionally change cluster
# module swap cluster/victini
# Specify arguments to sbatch, may also be put in job.sh
# SBATCH_ARGS=

### Do not change anything below

# TODO: It is assumed that the same cluster is used when restarting the workflow,
#       without explicitly checking this. It should be checked.
[ -e job.id ] || (
    echo "Submitting new compute job"
    sbatch --parsable ${SBATCH_ARGS} job.sh | sed -e 's/;.*//' > job.id
)
[ -s job.id ] || (echo "Submission failed"; exit 1)

# Wait for the job to complete
# The polling loop below is discouraged in the SLURM documentation,
# yet this is also how the sbatch --wait option works internally.
# See https://bugs.schedmd.com/show_bug.cgi?id=14638
# The maximum sleep time between two calls in `sbatch --wait` is 32 seconds.
# See https://github.com/SchedMD/slurm/blob/master/src/sbatch/sbatch.c
# Here, we take a random sleep time between 30 and 60 seconds to play nice.
JOBID=$(cat job.id)
while true; do
    # Random sleep to avoid many scontrol calls at the same time.
    SLEEP=$(( ( RANDOM % 30 )  + 30 ))
    # echo ${SLEEP}
    sleep ${SLEEP}
    LINE=$(scontrol show job ${JOBID} 2>&1 | egrep '(JobState|Invalid job id specified)')
    # echo $LINE
    # If the line is empty, scontrol failed due to some timeout, which happens occasionally.
    if [ -n "${LINE}" ]; then
        # When the state is anything different from pending, configuring or running, we stop.
        if (egrep -v -q 'JobState=(RUNNING|CONFIGURING|PENDING)' <<< ${LINE}); then
            break
        fi
        # If the job is long gone, scontrol may no longer know the job id.
        # This practically means the job completed some time ago.
        if (grep -q "Invalid job id specified" <<< ${LINE}); then
            break
        fi
    fi
done
