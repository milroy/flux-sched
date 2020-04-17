#!/bin/sh

export FLUX_EXEC_PATH=/home/milroy1/flux/flux-sched/t/scripts/:$FLUX_EXEC_PATH
flux module remove resource
flux module load -r 0 resource load-file=/home/milroy1/flux/flux-sched/t/data/resource/jgfs/dynamic/test-level3-001.json load-format=jgf prune-filters=ALL:core subsystems=containment policy=low match-format=jgf

python3 t/scripts/child-uri-setup.py

flux resource match allocate /home/milroy1/flux/flux-sched/t/data/resource/jobspecs/dynamic/test-level3-001.yaml

sleep 300
