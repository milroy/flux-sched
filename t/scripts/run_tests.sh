#!/bin/sh

export FLUX_EXEC_PATH=/home/milroy1/flux/flux-sched/t/scripts/:$FLUX_EXEC_PATH
flux module remove resource
flux module load -r 0 resource load-file=/home/milroy1/flux/flux-sched/t/data/resource/jgfs/dynamic/test-level$1-001.json load-format=jgf prune-filters=ALL:core subsystems=containment policy=low match-format=jgf

i=1
testiter=0
for arg in "$@"; do
  if [[ $i -gt 2 ]]; then
    if [[ $1 -eq $(( ${i} - 2 )) ]]; then
       testiter=$arg
       break
    fi
  fi
  ((i++))
done

echo "Nested flux level $1"
echo "Submitting ${testiter} jobs at this level"
for (( j=1; j<=testiter; j++ )); do
  flux resource match allocate /home/milroy1/flux/flux-sched/t/data/resource/jobspecs/dynamic/test-level$1-001.yaml
done

if [[ $1 -lt $2 ]]; then
  array=( "$@" )
  unset array[1]
  unset array[2]
  flux start ./run_tests.sh $(( $1 + 1 )) $2 "${array[@]}"

else
  flux resource match grow /home/milroy1/flux/flux-sched/t/data/resource/jobspecs/dynamic/test-level$1-001.yaml
fi
