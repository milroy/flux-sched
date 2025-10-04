#!/bin/bash
#
# report_and_run.sh

rank="$FLUX_TASK_RANK"
node=$(hostname)

# pid=$$
# echo $pid
# binding=$(hwloc-ps --pid $$ --cpuset | awk '{print $2}')
# binding=$(hwloc-bind --get --taskset)
# The PEWPEWPWE is to help identify the line in the output
# binding=$(echo "$cores" | hwloc-calc --taskset | tail -n 1)

cores=$(hwloc-bind --get | hwloc-calc --quiet --intersect core 2>/dev/null | tail -n 1)
binding=$(hwloc-ps --pid $$ --cpuset | awk '{print $2}')

echo "PEWPEWPEW $rank $node $binding $cores" >&2

# Execute the real application (if any).
# if [ $# -gt 0 ]; then
#    exec "$@"
# fi