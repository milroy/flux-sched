#!/bin/bash

# This script reads an hwloc XML topology and reports the mapping
# of NUMA nodes to their Cores (with cpusets) and any attached GPUs.
# It uses a hybrid parsing strategy to handle different XML structures.

if ! command -v xmllint &> /dev/null
then
    echo "xmllint not found." >&2
    exit 1
fi

xml_output=""
input_source=""

if [ $# -eq 0 ]; then
    input_source="live system"
    xml_output=$(lstopo -p --no-io --output-format xml 2>&1)
    exit_code=$?
    if [ $exit_code -ne 0 ]; then
        xml_output=$(lstopo -p --no-io -o -.xml 2>&1)
        exit_code=$?
    fi
    if [ $exit_code -ne 0 ]; then
        echo "Failed to generate XML from lstopo." >&2
        echo "$xml_output" >&2
        exit 1
    fi
elif [ $# -eq 1 ]; then
    input_source="file: $1"

    if [ ! -f "$1" ]; then
        echo "File not found: $1" >&2
        exit 1
    fi
    xml_output=$(cat "$1")
else
    echo "Usage: $0 [path-to-topology.xml]" >&2
    exit 1
fi

# Discover all NUMA nodes from the XML content.
numa_indices=$(xmllint --xpath '//object[@type="NUMANode"]/@os_index' - <<< "$xml_output" | sed 's/os_index="\([0-9]*\)"/\1/g')

if [ -z "$numa_indices" ]; then
    echo "Error: No NUMANode objects with an 'os_index' were found in the input from $input_source." >&2
    exit 1
fi

for i in $numa_indices; do
      
    # 1. Find all Core objects that are descendants of the current NUMA node.
    core_indices_in_numa=$(xmllint --xpath "//object[@type='NUMANode' and @os_index='$i']//object[@type='Core']/@os_index" - 2>/dev/null <<< "$xml_output" \
        | sed 's/os_index="\([0-9]*\)"/\1/g')

    # If that fails, try the flat-topology fallback.
    if [ -z "$core_indices_in_numa" ]; then
        numa_nodeset=$(xmllint --xpath "string(//object[@type='NUMANode' and @os_index='$i']/@nodeset)" - <<< "$xml_output")
        if [ -n "$numa_nodeset" ]; then
            core_indices_in_numa=$(xmllint --xpath "//object[@type='Core' and @nodeset='$numa_nodeset']/@os_index" - 2>/dev/null <<< "$xml_output" \
                | sed 's/os_index="\([0-9]*\)"/\1/g')
        fi
    fi
    
    # 2. Build the output string iteratively, mapping CORE to its CPUSET.
    output="NUMA_NODE=${i}"
    core_cpuset_output=""

    for j in $core_indices_in_numa; do

        # For each core, find its cpuset attribute directly.
        cpuset=$(xmllint --xpath "string(//object[@type='Core' and @os_index='$j']/@cpuset)" - 2>/dev/null <<< "$xml_output")
        
        # Append CORE<id>_CPUSET=<value> to the string.
        core_cpuset_output+=" CORE${j}_CPUSET=${cpuset}"
    done
    
    # 3. Restore the GPU parsing logic.
    numa_nodeset=$(xmllint --xpath "string(//object[@type='NUMANode' and @os_index='$i']/@nodeset)" - 2>/dev/null <<< "$xml_output")
    gpu_list=$(xmllint --xpath "//object[@type='OSDev' and contains(@name, 'nvidia') and @nodeset='$numa_nodeset']/@name" - 2>/dev/null <<< "$xml_output" \
        | awk -F'"' '{for (k=2; k<=NF; k+=2) printf "GPU:%s,", $k}' \
        | sed 's/,$//')

    if [ -n "$core_cpuset_output" ]; then
        output="${output}${core_cpuset_output}"
    fi
    if [ -n "$gpu_list" ]; then
        output="${output} GPUS=${gpu_list}"
    fi
    echo "$output"
done
