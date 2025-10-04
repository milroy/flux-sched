#!/bin/sh
#
test_description='Ensure fluxion exclusive or implicit exclusive allocate consistently'

. `dirname $0`/sharness.sh

power_xml="${SHARNESS_TEST_SRCDIR}/data/hwloc-data/002N/corona.xml"
fluxbind="flux python ${SHARNESS_TEST_SRCDIR}/scripts/flux-binding.py"
show_topology="${SHARNESS_TEST_SRCDIR}/scripts/shell/binding/show_topology.sh"

# This is how to enable a faux topology, and add --xml to the fluxbind commands
# export TEST_HWLOC_XMLFILE=${power_xml}
# --xml ${power_xml}

if test_have_prereq ASAN; then
    skip_all='skipping issues tests under AddressSanitizer'
    test_done
fi

test_under_flux 1

# Usage: wait_for_allocated_count N
wait_for_allocated_count() {
    retries=5
    while test $retries -ge 0; do
        test $(flux resource list -s allocated -no {nnodes}) -eq $1 && return 0
        retries=$(($retries-1))
        sleep 0.1
    done
    return 1
}

test_expect_success 'successfully load fluxion' '
	flux module remove sched-simple &&
	flux module remove resource &&
	flux config load <<-EOF &&
	[job-manager.housekeeping]
	command = [
		"sh",
		"-c",
		"test \$(flux getattr rank) -eq 4 && sleep inf; exit 0"
	]
	release-after = "0s"

	[resource]
	noverify = true
	norestrict = true

	[sched-fluxion-resource]
	match-policy = "low"
	EOF
	flux module load resource monitor-force-up &&
	flux module load sched-fluxion-resource &&
	flux module load sched-fluxion-qmanager &&
	flux module unload job-list &&
	flux module list &&
	flux queue start --all --quiet &&
	flux resource list &&
	flux resource status
'

flux resource list
test_expect_success 'report current (discovered) topology (xml)' '
    # This reports the topology of the node that is discovered
    $show_topology    
'

test_expect_success 'report faux power topology (xml)' '
	# This is a topology that is generated based on power.json (large)
	# We likely could reduce this in size if needed.
    $show_topology $power_xml
'

echo $fluxbind 
test_expect_success 'asking for two tasks, no binding, maps across nodes.' '
    # This ensures we have faux cpuset and ids to match the 5 node cluster
    $fluxbind --cpu-affinity=per-task --cores-per-task 1 -N1 -n4 --cores-per-task 1
'

test_expect_success 'unload fluxion modules' '
 	remove_qmanager &&
 	remove_resource &&
 	flux module load sched-simple
'
test_done
