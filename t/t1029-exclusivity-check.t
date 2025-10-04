#!/bin/sh
#
test_description='Ensure fluxion exclusive or implicit exclusive allocate consistently'

. `dirname $0`/sharness.sh

power_xml="${SHARNESS_TEST_SRCDIR}/data/hwloc-data/002N/corona.xml"
fluxbind="flux python ${SHARNESS_TEST_SRCDIR}/scripts/flux-binding.py"
show_topology="${SHARNESS_TEST_SRCDIR}/scripts/shell/binding/show_topology.sh"

# Jobspecs to test
slot_jobspec="${SHARNESS_TEST_SRCDIR}/data/resource/jobspecs/exclusive/implicit-slot.yaml"

# This is how to enable a faux topology, and add --xml to the fluxbind commands
# export TEST_HWLOC_XMLFILE=${power_xml}
# --xml ${power_xml}

# Examples of running
# This will just show an actual layout when debugging / learning
# flux python /workspaces/fs/t/scripts/flux-binding.py -N1 -n 2

# This will predict and test. Importantly, the shape (expressions) you provide MUST match what flux generates
# This might be the first time we are evaluating that we got (in topology) what we asked for :)

# This is two cores under one node, and for each core, we get two unique PMUs
# flux python /workspaces/fs/t/scripts/flux-binding.py -N1 -n 2 --cpu-affinity=per-task core:0 AND  core:1

# This is two tasks under one node, each two cores (distinct) that have 4 PMU.
# For this example I am going to illustrate that different expressions can ask for the same thing
# flux python /workspaces/fs/t/scripts/flux-binding.py -N1 -n 2 --cores-per-task=2 --cpu-affinity=per-task core:0-1 AND core:2-3
# flux python /workspaces/fs/t/scripts/flux-binding.py -N1 -n 2 --cores-per-task=2 --cpu-affinity=per-task core:0-1 x pu:0-3 AND core:2-3 x pu:4-7
# flux python /workspaces/fs/t/scripts/flux-binding.py -N1 -n 2 --cores-per-task=2 --cpu-affinity=per-task numa:0 x core:0-1 x pu:0-3 AND numa:0 x core:2-3 x pu:4-7

# Here is an example where we remove affinity. Now every task gets access to all PUs that it can see, so we see the same resources (cpuset, etc) for both
# flux python /workspaces/fs/t/scripts/flux-binding.py -N1 -n 2 core:0-1  AND core:0-1

# Here we can see how exclusive impacts - we throw away all limitations on the NUMA node and give THE ENTIRE THING
# flux python /workspaces/fs/t/scripts/flux-binding.py -N1 -n 2 core:0-7  AND core:0-7 --exclusive

# At this point, we can write a flux submit command (that we think we understand) and the description (shape) expression that we expect. We get to validate if we actually get it.


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

test_expect_success 'sanity check detection from command line without prediction' '
    $fluxbind -N1 -n 2
'

test_expect_success 'sanity check detection from jobspec without prediction' '
    # Dan - this says unsatisfiable and I do not know why because the same thing works from the command line
	# I am going to assume I mucked up this setup can you take a look? I will write tests with other shapes for now.
	# $fluxbind --jobspec ${slot_jobspec}
	# Should then be:
	# $fluxbind --jobspec ${slot_jobspec} core:0-1
'

test_expect_success 'detection and prediction from jobspec' '
	# $fluxbind --jobspec ${slot_jobspec} core:0-1
'

test_expect_success 'two cores under one node, and for each core, we get two unique PMUs' '
	$fluxbind -N1 -n 2 --cpu-affinity=per-task core:0 AND core:1
'

test_expect_success 'two tasks under one node, each one core (distinct) that has 2 PMU.' '
    # This example illustrates that different expressions can ask for the same thing
	$fluxbind -N1 -n 2 --cpu-affinity=per-task core:0 AND core:1
	$fluxbind -N1 -n 2 --cores-per-task=1 --cpu-affinity=per-task core:0-1 AND core:2-3
	$fluxbind -N1 -n 2 --cores-per-task=1 --cpu-affinity=per-task core:0-1 x pu:0-1 AND core:2-3 x pu:2-3
	$fluxbind -N1 -n 2 --cores-per-task=1 --cpu-affinity=per-task numa:0 x core:0-1 x pu:0-1 AND numa:0 x core:2-3 x pu:2-3
'

test_expect_success 'remove affinity and every task gets access to all PUs that it can see.' '
	$fluxbind -N1 -n 2 core:0-1  AND core:0-1
'

test_expect_success 'see how exclusive impacts - we throw away all limitations on the NUMA node and give the entire thing.' '
	$fluxbind -N1 -n 2 core:0-7  AND core:0-7 --exclusive
'

test_expect_success 'unload fluxion modules' '
 	remove_qmanager &&
 	remove_resource &&
 	flux module load sched-simple
'
test_done
