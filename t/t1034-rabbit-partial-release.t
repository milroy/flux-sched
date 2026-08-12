#!/bin/sh
#
test_description='Test that fluxion does not leak rabbit ssd state when a
multi-node rabbit job is released to housekeeping as fragmented, per-node
.free RPCs (the production path), and drives an explicit full cancel.

Background: vertices never named in a freed R fragment -- rank-less rabbit
ssd vertices, and ranked-but-never-freed rabbit vertices -- used to leak
their per-job allocation/tag state when a job holding them was released via
a sequence of rv1exec partial-cancels followed by a full cancel.
dfu_impl_t::sweep_job_state() now purges all remaining per-job vertex state
at job retirement. t/t3044-resource-rabbit-cancel.t exercises this at the
resource-query level by feeding partial-cancel/cancel commands directly to
resource-query. This test instead drives the *live* production path: a real
flux instance, with job-manager housekeeping enabled and configured with a
per-node command that completes at different times on different ranks, so
that a multi-node rabbit jobs release arrives at fluxion as multiple
partial-cancel fragments (one per rank, as flux-core housekeeping actually
does it), followed by qmanagers final cancel once the job goes inactive.
'

. $(dirname $0)/sharness.sh

if test_have_prereq ASAN; then
    skip_all='skipping rabbit partial-release tests under AddressSanitizer'
    test_done
fi

cluster_jgf="${SHARNESS_TEST_SRCDIR}/data/resource/jgfs/rabbit.json"
HOSTLIST="hetchy[1,201-202,1001-1018]"
SIZE="$(flux hostlist -c ${HOSTLIST})"

test_under_flux ${SIZE}

# Usage: hk_wait_for_running count
# Wait (up to 30s) for the number of jobs actively in housekeeping to
# reach $1.  Copied from t1026-rv1-partial-release.t.
hk_wait_for_running () {
	count=0
	while test $(flux housekeeping list -no {id} | wc -l) -ne $1; do
		count=$(($count+1));
		test $count -eq 300 && return 1 # max 300 * 0.1s sleep = 30s
		sleep 0.1
	done
}

# Usage: hk_wait_for_allocated_nnodes count
# Wait (up to 30s) for the current housekeeping entry's remaining
# allocated node count to reach $1.  Copied from t1026-rv1-partial-release.t.
# Used here as the fragmentation proof: catching this at nnodes=1 for a
# 2-node job means one node's housekeeping has already completed and been
# released to the scheduler while the other has not -- i.e. the release
# arrived (or is arriving) as separate per-node fragments, not a single
# free covering the whole job.
hk_wait_for_allocated_nnodes () {
	count=0
	while test $(flux housekeeping list -no {allocated.nnodes}) -ne $1; do
		count=$(($count+1));
		test $count -eq 300 && return 1 # max 300 * 0.1s sleep = 30s
		sleep 0.1
	done
}

# Usage: fluxion_allocated ncores|nnodes
fluxion_allocated () {
	FLUX_RESOURCE_LIST_RPC=sched.resource-status \
		flux resource list -s allocated -no {$1}
}

# The multi-node rabbit jobspec: 2 exclusive node+core slots plus one
# exclusive ssd share, all confined to a single chassis (mirrors the
# jobspec shapes used by t3044-resource-rabbit-cancel.t and
# data/resource/jobspecs/advanced/rabbit.yaml, but widened to 2 nodes so
# that housekeeping must release 2 distinct broker ranks).  With
# match-policy=first on rabbit.json this selects hetchy1017 and hetchy1018
# (chassis1), the same pair t3044 exercises at the resource-query level.
cat >job.json <<'EOF'
{
    "attributes": {"system": {"duration": 60}},
    "resources": [
        {"count": 1, "type": "chassis", "with": [
            {"count": 2, "exclusive": true, "type": "node", "with": [
                {"count": 1, "label": "task", "type": "slot", "with": [
                    {"count": 1, "type": "core"}
                ]}
            ]},
            {"count": 1, "exclusive": true, "type": "ssd"}
        ]}
    ],
    "tasks": [{"command": ["true"], "count": {"per_slot": 1}, "slot": "task"}],
    "version": 1
}
EOF

# Control jobspec: the same shape but a single exclusive node+core slot, so
# housekeeping only ever has one rank to release -- a single-fragment
# (non-fragmented) release, used as a clean-release control.
cat >control.json <<'EOF'
{
    "attributes": {"system": {"duration": 60}},
    "resources": [
        {"count": 1, "type": "chassis", "with": [
            {"count": 1, "exclusive": true, "type": "node", "with": [
                {"count": 1, "label": "task", "type": "slot", "with": [
                    {"count": 1, "type": "core"}
                ]}
            ]},
            {"count": 1, "exclusive": true, "type": "ssd"}
        ]}
    ],
    "tasks": [{"command": ["true"], "count": {"per_slot": 1}, "slot": "task"}],
    "version": 1
}
EOF

test_expect_success 'configure flux with rabbit JGF, first policy, and staggered housekeeping' '
	flux config load <<-EOF &&
	[job-manager.housekeeping]
	command = [
	    "sh",
	    "-c",
	    "test \$(( \$(flux getattr rank) % 2 )) -eq 0 && exit 0 || sleep 2"
	]
	release-after = "0s"

	[sched-fluxion-resource]
	match-policy = "first"

	[resource]
	noverify = true
	norestrict = true
	scheduling = "${cluster_jgf}"

	[[resource.config]]
	hosts = "${HOSTLIST}"
	cores = "0-1"
	EOF
	flux config get job-manager.housekeeping
'
# The housekeeping command staggers completion by the *parity* of the
# broker rank running it (even ranks return immediately, odd ranks sleep
# 2s) rather than hardcoding specific rank numbers: whichever two adjacent
# node ranks fluxion selects for the 2-node jobspec above, they differ in
# parity, so one always finishes housekeeping ~2s after the other. This is
# what forces the multi-node job's release into two separate per-rank
# fragments instead of one, exactly as flux-core housekeeping's
# release-after=0s ("released as each target completes") does in
# production when nodes finish their epilog work at different times.

test_expect_success 'load fluxion modules with the rabbit JGF' '
	flux module remove -f sched-simple &&
	flux module remove -f sched-fluxion-qmanager &&
	flux module remove -f sched-fluxion-resource &&
	flux module reload resource &&
	flux module load sched-fluxion-resource &&
	flux module load sched-fluxion-qmanager &&
	test_debug flux module list &&
	flux resource list
'

# Check job manager hello debug message for +partial-ok flag. Without this,
# flux-core would not support the incremental per-node release that
# fluxion's partial-cancel path (and thus the fix under test) depends on.
if flux dmesg | grep -q +partial-ok; then
    test_set_prereq HAVE_PARTIAL_OK
fi

#
# Leak test: a 2-node rabbit job whose release fragments across 2 ranks.
#

test_expect_success HAVE_PARTIAL_OK 'submit multi-node rabbit job and let it complete' '
	jobid=$(flux job submit job.json) &&
	echo ${jobid} >jobid.multi &&
	flux job wait-event -vt10 ${jobid} alloc &&
	flux job info ${jobid} R | tee R.multi.json | jq -e ".execution.R_lite[0].rank | contains(\"-\")" &&
	flux job wait-event -vt15 ${jobid} finish
'

test_expect_success HAVE_PARTIAL_OK 'the release is fragmented: one rank still in housekeeping while the other has already been freed' '
	jobid=$(cat jobid.multi) &&
	hk_wait_for_allocated_nnodes 1 &&
	test $(fluxion_allocated nnodes) -eq 1
'

test_expect_success HAVE_PARTIAL_OK 'housekeeping fully drains and the job goes inactive' '
	jobid=$(cat jobid.multi) &&
	hk_wait_for_running 0 &&
	flux job wait-event -vt10 ${jobid} clean
'

test_expect_success HAVE_PARTIAL_OK 'no vertices remain allocated or tagged to the retired multi-node job' '
	jobid=$(cat jobid.multi) &&
	flux ion-resource find -q --format=jgf jobid-alloc=${jobid} >alloc_multi.json &&
	test_debug "cat alloc_multi.json" &&
	jq -e ". == null" alloc_multi.json &&
	flux ion-resource find -q --format=jgf jobid-tag=${jobid} >tag_multi.json &&
	test_debug "cat tag_multi.json" &&
	jq -e ". == null" tag_multi.json
'

test_expect_success HAVE_PARTIAL_OK 'sched-now=allocated shows nothing allocated (no leaked ssd/rabbit/chassis vertex)' '
	flux ion-resource find -q --format=jgf sched-now=allocated >sched_now_multi.json &&
	test_debug "cat sched_now_multi.json" &&
	jq -e ". == null" sched_now_multi.json
'

test_expect_success HAVE_PARTIAL_OK 'capacity assertion: the same rabbit job can be re-allocated (not fossilized)' '
	jobid2=$(flux job submit job.json) &&
	echo ${jobid2} >jobid.multi2 &&
	flux job wait-event -vt15 ${jobid2} alloc
'

test_expect_success HAVE_PARTIAL_OK 'clean up the second multi-node job' '
	jobid2=$(cat jobid.multi2) &&
	flux job wait-event -vt15 ${jobid2} finish &&
	hk_wait_for_running 0 &&
	flux job wait-event -vt10 ${jobid2} clean &&
	flux ion-resource find -q --format=jgf jobid-alloc=${jobid2} >alloc_multi2.json &&
	jq -e ". == null" alloc_multi2.json
'

#
# Control: a single-node rabbit job whose release is necessarily a single
# fragment (housekeeping only ever has one rank to release).  Must also
# end clean; this guards against the fix papering over a real leak by
# coincidentally only checking the multi-fragment case.
#

test_expect_success HAVE_PARTIAL_OK 'submit single-node (control) rabbit job and let it complete' '
	jobid3=$(flux job submit control.json) &&
	echo ${jobid3} >jobid.single &&
	flux job wait-event -vt10 ${jobid3} alloc &&
	flux job info ${jobid3} R | tee R.single.json | jq -e ".execution.R_lite | length == 1" &&
	flux job wait-event -vt15 ${jobid3} finish &&
	hk_wait_for_running 0 &&
	flux job wait-event -vt10 ${jobid3} clean
'

test_expect_success HAVE_PARTIAL_OK 'no vertices remain allocated or tagged to the retired single-node job' '
	jobid3=$(cat jobid.single) &&
	flux ion-resource find -q --format=jgf jobid-alloc=${jobid3} >alloc_single.json &&
	test_debug "cat alloc_single.json" &&
	jq -e ". == null" alloc_single.json &&
	flux ion-resource find -q --format=jgf jobid-tag=${jobid3} >tag_single.json &&
	test_debug "cat tag_single.json" &&
	jq -e ". == null" tag_single.json &&
	flux ion-resource find -q --format=jgf sched-now=allocated >sched_now_single.json &&
	jq -e ". == null" sched_now_single.json
'

test_expect_success 'remove manually loaded modules' '
	remove_qmanager &&
	remove_resource
'

test_done
