#!/bin/sh

test_description='test rank-less (rabbit ssd) partial cancel does not leak'

. $(dirname $0)/sharness.sh

cmd_dir="${SHARNESS_TEST_SRCDIR}/data/resource/commands/rabbit_cancel"
exp_dir="${SHARNESS_TEST_SRCDIR}/data/resource/expected/rabbit_cancel"
rabbit_jgf="${SHARNESS_TEST_SRCDIR}/data/resource/jgfs/rabbit.json"
rabbit_nested_jgf="${SHARNESS_TEST_SRCDIR}/data/resource/jgfs/rabbit-nested.json"
query="../../resource/utilities/resource-query"

#
# Background: vertices that are never named in a freed R fragment --
# rank-less rabbit ssd vertices, and ranked vertices such as rabbits whose
# rank never appears in a .free (only compute node ranks do) -- used to
# leak their per-job allocation/tag state when a job holding them was
# released via an rv1exec partial-cancel followed by a full cancel.
# dfu_impl_t::sweep_job_state() now releases all remaining jobid state at
# job retirement, both after a full cancel and when a partial cancel
# itself reports full_cancel=true; rem_exclusive_filter is also now
# gated on full release in partial cancel.  Every test below allocates,
# releases and then verifies with "find" that no vertex is left tagged or
# allocated to the released jobid(s), and that the freed capacity can be
# re-allocated.
#

#
# Case (a): allocate, partial-cancel the job's two node ranks in a single
# rv1exec fragment, then fully cancel the job.  Run under four match
# policies that all select the same chassis1 nodes (hetchy1017/hetchy1018)
# for this jobspec: first (the default), high, hinodex and firstnodex.
#

cmds001="${cmd_dir}/cmds01.in"
test001_desc="partial-cancel + cancel of rabbit ssd job does not leak (pol=first)"
test_expect_success "${test001_desc}" '
    sed "s~@TEST_SRCDIR@~${SHARNESS_TEST_SRCDIR}~g" ${cmds001} > cmds001 &&
    ${query} -f jgf -L ${rabbit_jgf} -S CA -P first -t 001.R.out < cmds001 &&
    test_cmp 001.R.out ${exp_dir}/001.R.out
'

cmds002="${cmd_dir}/cmds01.in"
test002_desc="partial-cancel + cancel of rabbit ssd job does not leak (pol=high)"
test_expect_success "${test002_desc}" '
    sed "s~@TEST_SRCDIR@~${SHARNESS_TEST_SRCDIR}~g" ${cmds002} > cmds002 &&
    ${query} -f jgf -L ${rabbit_jgf} -S CA -P high -t 002.R.out < cmds002 &&
    test_cmp 002.R.out ${exp_dir}/002.R.out
'

cmds003="${cmd_dir}/cmds01.in"
test003_desc="partial-cancel + cancel of rabbit ssd job does not leak (pol=hinodex)"
test_expect_success "${test003_desc}" '
    sed "s~@TEST_SRCDIR@~${SHARNESS_TEST_SRCDIR}~g" ${cmds003} > cmds003 &&
    ${query} -f jgf -L ${rabbit_jgf} -S CA -P hinodex -t 003.R.out < cmds003 &&
    test_cmp 003.R.out ${exp_dir}/003.R.out
'

cmds004="${cmd_dir}/cmds01.in"
test004_desc="partial-cancel + cancel of rabbit ssd job does not leak (pol=firstnodex)"
test_expect_success "${test004_desc}" '
    sed "s~@TEST_SRCDIR@~${SHARNESS_TEST_SRCDIR}~g" ${cmds004} > cmds004 &&
    ${query} -f jgf -L ${rabbit_jgf} -S CA -P firstnodex -t 004.R.out < cmds004 &&
    test_cmp 004.R.out ${exp_dir}/004.R.out
'

# Same as case (a), but with all pruning filters set (ALL:core,ALL:node,
# ALL:ssd) so the ssd resource is also agfilter-tracked at every high-level
# vertex; the sweep must still release the ssd's jobid state.

cmds005="${cmd_dir}/cmds01.in"
test005_desc="partial-cancel + cancel of rabbit ssd job does not leak (all prune filters incl. ssd)"
test_expect_success "${test005_desc}" '
    sed "s~@TEST_SRCDIR@~${SHARNESS_TEST_SRCDIR}~g" ${cmds005} > cmds005 &&
    ${query} -f jgf -L ${rabbit_jgf} -S CA -P first --prune-filters=ALL:core,ALL:node,ALL:ssd -t 005.R.out < cmds005 &&
    test_cmp 005.R.out ${exp_dir}/005.R.out
'

#
# Case (b): same as (a), but the two node ranks are partial-cancelled in
# two separate rv1exec fragments (rank 17, then rank 18) before the job is
# fully cancelled.
#

cmds006="${cmd_dir}/cmds02.in"
test006_desc="multi-fragment partial-cancel + cancel of rabbit ssd job does not leak"
test_expect_success "${test006_desc}" '
    sed "s~@TEST_SRCDIR@~${SHARNESS_TEST_SRCDIR}~g" ${cmds006} > cmds006 &&
    ${query} -f jgf -L ${rabbit_jgf} -S CA -P first -t 006.R.out < cmds006 &&
    test_cmp 006.R.out ${exp_dir}/006.R.out
'

#
# Case (c): ratchet/outage coverage.  Two jobs each take one chassis'
# worth of rabbit ssd capacity; both are released (partial-cancel then
# cancel).  Before the fix, the entire rabbit capacity of both chassis
# would be fossilized and neither of two subsequent jobs could allocate.
#

cmds007="${cmd_dir}/cmds03.in"
test007_desc="releasing two jobs across both chassis does not fossilize rabbit capacity"
test_expect_success "${test007_desc}" '
    sed "s~@TEST_SRCDIR@~${SHARNESS_TEST_SRCDIR}~g" ${cmds007} > cmds007 &&
    ${query} -f jgf -L ${rabbit_jgf} -S CA -P first -t 007.R.out < cmds007 &&
    test_cmp 007.R.out ${exp_dir}/007.R.out
'

#
# Case (d): same flow as (a), but the ssd share is requested without
# "exclusive: true" (non-exclusive/tagged ssd allocation rather than a
# fully exclusive one).
#

cmds008="${cmd_dir}/cmds04.in"
test008_desc="partial-cancel + cancel of non-exclusive rabbit ssd job does not leak"
test_expect_success "${test008_desc}" '
    sed "s~@TEST_SRCDIR@~${SHARNESS_TEST_SRCDIR}~g" ${cmds008} > cmds008 &&
    ${query} -f jgf -L ${rabbit_jgf} -S CA -P first -t 008.R.out < cmds008 &&
    test_cmp 008.R.out ${exp_dir}/008.R.out
'

#
# Case (e): a single rv1exec partial-cancel names *all* of the ranks the
# job holds (both hetchy1017 and hetchy1018) in one fragment, with no
# separate "cancel" call.  Note this does *not* cause an automatic full
# retirement of the job: full_cancel only becomes true when the partial
# cancel's own resource accounting equals the job's entire allocation, and
# the rank-less ssd share (which cannot be named in an R_lite fragment)
# always remains outstanding, so the job stays ALLOCATED and its ssd stays
# tagged/allocated.  This was verified both with the default prune filters
# and with --prune-filters=ALL:core,ALL:node,ALL:ssd -- neither causes
# full_cancel to fire from the partial-cancel alone. An explicit "cancel"
# is therefore still required to retire the job and release its rank-less
# ssd vertices, which is what this test checks.
#

cmds009="${cmd_dir}/cmds05.in"
test009_desc="partial-cancel naming all ranks needs an explicit cancel to fully release rabbit ssd"
test_expect_success "${test009_desc}" '
    sed "s~@TEST_SRCDIR@~${SHARNESS_TEST_SRCDIR}~g" ${cmds009} > cmds009 &&
    ${query} -f jgf -L ${rabbit_jgf} -S CA -P first -t 009.R.out < cmds009 &&
    test_cmp 009.R.out ${exp_dir}/009.R.out
'

#
# Case (f): nested topology (chassis -> rabbit(ranked) -> ssd(rank-less))
# instead of rabbit.json's flat (chassis -> ssd(rank-less)) layout.  The
# rabbit vertex itself carries a rank (19/20), but that rank is never
# named in a production .free fragment, so like the rank-less ssds below
# it the rabbit's job state can only be released by the retirement sweep.
# The fragment here deliberately names only the compute node ranks, as
# flux-core housekeeping would.
#

cmds010="${cmd_dir}/cmds06.in"
test010_desc="partial-cancel + cancel does not leak rabbit ssd in nested chassis/rabbit/ssd topology"
test_expect_success "${test010_desc}" '
    sed "s~@TEST_SRCDIR@~${SHARNESS_TEST_SRCDIR}~g" ${cmds010} > cmds010 &&
    ${query} -f jgf -L ${rabbit_nested_jgf} -S CA -P first -t 010.R.out < cmds010 &&
    test_cmp 010.R.out ${exp_dir}/010.R.out
'

test_done
