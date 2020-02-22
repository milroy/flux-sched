#!/bin/sh

test_description='Test Elastic Jobs on Tiny Machine Configuration'

. $(dirname $0)/sharness.sh

unit_job="${SHARNESS_TEST_SRCDIR}/data/resource/jobspecs/elastic/test001.yaml"
job2="${SHARNESS_TEST_SRCDIR}/data/resource/jobspecs/elastic/test002.yaml"
job3="${SHARNESS_TEST_SRCDIR}/data/resource/jobspecs/elastic/test003.yaml"
job4="${SHARNESS_TEST_SRCDIR}/data/resource/jobspecs/elastic/test004.yaml"
exp_dir="${SHARNESS_TEST_SRCDIR}/data/resource/expected/elastic"
grugs="${SHARNESS_TEST_SRCDIR}/data/resource/grugs/tiny.graphml"
query="../../resource/utilities/resource-query"

test001_desc="can allocate four rigid jobs after all resources \
allocated by elastic jobs"
test_expect_success "${test001_desc}" '
    cat >cmds001 <<-EOF &&
	match allocate ${unit_job}
	match allocate ${unit_job}
	match allocate ${unit_job}
	match allocate ${unit_job}
	match allocate ${job2}
	match allocate ${job2}
	match allocate ${job2}
	match allocate ${job2}
	quit
EOF
    ${query} -L ${grugs} -S CA -P high -F jgf -t 001.R.out < cmds001 &&
    test_cmp 001.R.out ${exp_dir}/001.R.out
'

test002_desc="can't reserve elastic jobs after all resources allocated \
by elastic jobs"
test_expect_success "${test002_desc}" '
    cat >cmds002 <<-EOF &&
	match allocate_orelse_reserve ${unit_job}
	match allocate_orelse_reserve ${unit_job}
	match allocate_orelse_reserve ${unit_job}
	match allocate_orelse_reserve ${unit_job}
	match allocate_orelse_reserve ${unit_job}
	quit
EOF
    ${query} -L ${grugs} -S CA -P high -F jgf -t 002.R.out < cmds002 &&
    test_cmp 002.R.out ${exp_dir}/002.R.out
'

test003_desc="can't match allocate with satisfiability for \
elastic jobs"
test_expect_success "${test003_desc}" '
    cat >cmds003 <<-EOF &&
	match allocate_with_satisfiability ${unit_job}
	quit
EOF
    ${query} -L ${grugs} -S CA -P high -F jgf -t 003.R.out < cmds003 &&
    test_cmp 003.R.out ${exp_dir}/003.R.out
'

test004_desc="cancel works with elastic jobs"
test_expect_success "${test004_desc}" '
    cat >cmds004 <<-EOF &&
	match allocate_orelse_reserve ${unit_job}
	match allocate_orelse_reserve ${unit_job}
	match allocate_orelse_reserve ${unit_job}
	match allocate_orelse_reserve ${unit_job}
	match allocate_orelse_reserve ${unit_job}
	cancel 4
	match allocate_orelse_reserve ${unit_job}
	match allocate_orelse_reserve ${unit_job}
	quit
EOF
    ${query} -L ${grugs} -S CA -P high -F jgf -t 004.R.out < cmds004 &&
    test_cmp 004.R.out ${exp_dir}/004.R.out
'

test005_desc="update on already allocated elastic resource sets must succeed"
test_expect_success "${test005_desc}" '
    cat >cmds005 <<-EOF &&
	match allocate ${unit_job}
	match allocate ${unit_job}
	match allocate ${unit_job}
	match allocate ${unit_job}
	quit
EOF
    ${query} -L ${grugs} -S CA -P high -F jgf -t 005.R.out < cmds005 &&
    cat 005.R.out | grep -v INFO | \
split -l 1 --additional-suffix=".json" - cmds005_ &&
    cat >upd_cmds005 <<-EOF &&
	match allocate ${unit_job}
	match allocate ${unit_job}
	match allocate ${unit_job}
	match allocate ${unit_job}
	update allocate cmds005_aa.json 5 0 3600
	update allocate cmds005_ab.json 6 0 3600
	update allocate cmds005_ac.json 7 0 3600
	update allocate cmds005_ad.json 8 0 3600
	quit
EOF
    ${query} -L ${grugs} -S CA -P high -F jgf -t 005.R.out2 < upd_cmds005 &&
    cat 005.R.out | grep -v INFO > 005.R.out.filtered &&
    cat 005.R.out2 | grep -v INFO > 005.R.out2.filtered &&
    test_cmp 005.R.out.filtered ${exp_dir}/005.R.out.filtered
    test_cmp 005.R.out2.filtered ${exp_dir}/005.R.out2.filtered
'

test006_desc="update on partially allocated elastic resources must succeed"
test_expect_success "${test006_desc}" '
    cat >cmds006 <<-EOF &&
	match allocate ${unit_job}
	match allocate ${unit_job}
	match allocate ${unit_job}
	match allocate ${unit_job}
	quit
EOF
    ${query} -L ${grugs} -S CA -P high -F jgf -t 006.R.out < cmds006 &&
    cat >cmds006.2 <<-EOF &&
	match allocate ${job2}
	match allocate ${unit_job}
	match allocate ${unit_job}
	quit
EOF
    ${query} -L ${grugs} -S CA -P high -F jgf -t 006.2.R.out < cmds006.2 &&
    cat 006.2.R.out | grep -v INFO | \
split -l 1 --additional-suffix=".json" - cmds006_ &&
    cat >upd_cmds006 <<-EOF &&
	match allocate ${unit_job}
	update allocate cmds006_aa.json 2 0 3600
	match allocate ${unit_job}
	match allocate ${unit_job}
	match allocate ${unit_job}
	quit
EOF
    ${query} -L ${grugs} -S CA -P high -F jgf -t 006.R.out2 < upd_cmds006 &&
    cat 006.R.out | grep -v INFO > 006.R.out.filtered &&
    cat 006.R.out2 | grep -v INFO > 006.R.out2.filtered &&
    test_cmp 006.R.out.filtered ${exp_dir}/006.R.out.filtered &&
    test_cmp 006.R.out2.filtered ${exp_dir}/006.R.out2.filtered
'

test007_desc="update-allocate work for complex resource shape (cpu+gpu+memory)"
test_expect_success "${test007_desc}" '
    cat >cmds007 <<-EOF &&
        match allocate ${job3}
        match allocate ${job3}
        quit
EOF
    ${query} -L ${grugs} -S CA -P high -F jgf -t 007.R.out < cmds007 &&
    cat 007.R.out | grep -v INFO | \
split -l 1 --additional-suffix=".json" - cmds007_ &&
    cat >upd_cmds007 <<-EOF &&
        update allocate cmds007_aa.json 1 0 3600
        update allocate cmds007_ab.json 2 0 3600
        quit
EOF
    ${query} -L ${grugs} -S CA -P high -F jgf -t 007.R.out2 < upd_cmds007 &&
    test_cmp 007.R.out2 007.R.out
'

test008_desc="satisfiability works with a 1-node, 1-socket elastic jobspec"
test_expect_success "${test008_desc}" '
    flux module load resource load-file=${grug} load-format=grug \
prune-filters=ALL:core subsystems=containment policy=high
    flux resource match allocate_with_satisfiability ${unit_job} &&
    flux resource match allocate_with_satisfiability ${unit_job} &&
    flux resource match allocate_with_satisfiability ${unit_job} &&
    flux resource match allocate_with_satisfiability ${unit_job}
'

test009_desc="satisfiability returns EBUSY when no available elastic resources"
test_expect_success  "${test009_desc}" '
    test_expect_code 16 flux resource \
match allocate_with_satisfiability ${unit_job} &&
    test_expect_code 16 flux resource \
match allocate_with_satisfiability ${unit_job} &&
    test_expect_code 16 flux resource \
match allocate_with_satisfiability ${unit_job} &&
    test_expect_code 16 flux resource \
match allocate_with_satisfiability ${unit_job}
'

test010_desc="satisfiability returns ENODEV on unsatisfiable elastic jobspec"
test_expect_success  "${test010_desc}" '
    test_expect_code 19 flux resource \
match allocate_with_satisfiability ${jobspec2} &&
    test_expect_code 19 flux resource \
match allocate_with_satisfiability ${jobspec2} &&
    test_expect_code 19 flux resource \
match allocate_with_satisfiability ${jobspec2} &&
    test_expect_code 19 flux resource \
match allocate_with_satisfiability ${jobspec2} &&
    flux module remove resource
'


test_done


