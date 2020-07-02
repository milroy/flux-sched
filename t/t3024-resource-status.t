#!/bin/sh

test_description='Test Resource Status On Tiny Machine Configuration'

. $(dirname $0)/sharness.sh

cmd_dir="${SHARNESS_TEST_SRCDIR}/data/resource/commands/status"
exp_dir="${SHARNESS_TEST_SRCDIR}/data/resource/expected/status"
grugs="${SHARNESS_TEST_SRCDIR}/data/resource/grugs/tiny.graphml"
query="../../resource/utilities/resource-query"

#
# Selection Policy -- High ID first (-P high)
#     The resource vertex with higher ID is preferred among its kind
#     (e.g., node1 is preferred over node0 if available)
#

cmds001="${cmd_dir}/cmds01"
test001_desc="dump subgraph with any resource status "
test_expect_success "${test001_desc}" '
    ${query} -L ${grugs} -F jgf -S CA -P high < cmds001 &&
    test_cmp 001.R.out ${exp_dir}/001.R.out
'

cmds002="${cmd_dir}/cmds02"
test002_desc="dump subgraph with down resource status "
test_expect_success "${test002_desc}" '
    ${query} -L ${grugs} -F jgf -S CA -P high < cmds002 &&
    test_cmp 002.R.out ${exp_dir}/002.R.out
'

cmds003="${cmd_dir}/cmds03"
test003_desc="dump subgraph with up resource status "
test_expect_success "${test003_desc}" '
    ${query} -L ${grugs} -F jgf -S CA -P high < cmds003 &&
    test_cmp 003.R.out ${exp_dir}/003.R.out
'

cmds004="${cmd_dir}/cmds04"
test004_desc="dump subgraph with down resource status after setting node0 down"
test_expect_success "${test004_desc}" '
    ${query} -L ${grugs} -F jgf -S CA -P high < cmds004 &&
    test_cmp 004.R.out ${exp_dir}/004.R.out
'

cmds005="${cmd_dir}/cmds05"
test005_desc="dump subgraph with up resource status after setting node0 down"
test_expect_success "${test005_desc}" '
    ${query} -L ${grugs} -F jgf -S CA -P high < cmds005 &&
    test_cmp 005.R.out ${exp_dir}/005.R.out
'

test_done
