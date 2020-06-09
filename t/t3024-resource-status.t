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

test001_desc="get resource vertex status "
test_expect_success "${test001_desc}" '
    ${query} -L ${grugs} -F jgf -S CA -P high < ${cmd_dir}/cmds01 \
> 001.R.out &&
    test_cmp 001.R.out ${exp_dir}/001.R.out
'

test002_desc="change resource vertex status and get output "
test_expect_success "${test002_desc}" '
    ${query} -L ${grugs} -F jgf -S CA -P high < ${cmd_dir}/cmds02 \
> 002.R.out &&
    test_cmp 002.R.out ${exp_dir}/002.R.out
'

test_done
