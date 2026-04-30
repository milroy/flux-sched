#!/bin/sh

test_description='Test the idset match format'

. $(dirname $0)/sharness.sh

jobspec_dir="${SHARNESS_TEST_SRCDIR}/data/resource/jobspecs/match_format"
exp_dir="${SHARNESS_TEST_SRCDIR}/data/resource/expected/match_format"
query="../../resource/utilities/resource-query"

test_expect_success 'Generate rv1 with .scheduling key' '
    flux R encode --hosts=compute[1-10] --cores=0-7 --gpu=0-1 | jq . > R.json &&
    cat R.json | flux ion-R encode > R_JGF.json &&
    jq -S "del(.execution.starttime, .execution.expiration)" R.json > R.norm.json
'

test_expect_success 'resource_query can be loaded with jgf' '
    cat > match_jobspec_cmd <<-EOF &&
    match allocate ${jobspec_dir}/t3040.yaml
    quit
EOF
    jq -S .scheduling R_JGF.json > JGF.json &&
    ${query} -L JGF.json -f jgf -F rv1_nosched -t R1.out -P high < match_jobspec_cmd &&
    head -n1 R1.out | jq -S "del(.execution.starttime, .execution.expiration)" > r_match.json &&
    test_cmp r_match.json R.norm.json
'

test_expect_success 'idset emits only vertex IDs with policy=lonodex' '
    ${query} -L JGF.json -f jgf -F idset -t idset1.out -P lonodex < match_jobspec_cmd &&
    head -n1 idset1.out | jq -S . > match1.json &&
    test_must_fail grep "metadata" match1.json > /dev/null &&
    test_must_fail grep "type" match1.json > /dev/null &&
    jq -e "type == \"array\"" match1.json &&
    jq -e "length > 0" match1.json
'

test_expect_success 'idset output contains only string vertex IDs' '
    jq -e "all(.[]; type == \"string\")" match1.json
'

test_expect_success 'idset emits all vertex IDs with policy=low' '
    ${query} -L JGF.json -f jgf -F idset -t idset2.out -P low < match_jobspec_cmd &&
    head -n1 idset2.out | jq -S . > match2.json &&
    jq -e "type == \"array\"" match2.json &&
    jq -e "length > 0" match2.json
'

test_expect_success 'idset output with policy=low has more IDs than lonodex' '
    match1_count=$(jq "length" match1.json) &&
    match2_count=$(jq "length" match2.json) &&
    test $match2_count -gt $match1_count
'

test_expect_success 'idset emits only non-exclusive vertex IDs with exclusive nodes' '
    sed "4i\    exclusive: true" ${jobspec_dir}/t3040.yaml > jobspec_exclusive.yaml &&
    cat > match_jobspec_exclusive_cmd <<-EOF &&
    match allocate jobspec_exclusive.yaml
    quit
EOF
    ${query} -L JGF.json -f jgf -F idset -t idset3.out -P high < match_jobspec_exclusive_cmd &&
    head -n1 idset3.out | jq -S . > match3.json &&
    jq -e "type == \"array\"" match3.json &&
    jq -e "length > 0" match3.json &&
    match3_count=$(jq "length" match3.json) &&
    test $match3_count -eq $match1_count
'

test_done
