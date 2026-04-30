#!/bin/sh

test_description='Test the idset load format'

. $(dirname $0)/sharness.sh

query="../../resource/utilities/resource-query"
jobspec="${SHARNESS_TEST_SRCDIR}/data/resource/jobspecs/load_format/t3041.yaml"

test_expect_success 'Generate rv1 with .scheduling key' '
    flux R encode --hosts=compute[1-20] --cores=0-15 --gpu=0-3 | jq . > R.json &&
    cat R.json | flux ion-R encode > R_JGF.json &&
    jq -S "del(.execution.starttime, .execution.expiration)" R.json > R.norm.json
'

test_expect_success 'create match command for testing' '
    cat > match_jobspec_cmd <<-EOF
    match allocate ${jobspec}
    quit
EOF
'

test_expect_success 'generate idset to use to update regular JGF' '
    jq -S .scheduling R_JGF.json > JGF.json &&
    ${query} -L JGF.json -f jgf -F idset -t idset1.out -P lonodex < match_jobspec_cmd &&
    head -n1 idset1.out | jq -S . > idset.json &&
    jq -e "type == \"array\"" idset.json &&
    jq -e "length > 0" idset.json
'

test_expect_success 'idset contains only vertex ID strings' '
    jq -e "all(.[]; type == \"string\")" idset.json
'

test_expect_success 'update graph to allocate a job using idset' '
    cat > update_allocate_cmd <<-EOF &&
    update allocate idset idset.json 0 0 5
    f sched-now=allocated
    quit
EOF
    jq -S .scheduling R_JGF.json > JGF.json &&
    ${query} -L JGF.json -f idset -F jgf -t jgf2.out -P high < update_allocate_cmd &&
    tail -n4 jgf2.out | head -n1 > allocated.json
'

test_expect_success 'JGF output shows all cores and gpus allocated' '
    grep core allocated.json > /dev/null &&
    grep gpu allocated.json > /dev/null &&
    grep node allocated.json > /dev/null &&
    grep compute1 allocated.json > /dev/null &&
    jq -e ".graph.nodes[] | select(.metadata.type == \"core\") | .id" allocated.json > cores.json &&
    test 320 -eq $(cat cores.json | wc -l) &&
    jq -e ".graph.nodes[] | select(.metadata.type == \"gpu\") | .id" allocated.json > gpus.json &&
    test 80 -eq $(cat gpus.json | wc -l)
'

test_done
