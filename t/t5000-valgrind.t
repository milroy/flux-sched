#!/bin/sh

test_description='Run broker under valgrind with a small workload'

# Append --logfile option if FLUX_TESTS_LOGFILE is set in environment:
test -n "$FLUX_TESTS_LOGFILE" && set -- "$@" --logfile
. `dirname $0`/sharness.sh

if ! which valgrind >/dev/null; then
    skip_all='skipping valgrind tests since no valgrind executable found'
    test_done
fi
if ! test_have_prereq NO_ASAN; then
    skip_all='skipping valgrind tests since AddressSanitizer is active'
    test_done
fi

# Do not run test by default unless valgrind/valgrind.h was found, since
#  this has been known to introduce false positives (#1097). However, allow
#  run to be forced on the cmdline with -d, --debug.
#
have_valgrind_h() {
    grep -q "^#define HAVE_VALGRIND 1" ${SHARNESS_BUILD_DIRECTORY}/config.h
}
if ! have_valgrind_h && test "$debug" = ""; then
    skip_all='skipping valgrind tests b/c valgrind.h not found. Use -d, --debug to force'
    test_done
fi

export FLUX_PMI_SINGLETON=1 # avoid finding leaks in slurm libpmi.so

VALGRIND=`which valgrind`
VALGRIND_SUPPRESSIONS=${SHARNESS_TEST_SRCDIR}/valgrind/valgrind.supp
VALGRIND_WORKLOAD=${SHARNESS_TEST_SRCDIR}/valgrind/valgrind-workload.sh

VALGRIND_NBROKERS=${VALGRIND_NBROKERS:-2}

test_expect_success \
  "valgrind reports no new errors on $VALGRIND_NBROKERS broker run" '
	run_timeout 400 \
	flux start -s ${VALGRIND_NBROKERS} \
		--killer-timeout=120 \
		--wrap=libtool,e,${VALGRIND} \
		--wrap=--tool=memcheck \
		--wrap=--leak-check=full \
		--wrap=--gen-suppressions=all \
		--wrap=--trace-children=no \
		--wrap=--child-silent-after-fork=yes \
		--wrap=--num-callers=30 \
		--wrap=--leak-resolution=med \
		--wrap=--error-exitcode=1 \
		--wrap=--suppressions=$VALGRIND_SUPPRESSIONS \
		 ${VALGRIND_WORKLOAD}
'
test_done
