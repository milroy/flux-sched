/*****************************************************************************\
 * Copyright 2025 Lawrence Livermore National Security, LLC
 * (c.f. AUTHORS, NOTICE.LLNS, LICENSE)
 *
 * This file is part of the Flux resource manager framework.
 * For details, see https://github.com/flux-framework.
 *
 * SPDX-License-Identifier: LGPL-3.0
\*****************************************************************************/

extern "C" {
#if HAVE_CONFIG_H
# include <config.h>
#endif
}
#include <cstdlib>
#include <sstream>
#include <string>
#include <cstring>
#include <cstdint>
#include <cerrno>
#include <vector>
#include <map>
#include "resource/planner/c++/planner2.hpp"
#include "src/common/libtap/tap.h"
#include <iostream>

#include <sys/time.h>

static double get_elapse_time (timeval &st, timeval &et)
{
    double ts1 = (double)st.tv_sec + (double)st.tv_usec / 1000000.0f;
    double ts2 = (double)et.tv_sec + (double)et.tv_usec / 1000000.0f;
    return ts2 - ts1;
}

static void to_stream (int64_t base_time, uint64_t duration, const uint64_t *cnts,
                      const char **types, size_t len, std::stringstream &ss)
{
    if (base_time != -1)
        ss << "B(" << base_time << "):";

    ss << "D(" << duration << "):" << "R(<";
    for (unsigned int i = 0; i < len; ++i)
        ss << types[i] << "(" << cnts[i] << ")";

    ss << ">)";
}

static int test_add_remove ()
{
    size_t len = 5;
    size_t size = 0;
    int rc = -1;
    int64_t tmax = INT64_MAX;
    int64_t span1 = -1, span2 = -1, span3 = -1;
    const uint64_t resource_totals[] = {10, 20, 30, 40, 50};
    const uint64_t request1[] = {1, 0, 0, 0, 0};
    const uint64_t request2[] = {0, 2, 0, 0, 0};
    const uint64_t request3[] = {0, 0, 3, 0, 0};
    planner2 *plan2 = nullptr;
    std::stringstream ss;
    uint64_t n_res = 64;

    plan2 = new planner2 (n_res, "core", 0, tmax);
    plan2->add_span (0, 10, 64);
    plan2->add_span (12, 16, 10);
    plan2->add_span (10, 16, 48);
    plan2->add_span (12, 16, 10);
    std::cout << "planner2 size: " << plan2->m_multi_container.size () << "\n";

    std::cout << "Time iteration order\n";
    auto &time_it = plan2->m_multi_container.get<at_time> ();
    for (auto it : time_it) {
        std::cout << "time: " << it.at_time << " occupied: " << n_res - it.free_ct << " free resources: " << it.free_ct << "\n";
    }

    // std::cout << "Free resources iteration order\n";
    // auto &occ_it = plan2->m_multi_container.get<free_ct> ();
    // for (auto it : occ_it) {
    //     std::cout << "time: " << it.at_time << " occupied: " << n_res - it.free_ct << " free resources: " << it.free_ct << "\n";
    // }

    auto range = plan2->m_multi_container.get<at_time> ().range (
    [](uint64_t t){return t>=5;},  // "left" condition
    [](uint64_t t){return t<=60;}); // "right" condition
    std::cout << "Earliest at time iteration range\n";
    for (;range.first != range.second; ++range.first)
       std::cout << "time: " << range.first->at_time << " occupied: " << n_res - range.first->free_ct << " free resources: " << range.first->free_ct << "\n";

    std::cout << "Earliest time with free resources greater than or equal to example\n";
    auto mc = plan2->m_multi_container.get<time_count> ().lower_bound (6);
    std::cout << "time: " << mc->at_time << " occupied: " << n_res - mc->free_ct << " free resources: " << mc->free_ct << " End? " << (mc == plan2->m_multi_container.get<time_count> ().end ()) <<  "\n";

    auto mc2 = plan2->m_multi_container.get<time_count> ().lower_bound (boost::make_tuple (0, 6), earliest_free ());
    std::cout << "time: " << mc2->at_time << " occupied: " << n_res - mc2->free_ct << " free resources: " << mc2->free_ct << " End? " << (mc2 == plan2->m_multi_container.get<time_count> ().end ()) <<  "\n";

    std::cout << "Multi_index span size: " << plan2->m_span_lookup.size () << std::endl;
    std::cout << "Multi_index container size: " << plan2->m_multi_container.size () << std::endl;

    return 0;

}

static int stress_add ()
{
    int i = 0;
    bool rc = false;
    int64_t span;
    bool bo = false;
    uint64_t resource_total = 10000000;
    char resource_type[] = "hardware-thread";
    uint64_t counts100 = 100;
    planner2 *plan2 = nullptr;
    struct timeval st, et;

    plan2 = new planner2 (resource_total, "hardware-thread", 0, INT64_MAX);
    gettimeofday (&st, NULL);
    for (i = 0; i < 100000; ++i) {
        rc = plan2->avail_during (i, 4, counts100);
        bo = (bo || rc == false);
        span = plan2->add_span (i, 4, counts100);
        bo = (bo || span == -1);
    }

    //ok (!bo, "add_span 100000 times (4 spans overlap)");

    for (i = 100000; i < 200000; ++i) {
        rc = plan2->avail_during (i, 4, counts100);
        bo = (bo || rc == false);
        span = plan2->add_span (i, 4, counts100);
        bo = (bo || span == -1);
    }
    gettimeofday (&et, NULL);
    std::cout << "Time taken by code block: " << get_elapse_time (st, et) << " microseconds" << std::endl;
    ok (!bo, "add_span 100000 more (4 spans overlap)");
    std::cout << "Multi_index span size: " << plan2->m_span_lookup.size () << std::endl;
    std::cout << "Multi_index container size: " << plan2->m_multi_container.size () << std::endl;

    return 0;
}

int main (int argc, char *argv[])
{
    plan (1);

    test_add_remove ();

    stress_add ();

    done_testing ();

    return EXIT_SUCCESS;
}

/*
 * vi: ts=4 sw=4 expandtab
 */
