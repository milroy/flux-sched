/*****************************************************************************\
 * Copyright 2014 Lawrence Livermore National Security, LLC
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

    plan2 = new planner2 (64, "core", 0, tmax);
    plan2->add_span (10, 16, 48);
    plan2->add_span (12, 16, 10);
    std::cout << "planner2 size: " << plan2->m_multi_container.size () << "\n";

    std::cout << "Time iteration order\n";
    auto &time_it = plan2->m_multi_container.get<at_time> ();
    for (auto it : time_it) {
        std::cout << "time: " << it.at_time << " occupied: " << it.occupied_ct << " free resources: " << it.free_ct << "\n";
    }

    std::cout << "Free resources iteration order\n";
    auto &occ_it = plan2->m_multi_container.get<free_count> ();
    for (auto it : occ_it) {
        std::cout << "time: " << it.at_time << " occupied: " << it.occupied_ct << " free resources: " << it.free_ct << "\n";
    }

    auto range = plan2->m_multi_container.get<at_time> ().range (
    [](uint64_t t){return t>=7;},  // "left" condition
    [](uint64_t t){return t<=10;}); // "right" condition
    std::cout << "Earliest at time iteration range\n";
    for (;range.first != range.second; ++range.first)
       std::cout << "time: " << range.first->at_time << " occupied: " << range.first->occupied_ct << " free resources: " << range.first->free_ct << "\n";

    auto range2 = plan2->m_multi_container.get<free_count> ().range (
    [](uint64_t c){return c >= 16;},  // "left" condition
    [](uint64_t c){return c <= 32;}); // "right" condition
    std::cout << "Free resources iteration range\n";
    for (;range2.first != range2.second; ++range2.first)
       std::cout << "time: " << range2.first->at_time << " occupied: " << range2.first->occupied_ct << " free resources: " << range2.first->free_ct << "\n";

    auto range3 = boost::make_iterator_range (
        plan2->m_multi_container.get<time_count> ().lower_bound (std::make_tuple (2, 32)),
        plan2->m_multi_container.get<time_count> ().upper_bound (10)
    );
    std::cout << "Free resources and time iteration range\n";
    for (auto mc : range3)
       std::cout << "time: " << mc.at_time << " occupied: " << mc.occupied_ct << " free resources: " << mc.free_ct << "\n";


    return 0;

}

int main (int argc, char *argv[])
{
    plan (1);

    test_add_remove ();

    done_testing ();

    return EXIT_SUCCESS;
}

/*
 * vi: ts=4 sw=4 expandtab
 */
