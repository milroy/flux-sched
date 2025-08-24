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

static void to_stream (int64_t base_time,
                       uint64_t duration,
                       uint64_t cnts,
                       const char *type,
                       std::stringstream &ss)
{
    if (base_time != -1)
        ss << "B(" << base_time << "):";
    ss << "D(" << duration << "):"
       << "R_";
    ss << type << "(" << cnts << ")";
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
    planner2 *plan2 = nullptr, *plan3 = nullptr, *plan4 = nullptr;
    std::stringstream ss;
    uint64_t n_res = 64;

    plan2 = new planner2 (n_res, "core", 0, tmax);
    plan2->add_span (0, 10, 64);
    plan2->add_span (12, 16, 10);
    span3 = plan2->add_span (10, 16, 48);
    span2 = plan2->add_span (12, 16, 10);
    
    span1 = plan2->add_span (20, 6, 48);
    std::cout << "SPAN1: " << span1 << " SPAN2: " << span2 << "\n";
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

    auto mc2 = plan2->m_multi_container.get<time_count> ().upper_bound (at_free{6, 17}, earliest_free ());
    std::cout << "time: " << mc2->at_time << " occupied: " << n_res - mc2->free_ct << " free resources: " << mc2->free_ct << " End? " << (mc2 == plan2->m_multi_container.get<time_count> ().end ()) <<  "\n";

    std::cout << "Multi_index span size: " << plan2->m_span_lookup.size () << std::endl;
    std::cout << "Multi_index container size: " << plan2->m_multi_container.size () << std::endl;

    rc = plan2->remove_span (span3);

    std::cout << "Multi_index span size: " << plan2->m_span_lookup.size () << " rc: " << rc << std::endl;
    std::cout << "Multi_index container size: " << plan2->m_multi_container.size () << std::endl;

    std::cout << "Time iteration order after removal\n";
    auto &time_it2 = plan2->m_multi_container.get<at_time> ();
    for (auto it : time_it2) {
        std::cout << "time: " << it.at_time << " occupied: " << n_res - it.free_ct << " free resources: " << it.free_ct << "\n";
    }

    plan3 = new planner2 (n_res, "core", 0, tmax);
    plan3->m_multi_container.insert (time_point{0, 64, 1});
    plan3->m_multi_container.insert (time_point{10, 54, 1});
    uint64_t p = 9;
    uint64_t q = 12;
    auto range2 = plan3->m_multi_container.get<at_time> ().range (
    [&](uint64_t t){return t>0;},  // "left" condition
    [&](uint64_t t){return t<=q;}); // "right" condition
    std::cout << "LB TIME: " << range2.first->at_time << " occupied: " << n_res - range2.first->free_ct << " free resources: " << range2.first->free_ct << "\n";

    auto range3 = plan3->m_multi_container.get<at_time> ().range (boost::multi_index::unbounded, boost::lambda::_1<tmax); // "right" condition
    range3.second--;
    std::cout << "UB TIME: " << range3.second->at_time << " occupied: " << n_res - range3.second->free_ct << " free resources: " << range3.second->free_ct << "\n";

    plan4 = new planner2 (10, "core", 0, tmax);
    span3 = plan4->add_span (0, 10, 5);
    span3 = plan4->add_span (0, 10, 5);
    std::cout << "SPAN3: " << span3 << "\n";
    span3 = plan4->add_span (0, 10, 5);
    std::cout << "SPAN3: " << span3 << "\n";

    return 0;

}

static int test_basic_add_remove ()
{
    int rc;
    int64_t t;
    planner2 *plan2 = nullptr;
    std::stringstream ss;
    const char resource_type[] = "B";
    uint64_t resource_total = 1;
    uint64_t counts1 = resource_total;
    int64_t span1 = -1, span2 = -1, span3 = -1, span4 = -1, span5 = -1;

    to_stream (0, 10, resource_total, resource_type, ss);
    plan2 = new planner2 (resource_total, "hardware-thread", 0, 10);
    ok (plan2 != nullptr, "new with (%s)", ss.str ().c_str ());

    ss.str ("");
    to_stream (-1, 5, counts1, resource_type, ss);
    t = plan2->avail_time_first (0, 5, counts1);
    ok (t == 0, "first scheduled point is @%d for (%s)", t, ss.str ().c_str ());

    span1 = plan2->add_span (t, 5, counts1);
    ok (span1 != -1, "span1 added for (%s)", ss.str ().c_str ());

    ss.str ("");
    to_stream (-1, 2, counts1, resource_type, ss);
    t = plan2->avail_time_first (0, 2, counts1);
    ok (t == 5, "second point is @%d for (%s)", t, ss.str ().c_str ());

    span2 = plan2->add_span (t, 2, counts1);
    ok (span2 != -1, "span2 added for (%s)", ss.str ().c_str ());

    ss.str ("");
    to_stream (-1, 2, counts1, resource_type, ss);
    t = plan2->avail_time_first (0, 2, counts1);
    ok (t == 7, "third point is @%d for (%s)", t, ss.str ().c_str ());

    span3 = plan2->add_span (t, 2, counts1);
    ok (span3 != -1, "span3 added for (%s)", ss.str ().c_str ());

    ss.str ("");
    to_stream (-1, 2, counts1, resource_type, ss);
    t = plan2->avail_time_first (0, 2, counts1);
    ok (t == -1, "no scheduled point available for (%s)", ss.str ().c_str ());

    ss.str ("");
    to_stream (-1, 1, counts1, resource_type, ss);
    t = plan2->avail_time_first (0, 1, counts1);
    ok (t == 9, "fourth point is @%d for (%s)", t, ss.str ().c_str ());

    span4 = plan2->add_span (t, 1, counts1);
    ok (span4 != -1, "span4 added for (%s)", ss.str ().c_str ());

    t = plan2->m_span_lookup.find (span2)->second->start;
    ok (t == 5, "span_start_time returned %ju", (intmax_t)t);

    rc = plan2->remove_span (span2);
    ok (!rc, "span2 removed");

    rc = plan2->remove_span (span3);
    ok (!rc, "span3 removed");

    ss.str ("");
    to_stream (-1, 5, counts1, resource_type, ss);
    t = plan2->avail_time_first (0, 5, counts1);
    ok (t == -1, "no scheduled point available for (%s)", ss.str ().c_str ());

    ss.str ("");
    to_stream (-1, 4, counts1, resource_type, ss);
    t = plan2->avail_time_first (0, 4, counts1);
    ok (t == 5, "fifth point is @%d for (%s)", t, ss.str ().c_str ());

    span5 = plan2->add_span (t, 4, counts1);
    ok (span5 != -1, "span5 added for (%s)", ss.str ().c_str ());
    ss.str ("");

    return 0;
}

static int test_availability_checkers ()
{
    int rc;
    bool bo = false;
    int64_t t = -1;
    int64_t avail = -1, tmax = INT64_MAX;
    int64_t span1 = -1, span2 = -1, span3 = -1;
    uint64_t resource_total = 10;
    uint64_t counts1 = 1;
    uint64_t counts4 = 4;
    uint64_t counts5 = 5;
    uint64_t counts9 = 9;
    uint64_t counts10 = resource_total;
    const char resource_type[] = {"A"};
    planner2 *plan2 = nullptr;
    std::stringstream ss;

    to_stream (0, tmax, resource_total, resource_type, ss);
    plan2 = new planner2 (resource_total, resource_type, 0, tmax);
    ok (plan2 != nullptr, "new with (%s)", ss.str ().c_str ());

    ss.str ("");
    to_stream (-1, 5, counts10, resource_type, ss);
    rc = plan2->avail_during (0, 1, counts10);
    ok (rc, "avail check works (%s)", ss.str ().c_str ());

    ss.str ("");
    to_stream (-1, 1000, counts5, resource_type, ss);
    rc = plan2->avail_during (1, 1000, counts5);
    ok (rc, "avail check works (%s)", ss.str ().c_str ());

    span1 = plan2->add_span (1, 1000, counts5);
    ok ((span1 != -1), "span1 added for (%s)", ss.str ().c_str ());

    ss.str ("");
    to_stream (-1, 1000, counts10, resource_type, ss);
    rc = plan2->avail_during (2000, 1001, counts10);
    span2 = plan2->add_span (2000, 1001, counts10);
    ok ((span2 != -1), "span2 added for (%s)", ss.str ().c_str ());

    ss.str ("");
    to_stream (-1, 2990, counts1, resource_type, ss);
    rc = plan2->avail_during (10, 2991, counts1);
    ok (rc == 0, "over-alloc fails for (%s)", ss.str ().c_str ());

    ss.str ("");
    to_stream (-1, 1990, counts1, resource_type, ss);
    rc = plan2->avail_during (10, 1990, counts1);
    ok (rc, "overlapping works (%s)", ss.str ().c_str ());

    span3 = plan2->add_span (10, 1990, counts1);
    ok ((span3 != -1), "span3 added for (%s)", ss.str ().c_str ());

    ss.str ("");
    avail = plan2->avail_resources_at (1);
    bo = (bo || avail != 5);
    avail = plan2->avail_resources_at (10);
    bo = (bo || avail != 4);
    avail = plan2->avail_resources_at (1001);
    bo = (bo || avail != 9);
    avail = plan2->avail_resources_at (2000);
    bo = (bo || avail != 0);
    avail = plan2->avail_resources_at (2500);
    bo = (bo || avail != 0);
    avail = plan2->avail_resources_at (3000);
    bo = (bo || avail != 0);
    avail = plan2->avail_resources_at (3001);
    bo = (bo || avail != 10);
    ok (!bo, "avail_at_resources_* works");

    bo = false;
    rc = plan2->avail_during (2000, 1001, counts1);
    bo = (bo || rc != 0);
    avail = plan2->avail_resources_during (2000, 1001);
    bo = (bo || avail != 0);
    rc = plan2->avail_during (0, 1001, counts4);
    bo = (bo || rc != 1);
    avail = plan2->avail_resources_during (0, 1001);
    bo = (bo || avail != 4);
    rc = plan2->avail_during (10, 1990, counts4);
    bo = (bo || rc != 1);
    avail = plan2->avail_resources_during (10, 1990);
    bo = (bo || avail != 4);
    ok (!bo, "resources_during works");

    bo = false;
    rc = plan2->avail_during (4, 3, counts5);
    bo = (bo || rc != 1);
    avail = plan2->avail_resources_during (4, 3);
    bo = (bo || avail != 5);
    rc = plan2->avail_during (20, 980, counts4);
    bo = (bo || rc != 1);
    avail = plan2->avail_resources_during (20, 980);
    bo = (bo || avail != 4);
    rc = plan2->avail_during (1001, 998, counts9);
    bo = (bo || rc != 1);
    avail = plan2->avail_resources_during (1001, 998);
    bo = (bo || avail != 9);
    rc = plan2->avail_during (2500, 101, counts1);
    bo = (bo || rc != 0);
    avail = plan2->avail_resources_during (2500, 101);
    bo = (bo || avail != 0);
    ok (!bo, "resources_during works for a subset (no edges)");

    bo = false;
    rc = plan2->avail_during (0, 1000, counts4);
    bo = (bo || rc != 1);
    rc = plan2->avail_during (10, 990, counts4);
    bo = (bo || rc != 1);
    rc = plan2->avail_during (20, 981, counts4);
    bo = (bo || rc != 1);
    rc = plan2->avail_during (1001, 999, counts9);
    bo = (bo || rc != 1);
    ok (!bo, "resources_during works for a subset (1 edge)");

    bo = false;
    rc = plan2->avail_during (100, 1401, counts4);
    bo = (bo || rc != 1);
    rc = plan2->avail_during (1500, 1001, counts1);
    bo = (bo || rc != 0);
    rc = plan2->avail_during (1000, 1001, counts1);
    bo = (bo || rc != 0);
    ok (!bo, "resources_during works for >1 overlapping spans");

    bo = false;
    rc = plan2->avail_during (0, 3001, counts1);
    bo = (bo || rc != 0);
    rc = plan2->avail_during (0, 2001, counts1);
    bo = (bo || rc != 0);
    rc = plan2->avail_during (3001, 2000, counts10);
    bo = (bo || rc != 1);
    ok (!bo, "resources_during works for all spans");

    bo = false;
    t = plan2->avail_time_first (0, 9, counts5);
    bo = (bo || t != 0);
    //t = planner_avail_time_next (ctx);
    //bo = (bo || t != 1);
    //t = planner_avail_time_next (ctx);
    //bo = (bo || t != 1001);
    //t = planner_avail_time_next (ctx);
    //bo = (bo || t != 3001);
    //t = planner_avail_time_next (ctx);
    //bo = (bo || t != -1);
    ok (!bo && errno == ENOENT, "avail_time_* works");

    bo = false;
    t = plan2->avail_time_first (0, 10, counts9);
    bo = (bo || t != 1001);
    std::cout << "TIME: " << t << " BO: " << bo << "\n";
    //t = planner_avail_time_next (ctx);
    //bo = (bo || t != 3001);
    //t = planner_avail_time_next (ctx);
    //bo = (bo || t != -1);
    ok (!bo && errno == ENOENT, "avail_time_* test 2 works");

    return 0;
}

int test_remove_more ()
{
    int end = 0, i, rc;
    int64_t at, span;
    bool bo = false;
    uint64_t resource_total = 10;
    char resource_type[] = "core";
    uint64_t count = 5;
    int overlap_factor = resource_total / count;
    std::vector<int64_t> query_times;
    planner2 *plan2 = nullptr;
    std::stringstream ss;

    to_stream (0, INT64_MAX, resource_total, resource_type, ss);
    plan2 = new planner2 (resource_total, resource_type, 0, INT64_MAX);
    ok (plan2 != nullptr, "new with (%s)", ss.str ().c_str ());
    ss.str ("");

    std::vector<int64_t> spans;
    for (i = 0; i < 10000; ++i) {
        at = i / overlap_factor * 1000;
        span = plan2->add_span (at, 1000, count);
        spans.push_back (span);
        bo = (bo || span == -1);
    }

    for (i = 0; i < end; i += 4) {
        rc = plan2->remove_span (spans[i]);
        bo = (bo || rc == -1);
    }

    ok (!bo, "removing more works");

    return 0;
}

int test_stress_fully_overlap ()
{
    int i = 0;
    bool bo = false;
    int64_t t = -1;
    int64_t span;
    uint64_t resource_total = 10000000;
    uint64_t counts100 = 100;
    char resource_type[] = "hardware-thread";
    planner2 *plan2 = nullptr;
    std::stringstream ss;

    to_stream (0, INT64_MAX, resource_total, resource_type, ss);
    plan2 = new planner2 (resource_total, resource_type, 0, INT64_MAX);
    ok (plan2 != nullptr, "new with (%s)", ss.str ().c_str ());

    ss.str ("");
    for (i = 0; i < 100000; ++i) {
        t = plan2->avail_time_first (0, 4, counts100);
        bo = (bo || t != 0);
        span = plan2->add_span (t, 4, counts100);
        bo = (bo || span == -1);
    }
    ok (!bo, "add_span 100000 times (fully overlapped spans)");

    for (i = 0; i < 100000; ++i) {
        t = plan2->avail_time_first (0, 4, counts100);
        bo = (bo || t != 4);
        span = plan2->add_span (t, 4, counts100);
        bo = (bo || span == -1);
    }
    ok (!bo, "add_span 100000 more (fully overlapped spans)");

    return 0;
}

int test_stress_4spans_overlap ()
{
    int i = 0;
    int rc = 0;
    int64_t span;
    bool bo = false;
    uint64_t resource_total = 10000000;
    char resource_type[] = "hardware-thread";
    uint64_t counts100 = 100;
    planner2 *plan2 = nullptr;
    std::stringstream ss;
    struct timeval st, et;

    to_stream (0, INT64_MAX, resource_total, resource_type, ss);
    plan2 = new planner2 (resource_total, resource_type, 0, INT64_MAX);
    ok (plan2 != nullptr, "new with (%s)", ss.str ().c_str ());

    gettimeofday (&st, NULL);
    for (i = 0; i < 100000; ++i) {
        rc = plan2->avail_during (i, 4, counts100);
        bo = (bo || rc != 1);
        span = plan2->add_span (i, 4, counts100);
        bo = (bo || span == -1);
    }
    ok (!bo, "add_span 100000 times (4 spans overlap)");
    std::cout << "RC: " << rc << " BO: " << bo << " SPAN: " << span <<  "\n";

    for (i = 100000; i < 200000; ++i) {
        rc = plan2->avail_during (i, 4, counts100);
        bo = (bo || rc != 1);
        span = plan2->add_span (i, 4, counts100);
        bo = (bo || span == -1);
    }
    //ok (!bo, "add_span 100000 more (4 spans overlap)");
    gettimeofday (&et, NULL);
    std::cout << "Time taken by code block: " << get_elapse_time (st, et) << " microseconds" << std::endl;
    std::cout << "Planner span size: " << plan2->m_span_lookup.size () << std::endl;
    std::cout << "Planner span pts: " << plan2->m_multi_container.size () << std::endl;

    return 0;
}

static int test_more_add_remove ()
{
    int rc;
    int64_t span1 = -1, span2 = -1, span3 = -1, span4 = -1, span5 = -1, span6 = -1;
    bool bo = false;
    uint64_t resource_total = 100000;
    uint64_t resource1 = 36;
    uint64_t resource2 = 3600;
    uint64_t resource3 = 1800;
    uint64_t resource4 = 1152;
    uint64_t resource5 = 2304;
    uint64_t resource6 = 468;
    const char resource_type[] = "core";
    planner2 *plan2 = nullptr;
    std::stringstream ss;

    to_stream (0, INT64_MAX, resource_total, resource_type, ss);
    plan2 = new planner2 (resource_total, resource_type, 0, INT64_MAX);
    ok (plan2 != nullptr, "new with (%s)", ss.str ().c_str ());
    ss.str ("");

    span1 = plan2->add_span (0, 600, resource1);
    bo = (bo || span1 == -1);
    span2 = plan2->add_span (0, 57600, resource2);
    bo = (bo || span2 == -1);
    span3 = plan2->add_span (57600, 57600, resource3);
    bo = (bo || span3 == -1);
    span4 = plan2->add_span (115200, 57600, resource4);
    bo = (bo || span4 == -1);
    span5 = plan2->add_span (172800, 57600, resource5);
    bo = (bo || span5 == -1);
    span6 = plan2->add_span (115200, 900, resource6);
    bo = (bo || span6 == -1);

    rc = plan2->remove_span (span1);
    bo = (bo || rc == -1);
    rc = plan2->remove_span (span2);
    bo = (bo || rc == -1);
    rc = plan2->remove_span (span3);
    bo = (bo || rc == -1);
    rc = plan2->remove_span (span4);
    bo = (bo || rc == -1);
    rc = plan2->remove_span (span5);
    bo = (bo || rc == -1);
    rc = plan2->remove_span (span6);
    bo = (bo || rc == -1);

    span1 = plan2->add_span (0, 600, resource1);
    bo = (bo || span1 == -1);
    span2 = plan2->add_span (0, 57600, resource2);
    bo = (bo || span2 == -1);
    span3 = plan2->add_span (57600, 57600, resource3);
    bo = (bo || span3 == -1);
    span4 = plan2->add_span (115200, 57600, resource4);
    bo = (bo || span4 == -1);
    span5 = plan2->add_span (172800, 57600, resource5);
    bo = (bo || span5 == -1);
    span6 = plan2->add_span (115200, 900, resource6);
    bo = (bo || span6 == -1);

    ok (!bo, "more add-remove-add test works");

    return 0;
}

int main (int argc, char *argv[])
{
    plan (26);

    //test_add_remove ();

    test_basic_add_remove ();

    test_availability_checkers ();

    test_remove_more ();

    test_stress_fully_overlap ();

    test_stress_4spans_overlap ();

    test_more_add_remove ();

    done_testing ();

    return EXIT_SUCCESS;
}

/*
 * vi: ts=4 sw=4 expandtab
 */
