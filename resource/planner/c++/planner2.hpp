/*****************************************************************************\
 * Copyright 2024 Lawrence Livermore National Security, LLC
 * (c.f. AUTHORS, NOTICE.LLNS, LICENSE)
 *
 * This file is part of the Flux resource manager framework.
 * For details, see https://github.com/flux-framework.
 *
 * SPDX-License-Identifier: LGPL-3.0
\*****************************************************************************/

#ifndef PLANNER2_HPP
#define PLANNER2_HPP

#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <boost/multi_index_container.hpp>
#include <boost/multi_index/member.hpp>
#include <boost/multi_index/ordered_index.hpp>
#include <boost/multi_index/composite_key.hpp>
#include <boost/range/iterator_range_core.hpp>

struct request_multi {
    int64_t on_or_after = 0;
    uint64_t duration = 0;
    std::unordered_map<std::string, int64_t> counts;
};

struct planner_meta {
    uint64_t time;
    uint64_t occupied_resources;
    uint64_t free_resources;
};

/* tags for accessing the corresponding indices of planner_multi_meta */
struct earliest_time {};
struct occupied_count {};
struct time_count {};

template<typename T>
struct polyfill_allocator : std::allocator<T> {
        using std::allocator<T>::allocator;
        template<typename U>
        struct rebind {
            using other = polyfill_allocator<U>;
        };
        using pointer = T*;
        using const_pointer = T const*;
        using reference = T&;
        using const_reference = T const&;
};

using boost::multi_index_container;
using namespace boost::multi_index;
typedef multi_index_container<
    planner_meta, // container data
    indexed_by< // list of indexes
        ordered_unique<  // unordered_set-like; faster than ordered_unique in testing
            tag<earliest_time>, // index nametag
            member<planner_meta, uint64_t, &planner_meta::time> // index's key
        >,
        ordered_non_unique<  // unordered_set-like; faster than ordered_unique in testing
            tag<occupied_count>, // index nametag
            member<planner_meta, uint64_t, &planner_meta::occupied_resources> // index's key
        >,
        ordered_unique<  // unordered_set-like; faster than ordered_unique in testing
            tag<time_count>, // index nametag
            composite_key<planner_meta,
                member<planner_meta, uint64_t, &planner_meta::time>, // index's key
                member<planner_meta, uint64_t, &planner_meta::occupied_resources>
            > // index's key
        >
    >,
    polyfill_allocator<planner_meta>
> multi_container;

/*! Node in a span interval tree to enable fast retrieval of intercepting spans.
 */
struct span_t {
    bool operator== (const span_t &o) const;
    bool operator!= (const span_t &o) const;

    int64_t start;               /* start time of the span */
    int64_t last;                /* end time of the span */
    int64_t span_id;             /* unique span id */
    int64_t resource_count;             /* required resource quantity */
};

/*! Planner class
 */
class planner2 {
public:
    planner2 ();
    multi_container m_multi_container;

    int64_t m_total_resources = 0;
    std::string m_resource_type = "";
    int64_t m_plan_start = 0;      /* base time of the planner */
    int64_t m_plan_end = 0;        /* end time of the planner */

private:

};

#endif /* PLANNER2_HPP */

/*
 * vi: ts=4 sw=4 expandtab
 */
