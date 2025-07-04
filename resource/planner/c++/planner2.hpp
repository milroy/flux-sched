/*****************************************************************************\
 * Copyright 2025 Lawrence Livermore National Security, LLC
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
#include <map>
#include <unordered_set>
#include <boost/multi_index_container.hpp>
#include <boost/multi_index/member.hpp>
#include <boost/multi_index/ordered_index.hpp>
#include <boost/multi_index/composite_key.hpp>
#include <boost/range/iterator_range_core.hpp>
#include <boost/pool/pool_alloc.hpp>

#include <iostream>

struct time_point {
    uint64_t at_time = 0;
    uint64_t free_ct = 0;
    mutable uint64_t reference_ct = 1;
};

struct change_counts
{
  change_counts (uint64_t new_val):new_val (new_val){}
  void operator() (time_point &t)
  {
    t.free_ct -= new_val;
    //t.occupied_ct += new_val;
  }
private:
  uint64_t new_val;
};

/* tags for accessing the corresponding indices of planner_multi_meta */
struct at_time {};
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

typedef composite_key<time_point,
          member<time_point, uint64_t, &time_point::at_time>, // sort on time
          member<time_point, uint64_t, &time_point::free_ct> // and then sort by free
> at_free_ck;

struct earliest_free {
  bool operator() (const composite_key_result<at_free_ck> &k,
                   const boost::tuple<uint64_t, uint64_t> &q) const {
    return k.value.free_ct < q.get<1> ();
  }
};

typedef multi_index_container<
    time_point, // container data
    indexed_by< // list of indexes
        ordered_unique<  // map-like;
            tag<at_time>, // index nametag
            member<time_point, uint64_t, &time_point::at_time> // index's key
        >,
        ordered_unique<  // map-like;
            tag<time_count>, // index nametag
            at_free_ck // composite key type
        >
    >,
    boost::pool_allocator<time_point>
    //polyfill_allocator<time_point>
> multi_container;

/*! Node in a span interval tree to enable fast retrieval of intercepting spans.
 */
struct span_t {
    bool operator== (const span_t &o) const;
    bool operator!= (const span_t &o) const;

    uint64_t start = 0;              /* start time of the span */
    uint64_t end = 0;                /* end time of the span */
    int64_t span_id = 0;             /* span id */
    uint64_t res_occupied = 0;       /* required resource quantity */
};

/*! Planner class
 */
class planner2 {
public:
    planner2 () = default;
    planner2 (const uint64_t total_resources,
              const std::string &resource_type,
              const uint64_t plan_start,
              const uint64_t plan_end);
    ~planner2 () = default;

    bool avail_during (uint64_t at, uint64_t duration, uint64_t request) const;
    int64_t add_span (uint64_t start_time, uint64_t duration, uint64_t request);

    multi_container m_multi_container;
    uint64_t m_total_resources = 0;
    std::string m_resource_type = "";
    uint64_t m_plan_start = 0;      /* base time of the planner */
    uint64_t m_plan_end = 0;        /* end time of the planner */
    uint64_t m_span_counter = 0;
    std::map<int64_t, span_t *> m_span_lookup;

private:

};

#endif /* PLANNER2_HPP */

/*
 * vi: ts=4 sw=4 expandtab
 */
