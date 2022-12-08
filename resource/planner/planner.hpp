/*****************************************************************************\
 * Copyright 2014 Lawrence Livermore National Security, LLC
 * (c.f. AUTHORS, NOTICE.LLNS, LICENSE)
 *
 * This file is part of the Flux resource manager framework.
 * For details, see https://github.com/flux-framework.
 *
 * SPDX-License-Identifier: LGPL-3.0
\*****************************************************************************/

#ifndef PLANNER_HPP
#define PLANNER_HPP

#include <memory>
#include "planner_internal_tree.hpp"

struct request_t {
    int64_t on_or_after;
    uint64_t duration;
    int64_t count;
};

/*! Node in a span interval tree to enable fast retrieval of intercepting spans.
 */
struct span_t {
    int64_t start;               /* start time of the span */
    int64_t last;                /* end time of the span */
    int64_t span_id;             /* unique span id */
    int64_t planned;             /* required resource quantity */
    int in_system;               /* 1 when inserted into the system */
    std::shared_ptr <scheduled_point_t> start_p = nullptr;  /* scheduled point object at start */
    std::shared_ptr <scheduled_point_t> last_p = nullptr;   /* scheduled point object at last */
};

/*! Planner class
 */
class planner {
public:
    planner ();
    planner (const int64_t base_time, const uint64_t duration,
               const uint64_t resource_totals, const char *in_resource_type);
    planner (const planner &o);
    planner &operator= (const planner &o);
    ~planner ();

    int mt_tree_insert (std::shared_ptr <scheduled_point_t> point);
    int mt_tree_remove (std::shared_ptr <scheduled_point_t> point);
    int sp_tree_insert (std::shared_ptr <scheduled_point_t> point);
    int sp_tree_remove (std::shared_ptr <scheduled_point_t> point);
    void destroy_sp_tree ();
    std::shared_ptr<scheduled_point_t> sp_tree_search (int64_t at);
    std::shared_ptr<scheduled_point_t> sp_tree_get_state (int64_t at);
    std::shared_ptr<scheduled_point_t> sp_tree_next (std::shared_ptr<scheduled_point_t> point) const;
    std::shared_ptr<scheduled_point_t> sp_tree_next (std::shared_ptr<scheduled_point_t> point);
    std::shared_ptr<scheduled_point_t> mt_tree_get_mintime (const int64_t request) const;
    void clear_avail_time_iter ();
    void clear_span_lookup ();
    void span_lookup_erase (std::map<int64_t, std::shared_ptr<span_t>>::iterator &it);
    const std::map<int64_t, std::shared_ptr<span_t>> &get_span_lookup () const;
    std::map<int64_t, std::shared_ptr<span_t>> &get_span_lookup ();
    size_t span_lookup_get_size () const;
    void span_lookup_insert (int64_t span_id, std::shared_ptr<span_t> span);
    std::map<int64_t, std::shared_ptr<span_t>>::const_iterator get_span_lookup_iter () const;
    std::map<int64_t, std::shared_ptr<span_t>>::iterator get_span_lookup_iter ();
    void set_span_lookup_iter (std::map<int64_t, std::shared_ptr<span_t>>::iterator &it);
    void incr_span_lookup_iter ();

    int64_t get_total_resources () const;
    const std::string &get_resource_type () const;
    int64_t get_plan_start () const;
    int64_t get_plan_end () const;
    const std::shared_ptr<scheduled_point_t> &get_p0 () const;
    std::map<int64_t, std::shared_ptr<scheduled_point_t>> &get_avail_time_iter ();
    const std::map<int64_t, std::shared_ptr<scheduled_point_t>> &get_avail_time_iter () const;
    request_t &get_current_request ();
    const request_t &get_current_request () const;
    void set_avail_time_iter_set (int atime_iter_set);
    const int get_avail_time_iter_set () const;
    void incr_span_counter ();
    const uint64_t get_span_counter () const;

private:
    int64_t m_total_resources;
    std::string m_resource_type;
    int64_t m_plan_start;          /* base time of the planner */
    int64_t m_plan_end;            /* end time of the planner */
    scheduled_point_tree_t m_sched_point_tree;  /* scheduled point rb tree */
    mintime_resource_tree_t m_mt_resource_tree; /* min-time resrouce rb tree */
    std::shared_ptr<scheduled_point_t> m_p0 = nullptr;       /* system's scheduled point at base time */
    std::map<int64_t, std::shared_ptr<span_t>> m_span_lookup; /* span lookup */
    std::map<int64_t, std::shared_ptr<scheduled_point_t>> m_avail_time_iter; /* MT node track */
    std::map<int64_t, std::shared_ptr<span_t>>::iterator m_span_lookup_iter;
    
    request_t m_current_request;   /* the req copy for avail time iteration */
    int m_avail_time_iter_set;     /* iterator set flag */
    uint64_t m_span_counter;       /* current span counter */

    int copy_trees (const planner &o);
    int copy_maps (const planner &o);
    int clear ();
};

#endif /* PLANNER_HPP */

/*
 * vi: ts=4 sw=4 expandtab
 */
