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
#include "config.h"
#endif
}

#include <iostream>

#include "planner2.hpp"


/****************************************************************************
 *                                                                          *
 *                     Public Span_t Methods                                *
 *                                                                          *
 ****************************************************************************/

bool span_t::operator== (const span_t &o) const
{
    if (start != o.start)
        return false;
    if (end != o.end)
        return false;
    if (span_id != o.span_id)
        return false;
    if (res_occupied != o.res_occupied)
        return false;

    return true;
}

bool span_t::operator!= (const span_t &o) const
{
    return !operator == (o);
}

/****************************************************************************
 *                                                                          *
 *                     Public Planner Methods                               *
 *                                                                          *
 ****************************************************************************/

planner2::planner2 (const uint64_t total_resources,
                    const std::string &resource_type,
                    const uint64_t plan_start,
                    const uint64_t plan_end)
{
    m_total_resources = total_resources;
    m_resource_type = resource_type;
    m_plan_start = plan_start;
    m_plan_end = plan_end;
}

 bool planner2::avail_during (uint64_t at, uint64_t duration, uint64_t request) const
 {
    if ((at + duration) > m_plan_end) {
        errno = ERANGE;
        return false;
    }

    auto &by_time = m_multi_container.get<at_time> ();
    auto ub = by_time.upper_bound (at + duration);
    auto lb = by_time.lower_bound (at);
    for (const auto &at : boost::make_iterator_range (lb, ub)) {
        std::cout << " at " <<  at.at_time << " occupied " << at.occupied_ct << "\n";
        if (at.free_ct < request)
            return false; 
    }

    return true;
 }

int64_t planner2::add_span (uint64_t start_time, uint64_t duration, uint64_t request)
{
    int64_t span_id = -1;
    uint64_t newval = 0;
    uint64_t oldval = 0;

    if (start_time < m_plan_start || duration < 1 || request > m_total_resources || 
        start_time + duration > m_plan_end) {
            errno = EINVAL;
            return span_id;
    }

    if (!avail_during (start_time, duration, request)) {
        errno = EINVAL;
        return span_id;
    }

    try {
        ++m_span_counter;
        if (m_span_lookup.find (m_span_counter) != m_span_lookup.end ()) {
            errno = EEXIST;
            return span_id;
        }
        span_t *new_span = new span_t;
        new_span->span_id = m_span_counter;
        span_id = new_span->span_id;
        new_span->start = start_time;
        new_span->end = start_time + duration;
        new_span->res_occupied = request;
        m_span_lookup.insert ({span_id, new_span});
    } catch (std::bad_alloc &e) {
        errno = ENOMEM;
        return -1;
    }

    auto lb_exists = m_multi_container.get<at_time> ().find (start_time);
    if (lb_exists == m_multi_container.get<at_time> ().end ()) {
        m_multi_container.insert (time_point{start_time, request, m_total_resources - request, 1});
    } else {
        if (lb_exists->free_ct < request) {
            errno = EINVAL;
            return -1;
        }
        newval = lb_exists->free_ct - request;
        oldval = lb_exists->free_ct;
        m_multi_container.modify (lb_exists, change_counts (newval), change_counts (oldval));
        // what can we do about rollback above?
        lb_exists->reference_ct += 1;
    }
    auto ub_exists = m_multi_container.get<at_time> ().find (start_time + duration);
    if (ub_exists == m_multi_container.get<at_time> ().end ()) {
        m_multi_container.insert (time_point{start_time + duration, request, m_total_resources - request, 1});
    } else {
        if (ub_exists->free_ct < request) {
            errno = EINVAL;
            return -1;
        }
        newval = ub_exists->free_ct - request;
        oldval = ub_exists->free_ct;
        m_multi_container.modify (ub_exists, change_counts (newval), change_counts (oldval));
        // what can we do about rollback above?
        ub_exists->reference_ct += 1;
    }

    auto &by_time = m_multi_container.get<at_time> ();
    auto lb = ++lb_exists;
    for (auto &at = lb; at != ub_exists; ++at) {
        if (at->free_ct < request) {
            errno = EINVAL;
            return -1;
        }
        newval = at->free_ct - request;
        oldval = at->free_ct;
        m_multi_container.modify (at, change_counts (newval), change_counts (oldval));
        // what can we do about rollback above?
        at->reference_ct += 1;
    }
    return span_id;
}

/*
 * vi: ts=4 sw=4 expandtab
 */
