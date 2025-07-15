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
    m_multi_container.insert (time_point{plan_start, total_resources, 1});
}

 bool planner2::avail_during (uint64_t at, uint64_t duration, uint64_t request) const
 {
    if ((at + duration) > m_plan_end) {
        errno = ERANGE;
        return false;
    }

    auto &by_time = m_multi_container.get<at_time> ();
    auto ub = by_time.lower_bound (at + duration);
    auto lbound = by_time.range (boost::multi_index::unbounded, boost::lambda::_1 <= at);
    auto lb = lbound.second;
    if (lb != by_time.end () && lb != by_time.begin ()) {
        --lb;
    } else {
        if (lbound.first != by_time.end ()) {
            auto tmp = by_time.rbegin ();
            if (tmp != by_time.rend ()) {
                ++tmp;
                lb = tmp.base ();
            }
        }
    }
    if (ub == by_time.end () && ub != by_time.begin ()) {
        --ub;
    }
    for (const auto &at : boost::make_iterator_range (lb, ub)) {
        if (at.free_ct < request)
            return false; 
    }

    return true;
 }

int64_t planner2::add_span (uint64_t start_time, uint64_t duration, uint64_t request)
{
    int64_t span_id = -1;

    if (start_time < m_plan_start || duration < 1 || request > m_total_resources || 
        start_time + duration > m_plan_end) {
            errno = EINVAL;
            return -1;
    }

    if (!avail_during (start_time, duration, request)) {
        errno = EINVAL;
        return -1;
    }

    try {
        ++m_span_counter;
        if (m_span_lookup.find (m_span_counter) != m_span_lookup.end ()) {
            errno = EEXIST;
            return -1;
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
    uint64_t et = start_time + duration;
    bool found_start = false;
    bool found_end = false;
    auto &by_time = m_multi_container.get<at_time> ();
    auto lbound = by_time.range (boost::multi_index::unbounded, boost::lambda::_1 <= start_time);
    auto ub = by_time.lower_bound (et);
    auto lb = lbound.second;
    if (lb != by_time.end () && lb != by_time.begin ()) {
        // decrement because .second points to element after
        --lb;
        if (lb->at_time == start_time) {
            found_start = true;
            by_time.modify (lb, reduce_free (request));
            lb->reference_ct += 1;
            // Advance iterator so loop below doesn't re-find start
            lb++;
        }
    } else {
        // Iterator could be one past end time, which could be .end ()
        // Need to check if first is also .end () in which case no element found
        if (lbound.first != by_time.end ()) {
            auto tmp = by_time.rbegin ();
            if (tmp != by_time.rend ()) {
                // Advance reverse iterator
                ++tmp;
                lb = tmp.base ();
            }
        } else {
            errno = ERANGE;
            return -1;
        }
    }

    if (ub != by_time.end ()) {
        if (ub->at_time == et) {
            found_end = true;
            // By convention, end time does not occupy resources
            ub->reference_ct += 1;
            ub--;
        }
    }

    for (auto &at = lb; at != ub; ++at) {
        if (at->at_time == start_time) {
            found_start = true;
        }
        if (at->at_time == et) {
            found_end = true;
            // By convention, end time does not occupy resources
            at->reference_ct += 1;
            break;
        }
        if (at->at_time < start_time)
            continue;
        if (at->at_time > et)
            break;
        if (at->free_ct < request) {
            errno = EINVAL;
            return -1;
        }
        by_time.modify (at, reduce_free (request));
        at->reference_ct += 1;
    }
    if (!found_start) {
        by_time.emplace_hint (lb, start_time, m_total_resources - request, 1);
    }
    if (!found_end) {
        // By convention, end time does not occupy resources
        by_time.emplace_hint (ub, start_time + duration, m_total_resources, 1);
    }

    return span_id;
}

int64_t planner2::remove_span (int64_t span_id)
{
    auto span_it = m_span_lookup.find (span_id);
    if (span_it == m_span_lookup.end ()) {
        errno = ENOENT;
        return -1;
    }

    auto &by_time = m_multi_container.get<at_time> ();
    auto start = by_time.find (span_it->second->start);
    for (auto &at = start; at->at_time <= span_it->second->end ;) {
        at->reference_ct--;
        if (at->reference_ct == 0) {
            at = by_time.erase (at);
        } else {
            if (at->at_time != span_it->second->end)
                by_time.modify (at, add_free (span_it->second->res_occupied));
            ++at;
        }
    }
    m_span_lookup.erase (span_it);

    return 0;
}

/*
 * vi: ts=4 sw=4 expandtab
 */
