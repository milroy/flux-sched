/*****************************************************************************\
 * Copyright 2023 Lawrence Livermore National Security, LLC
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

#include <cstdlib>
#include <cerrno>
#include <cstring>
#include <limits>
#include <memory>
#include <vector>
#include <map>

#include "planner_multi.hpp"

////////////////////////////////////////////////////////////////////////////////
// Public Planner Multi Methods
////////////////////////////////////////////////////////////////////////////////

planner_multi::planner_multi () = default;

planner_multi::planner_multi (int64_t base_time,
                              uint64_t duration,
                              const uint64_t *resource_totals,
                              const char **resource_types,
                              size_t len)
{
    size_t i = 0;
    std::string type;
    planner_t *p = nullptr;

    m_iter.on_or_after = 0;
    m_iter.duration = 0;
    for (i = 0; i < len; ++i) {
        try {
            type = std::string (resource_types[i]);
            p = new planner_t (base_time, duration, resource_totals[i], resource_types[i]);
        } catch (std::bad_alloc &e) {
            errno = ENOMEM;
            throw std::bad_alloc ();
        }
        m_iter.counts[type] = 0;
        m_types_totals_planners.push_back ({type, resource_totals[i], p});
    }
    m_span_counter = 0;
}

planner_multi::planner_multi (const planner_multi &o)
{
    for (auto &iter : o.m_types_totals_planners) {
        planner_t *np = nullptr;
        if (iter.planner) {
            try {
                np = new planner_t (*(iter.planner->plan));
            } catch (std::bad_alloc &e) {
                errno = ENOMEM;
            }
            // planner copy ctor can throw runtime_error, resulting in nullptr
            if (np == nullptr)
                throw std::runtime_error (
                    "ERROR in planner copy ctor"
                    " in planner_multi copy"
                    " constructor\n");
        } else {
            try {
                np = new planner_t ();
            } catch (std::bad_alloc &e) {
                errno = ENOMEM;
                throw std::bad_alloc ();
            }
        }
        m_types_totals_planners.push_back ({iter.resource_type, iter.resource_total, np});
    }
    m_iter = o.m_iter;
    m_span_lookup = o.m_span_lookup;
    // o's iterator points into o's m_span_lookup; copying it would leave
    // this object holding an iterator into another container.  Reset it to
    // a defined state instead; users must call avail_time_first () before
    // avail_time_next () anyway.
    m_span_lookup_iter = m_span_lookup.end ();
    m_span_counter = o.m_span_counter;
}

planner_multi &planner_multi::operator= (const planner_multi &o)
{
    if (this == &o)
        return *this;

    // Erase *this so the vectors are empty
    erase ();

    for (const auto &iter : o.m_types_totals_planners) {
        planner_t *np = nullptr;
        if (iter.planner) {
            try {
                // Invoke copy constructor to avoid the assignment
                // operator erase () penalty.
                np = new planner_t (*(iter.planner->plan));
            } catch (std::bad_alloc &e) {
                errno = ENOMEM;
            }
            // planner copy ctor can throw runtime_error, resulting in nullptr
            if (np == nullptr)
                throw std::runtime_error (
                    "ERROR in planner copy ctor"
                    " in planner_multi assn"
                    " operator\n");
        } else {
            try {
                np = new planner_t ();
            } catch (std::bad_alloc &e) {
                errno = ENOMEM;
                throw std::bad_alloc ();
            }
        }
        m_types_totals_planners.push_back ({iter.resource_type, iter.resource_total, np});
    }
    m_iter = o.m_iter;
    m_span_lookup = o.m_span_lookup;
    // See the copy constructor: never adopt an iterator into o's container.
    m_span_lookup_iter = m_span_lookup.end ();
    m_span_counter = o.m_span_counter;

    return *this;
}

bool planner_multi::operator== (const planner_multi &o) const
{
    if (m_span_counter != o.m_span_counter)
        return false;
    if (m_span_lookup != o.m_span_lookup)
        return false;
    if (m_iter.on_or_after != o.m_iter.on_or_after)
        return false;
    if (m_iter.duration != o.m_iter.duration)
        return false;
    if (m_iter.counts != o.m_iter.counts)
        return false;

    if (m_types_totals_planners.size () != o.m_types_totals_planners.size ())
        return false;
    const auto &o_by_type = o.m_types_totals_planners.get<res_type> ();
    const auto &by_type = m_types_totals_planners.get<res_type> ();
    for (const auto &data : by_type) {
        const auto o_data = o_by_type.find (data.resource_type);
        if (o_data == o_by_type.end ())
            return false;
        if (data.resource_type != o_data->resource_type)
            return false;
        if (data.resource_total != o_data->resource_total)
            return false;
        if (*(data.planner->plan) != *(o_data->planner->plan))
            return false;
    }

    return true;
}

bool planner_multi::operator!= (const planner_multi &o) const
{
    return !operator== (o);
}

void planner_multi::erase ()
{
    if (!m_types_totals_planners.empty ()) {
        for (auto iter : m_types_totals_planners) {
            if (iter.planner) {
                delete iter.planner;
                iter.planner = nullptr;
            }
        }
    }
    m_types_totals_planners.clear ();
}

planner_multi::~planner_multi ()
{
    erase ();
}

planner_t *planner_multi::get_planner_at (size_t i) const
{
    return m_types_totals_planners.at (i).planner;
}

planner_t *planner_multi::get_planner_at (const char *type) const
{
    auto &by_res = m_types_totals_planners.get<res_type> ();
    return by_res.find (type)->planner;
}

int planner_multi::update (const uint64_t *resource_totals,
                           const char **resource_types,
                           size_t len)
{
    // An empty planner_multi has no planner at index 0 to derive the base
    // time and duration from
    if (m_types_totals_planners.size () < 1) {
        errno = EINVAL;
        return -1;
    }
    // Preserve the historical semantics of an empty request: it describes
    // no resource types, so nothing is added, updated, or deleted.
    if (len == 0)
        return 0;

    for (size_t i = 0; i < len; ++i) {
        if (resource_totals[i] > static_cast<uint64_t> (std::numeric_limits<int64_t>::max ())) {
            errno = ERANGE;
            return -1;
        }
    }

    // Fast path: the request names the current resource types in the
    // current order (the steady state when the traverser re-primes a
    // vertex), so only the totals can change. Positional identity also
    // implies no duplicates.  Everything below is no-throw, so the
    // all-or-nothing guarantee holds trivially.
    if (len == m_types_totals_planners.size ()) {
        bool same_composition = true;
        for (size_t i = 0; i < len; ++i) {
            if (m_types_totals_planners[i].resource_type != resource_types[i]) {
                same_composition = false;
                break;
            }
        }
        if (same_composition) {
            // update_total walks the scheduled points without allocating
            // and cannot fail; noop when the total is unchanged.
            for (size_t i = 0; i < len; ++i) {
                auto &meta = m_types_totals_planners[i];
                meta.resource_total = resource_totals[i];
                meta.planner->plan->update_total (resource_totals[i]);
            }
            return 0;
        }
    }

    // Slow path: the composition changed (planners added, deleted, or
    // reordered). Stage the entire new state on the side, then commit
    // with no-throw operations only, so any failure leaves *this
    // untouched.
    staged_update staged;
    if (stage_update (resource_totals, resource_types, len, staged) < 0)
        return -1;
    commit_update (staged);
    return 0;
}

// Validate the request and build the updated planner set, iterator
// request counts, and per-span metadata vectors into `staged` without
// mutating *this. Totals must already be range-checked. Returns -1 with
// errno set (EINVAL, ENOMEM) on failure.
int planner_multi::stage_update (const uint64_t *resource_totals,
                                 const char **resource_types,
                                 size_t len,
                                 staged_update &staged)
{
    static constexpr size_t NPOS = std::numeric_limits<size_t>::max ();
    // Assume the base time and duration of the planner at index 0
    int64_t base_time = get_planner_at (static_cast<size_t> (0))->plan->get_plan_start ();
    int64_t duration = get_planner_at (static_cast<size_t> (0))->plan->get_plan_end () - base_time;
    if (duration < 0) {
        errno = EINVAL;
        return -1;
    }
    try {
        std::unordered_set<std::string> rtypes;
        std::vector<size_t> old_pos (len, NPOS);
        auto &by_res = m_types_totals_planners.get<res_type> ();
        auto &by_idx = m_types_totals_planners.get<idx> ();

        for (size_t i = 0; i < len; ++i) {
            // A type appearing twice would be assigned two conflicting
            // positions and totals; reject the request as malformed.
            if (!rtypes.insert (resource_types[i]).second) {
                errno = EINVAL;
                return -1;
            }
        }
        for (size_t i = 0; i < len; ++i) {
            std::string type{resource_types[i]};
            planner_t *p = nullptr;
            int64_t count = 0;
            auto it = by_res.find (type);
            if (it == by_res.end ()) {
                staged.added.push_back (std::make_unique<planner_t> (base_time,
                                                                     static_cast<uint64_t> (
                                                                         duration),
                                                                     resource_totals[i],
                                                                     resource_types[i]));
                p = staged.added.back ().get ();
            } else {
                p = it->planner;
                old_pos[i] = static_cast<size_t> (by_idx.iterator_to (*it) - by_idx.begin ());
                auto cit = m_iter.counts.find (type);
                if (cit != m_iter.counts.end ())
                    count = cit->second;
            }
            staged.counts[type] = count;
            staged.set.push_back (planner_multi_meta{std::move (type), resource_totals[i], p});
        }
        for (auto &meta : m_types_totals_planners)
            if (rtypes.find (meta.resource_type) == rtypes.end ())
                staged.removed.push_back (meta.planner);
        // Rebuild each span's per-planner metadata vector in the new
        // planner order: entries follow their planner to its new
        // position; types added by this update hold no allocation for
        // existing spans, which is what a -1 entry denotes.
        staged.span_vecs.reserve (m_span_lookup.size ());
        for (auto &kv : m_span_lookup) {
            std::vector<int64_t> nv (len, -1);
            for (size_t j = 0; j < len; ++j)
                if (old_pos[j] != NPOS && old_pos[j] < kv.second.size ())
                    nv[j] = kv.second[old_pos[j]];
            staged.span_vecs.push_back (std::move (nv));
        }
    } catch (std::bad_alloc &e) {
        errno = ENOMEM;
        return -1;
    }
    return 0;
}

// Install a staged update. No-throw: container swaps, in-place vector
// swaps (which keep m_span_lookup_iter valid, since the map's structure
// does not change), arithmetic total updates, and deletions.
void planner_multi::commit_update (staged_update &staged)
{
    m_types_totals_planners.swap (staged.set);
    m_iter.counts.swap (staged.counts);
    size_t k = 0;
    for (auto &kv : m_span_lookup)
        kv.second.swap (staged.span_vecs[k++]);
    // Retained planners may have new totals; update_total walks the
    // scheduled points without allocating and cannot fail.  For planners
    // added above the delta is zero and this is a noop.
    for (auto &meta : m_types_totals_planners)
        meta.planner->plan->update_total (meta.resource_total);
    for (auto *p : staged.removed)
        delete p;
    for (auto &up : staged.added)
        up.release ();
}

bool planner_multi::planner_at (const char *type) const
{
    auto &by_res = m_types_totals_planners.get<res_type> ();
    auto result = by_res.find (type);
    if (result == by_res.end ())
        return false;
    else
        return true;
}

size_t planner_multi::get_planners_size () const
{
    return m_types_totals_planners.size ();
}

int64_t planner_multi::get_resource_total_at (size_t i) const
{
    return m_types_totals_planners.at (i).resource_total;
}

int64_t planner_multi::get_resource_total_at (const char *type) const
{
    auto &by_res = m_types_totals_planners.get<res_type> ();
    auto result = by_res.find (type);
    if (result == by_res.end ())
        return -1;
    else
        return result->resource_total;
}

const char *planner_multi::get_resource_type_at (size_t i) const
{
    return m_types_totals_planners.at (i).resource_type.c_str ();
}

size_t planner_multi::get_resource_type_idx (const char *type) const
{
    auto by_res = m_types_totals_planners.get<res_type> ().find (type);
    if (by_res == m_types_totals_planners.get<res_type> ().end ())
        return m_types_totals_planners.size ();
    auto curr_idx = m_types_totals_planners.get<idx> ().iterator_to (*by_res);
    return curr_idx - m_types_totals_planners.begin ();
}

struct request_multi &planner_multi::get_iter ()
{
    return m_iter;
}

std::map<uint64_t, std::vector<int64_t>> &planner_multi::get_span_lookup ()
{
    return m_span_lookup;
}

std::map<uint64_t, std::vector<int64_t>>::iterator &planner_multi::get_span_lookup_iter ()
{
    return m_span_lookup_iter;
}

void planner_multi::set_span_lookup_iter (std::map<uint64_t, std::vector<int64_t>>::iterator &it)
{
    m_span_lookup_iter = it;
}

void planner_multi::incr_span_lookup_iter ()
{
    m_span_lookup_iter++;
}

uint64_t planner_multi::get_span_counter ()
{
    return m_span_counter;
}

void planner_multi::set_span_counter (uint64_t sc)
{
    m_span_counter = sc;
}

void planner_multi::incr_span_counter ()
{
    m_span_counter++;
}

////////////////////////////////////////////////////////////////////////////////
// Public Planner_multi_t methods
////////////////////////////////////////////////////////////////////////////////

// The wrapper constructors rethrow so that a planner_multi_t can never
// be observed with a null inner planner_multi; `new planner_multi_t (...)`
// either succeeds completely or throws (operator new releases the wrapper
// allocation automatically when the constructor throws).

planner_multi_t::planner_multi_t ()
{
    try {
        plan_multi = new planner_multi ();
    } catch (std::bad_alloc &e) {
        errno = ENOMEM;
        throw;
    }
}

planner_multi_t::planner_multi_t (const planner_multi &o)
{
    try {
        plan_multi = new planner_multi (o);
    } catch (std::bad_alloc &e) {
        errno = ENOMEM;
        throw;
    }
}

planner_multi_t::planner_multi_t (int64_t base_time,
                                  uint64_t duration,
                                  const uint64_t *resource_totals,
                                  const char **resource_types,
                                  size_t len)
{
    try {
        plan_multi = new planner_multi (base_time, duration, resource_totals, resource_types, len);
    } catch (std::bad_alloc &e) {
        errno = ENOMEM;
        throw;
    }
}

planner_multi_t::~planner_multi_t ()
{
    delete plan_multi;
}

/*
 * vi: ts=4 sw=4 expandtab
 */
