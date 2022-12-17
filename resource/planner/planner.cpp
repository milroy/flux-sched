/*****************************************************************************\
 * Copyright 2014 Lawrence Livermore National Security, LLC
 * (c.f. AUTHORS, NOTICE.LLNS, LICENSE)
 *
 * This file is part of the Flux resource manager framework.
 * For details, see https://github.com/flux-framework.
 *
 * SPDX-License-Identifier: LGPL-3.0
\*****************************************************************************/

#include <limits>
#include <map>
#include <list>
#include <string>

#include "planner.h"


/****************************************************************************
 *                                                                          *
 *                     Public Planner Methods                               *
 *                                                                          *
 ****************************************************************************/

planner::planner ()
{
    // Empty planner with essential members
    // set for comparsons.
    m_total_resources = 0;
    m_resource_type = "";
    m_plan_start = 0;
    m_plan_end = 0;
    m_p0 = nullptr;
    m_avail_time_iter_set = 0;
    m_span_counter = 0;
}

planner::planner (const int64_t base_time, const uint64_t duration,
                  const uint64_t resource_totals, const char *in_resource_type)
{
    m_total_resources = static_cast<int64_t> (resource_totals);
    m_resource_type = in_resource_type;
    m_plan_start = base_time;
    m_plan_end = base_time + static_cast<int64_t> (duration);
    m_p0 = new scheduled_point_t ();
    m_p0->in_mt_resource_tree = 0;
    m_p0->new_point = 1;
    m_p0->at = base_time;
    m_p0->ref_count = 1;
    m_p0->remaining = m_total_resources;
    m_sched_point_tree.insert (m_p0);
    m_mt_resource_tree.insert (m_p0);
    m_avail_time_iter_set = 0;
    m_span_counter = 0;
}

planner::planner (const planner &o)
{
    int rc = -1;
    // Important: need to copy trees first,
    // since map copies fetch the scheduled
    // points inserted into the trees.
    if ( (rc = copy_trees (o)) < 0) {
        throw std::runtime_error ("ERROR copying trees\n");
    }
    if ( (rc = copy_maps (o)) < 0) {
        throw std::runtime_error ("ERROR copying maps\n");
    }

    m_total_resources = o.m_total_resources;
    m_resource_type = o.m_resource_type;
    m_plan_start = o.m_plan_start;
    m_plan_end = o.m_plan_end;
    m_current_request = o.m_current_request;
    m_avail_time_iter_set = o.m_avail_time_iter_set;
    m_span_counter = o.m_span_counter;
    // After above copy the SP tree now has the full state of the base point.
    m_p0 = m_sched_point_tree.get_state (m_plan_start);
}

planner &planner::operator= (const planner &o)
{
    int rc = -1;

    if ( (rc = erase ()) != 0) {
       throw std::runtime_error ("ERROR erasing *this\n");
    }
    // Important: need to copy trees first,
    // since map copies fetch the scheduled
    // points inserted into the trees.
    if ( (rc = copy_trees (o)) != 0) {
        throw std::runtime_error ("ERROR copying trees to *this\n");
    }
    if ( (rc = copy_maps (o)) != 0) {
        throw std::runtime_error ("ERROR copying maps to *this\n");
    }

    m_total_resources = o.m_total_resources;
    m_resource_type = o.m_resource_type;
    m_plan_start = o.m_plan_start;
    m_plan_end = o.m_plan_end;
    m_current_request = o.m_current_request;
    m_avail_time_iter_set = o.m_avail_time_iter_set;
    m_span_counter = o.m_span_counter;
    // After above copy the SP tree now has the full state of the base point.
    m_p0 = m_sched_point_tree.get_state (m_plan_start);

    return *this;
}

bool planner::operator== (const planner &o) const
{
    if (m_total_resources != o.m_total_resources)
        return false;
    if (m_resource_type != o.m_resource_type)
        return false;
    if (m_plan_start != o.m_plan_start)
        return false;
    if (m_plan_end != o.m_plan_end)
        return false;
    if (m_avail_time_iter_set != o.m_avail_time_iter_set)
        return false;
    if (m_span_counter != o.m_span_counter)
        return false;
    // m_p0 or o.m_p0 could be uninitialized
    if (m_p0 && o.m_p0) {
        if (!scheduled_points_equal (*m_p0, *(o.m_p0)))
            return false;
    } else if (m_p0 || o.m_p0) {
        return false;
    } // else both nullptr
    if (!span_lookups_equal (o))
        return false;
    if (!avail_time_iters_equal (o))
        return false;
    if (!trees_equal (o))
        return false;
    return true;
}

planner::~planner ()
{
    // Destructor is nothrow
    // The destroy function called in the 
    // scheduled point tree deletes all scheduled
    // points (including m_p0)
    // inserted in the class ctor
    // or reinitialize.
    erase ();
}

int planner::erase ()
{
    int rc = 0;

    // returns 0 or a negative number
    rc = restore_track_points ();
    m_span_lookup.clear ();
    if (m_p0 && m_p0->in_mt_resource_tree)
        rc += m_mt_resource_tree.remove (m_p0);
    m_sched_point_tree.destroy ();
    m_mt_resource_tree.clear ();

    return rc;
}

int planner::reinitialize (int64_t base_time, uint64_t duration)
{
    int rc = 0;

    m_plan_start = base_time;
    m_plan_end = base_time + static_cast<int64_t> (duration);
    m_p0 = new scheduled_point_t ();
    m_p0->at = base_time;
    m_p0->ref_count = 1;
    m_p0->remaining = m_total_resources;
    // returns 0 or -1
    rc = m_sched_point_tree.insert (m_p0);
    // returns 0 or -1
    rc += m_mt_resource_tree.insert (m_p0);
    m_avail_time_iter_set = 0;
    m_span_counter = 0;

    return rc;
}

int planner::restore_track_points ()
{
    int rc = 0;

    if (!m_avail_time_iter.empty ()) {
        for (auto &kv : m_avail_time_iter) {
            // returns 0 or -1
            rc += m_mt_resource_tree.insert (kv.second);
        }
    }
    m_avail_time_iter.clear ();

    return rc;
}

int64_t planner::get_total_resources () const
{
    return m_total_resources;
}

const std::string &planner::get_resource_type () const
{
    return m_resource_type;
}

int64_t planner::get_plan_start () const
{
    return m_plan_start;
}

int64_t planner::get_plan_end () const
{
    return m_plan_end;
}

int planner::mt_tree_insert (scheduled_point_t *point)
{
    return m_mt_resource_tree.insert (point);
}

int planner::mt_tree_remove (scheduled_point_t *point)
{
    return m_mt_resource_tree.remove (point);
}

int planner::sp_tree_insert (scheduled_point_t *point)
{
    return m_sched_point_tree.insert (point);
}

int planner::sp_tree_remove (scheduled_point_t *point)
{
    return m_sched_point_tree.remove (point);
}

void planner::destroy_sp_tree ()
{
    m_sched_point_tree.destroy ();
}

scheduled_point_t *planner::sp_tree_search (int64_t at)
{
    return m_sched_point_tree.search (at);
}

scheduled_point_t *planner::sp_tree_get_state (int64_t at)
{
    return m_sched_point_tree.get_state (at);
}

scheduled_point_t *planner::sp_tree_next (scheduled_point_t *point) const
{
    return m_sched_point_tree.next (point);
}

scheduled_point_t *planner::mt_tree_get_mintime (int64_t request) const
{
    return m_mt_resource_tree.get_mintime (request);
}

void planner::clear_span_lookup ()
{
    m_span_lookup.clear ();
}

void planner::span_lookup_erase (std::map<int64_t,
                                 std::shared_ptr<span_t>>::iterator &it)
{
    m_span_lookup.erase (it);
}

const std::map<int64_t, std::shared_ptr<span_t>>
                        &planner::get_span_lookup_const () const
{
    return m_span_lookup;
}

std::map<int64_t, std::shared_ptr<span_t>> &planner::get_span_lookup ()
{
    return m_span_lookup;
}

size_t planner::span_lookup_get_size () const
{
    return m_span_lookup.size ();
}

void planner::span_lookup_insert (int64_t span_id,
                                  std::shared_ptr<span_t> span)
{
    m_span_lookup.insert (std::pair<int64_t, std::shared_ptr<span_t>> (span_id,
                                                                       span));
}

void planner::set_span_lookup_iter (std::map<int64_t,
                                    std::shared_ptr<span_t>>::iterator &it)
{
    m_span_lookup_iter = it;
}

const std::map<int64_t, std::shared_ptr<span_t>>::iterator 
                                    planner::get_span_lookup_iter () const
{
    return m_span_lookup_iter;
}

void planner::incr_span_lookup_iter ()
{
    m_span_lookup_iter++;
}

std::map<int64_t, scheduled_point_t *> &planner::get_avail_time_iter ()
{
    return m_avail_time_iter;
}

const std::map<int64_t, scheduled_point_t *> 
                        &planner::get_avail_time_iter_const () const
{
    return m_avail_time_iter;
}

void planner::clear_avail_time_iter ()
{
    m_avail_time_iter.clear ();
}

void planner::set_avail_time_iter_set (int atime_iter_set)
{
    m_avail_time_iter_set = atime_iter_set;
}

const int planner::get_avail_time_iter_set () const
{
    return m_avail_time_iter_set;
}

request_t &planner::get_current_request ()
{
    return m_current_request;
}

const request_t &planner::get_current_request_const () const
{
    return m_current_request;
}

void planner::incr_span_counter ()
{
    m_span_counter++;
}
    
const uint64_t planner::get_span_counter () const
{
    return m_span_counter;
}

/****************************************************************************
 *                                                                          *
 *                     Private Planner Methods                              *
 *                                                                          *
 ****************************************************************************/

int planner::copy_trees (const planner &o)
{
    int rc = 0;

    // Incoming planner could be empty.
    if (!o.m_sched_point_tree.empty ()) {
        // Need to get the state at plan_start, not just m_p0 since they could
        // have diverged after scheduling.
        scheduled_point_t *point = o.m_sched_point_tree.get_state (o.m_plan_start);
        scheduled_point_t *new_point = nullptr;
        while (point) {
            new_point = new scheduled_point_t ();
            new_point->at = point->at;
            new_point->in_mt_resource_tree = point->in_mt_resource_tree;
            new_point->new_point = point->new_point;
            new_point->ref_count = point->ref_count;
            new_point->scheduled = point->scheduled;
            new_point->remaining = point->remaining;
            if ( (rc = m_sched_point_tree.insert (new_point)) != 0)
                return rc;
            if ( (rc = m_mt_resource_tree.insert (new_point)) != 0)
                return rc;
            point = o.m_sched_point_tree.next (point);
        }
    } else {
        // Erase if incoming planner is empty.
        rc = erase ();
    }
    return rc;
}

int planner::copy_maps (const planner &o)
{
    int rc = 0;

    if (!o.m_span_lookup.empty ()) {
        for (auto const &span_it : o.m_span_lookup) {
            std::shared_ptr<span_t> new_span = std::make_shared<span_t> ();
            // Need to deep copy spans, else copies of planners will share
            // pointers.
            new_span->start = span_it.second->start;
            new_span->last = span_it.second->last;
            new_span->span_id = span_it.second->span_id;
            new_span->planned = span_it.second->planned;
            new_span->in_system = span_it.second->in_system;
            new_span->start_p = m_sched_point_tree.get_state (new_span->start);
            new_span->last_p = m_sched_point_tree.get_state (new_span->last);
            m_span_lookup[span_it.first] = new_span;
        }
    } else {
        m_span_lookup.clear ();
    }
    if (!o.m_avail_time_iter.empty ()) {
        for (auto const &avail_it : o.m_avail_time_iter) {
            // Scheduled point already copied into trees, so fetch
            // from SP tree.
            scheduled_point_t *new_avail =
                    m_sched_point_tree.get_state (avail_it.second->at);
            m_avail_time_iter[avail_it.first] = new_avail;
        }
    } else {
        m_avail_time_iter.clear ();
    }

    return rc;
}

bool planner::scheduled_points_equal (const scheduled_point_t &lhs,
                                      const scheduled_point_t &rhs) const
{
    if (lhs.at != rhs.at)
        return false;
    if (lhs.in_mt_resource_tree != rhs.in_mt_resource_tree)
        return false;
    if (lhs.new_point != rhs.new_point)
        return false;
    if (lhs.ref_count != rhs.ref_count)
        return false;
    if (lhs.remaining != rhs.remaining)
        return false;
    if (lhs.scheduled != rhs.scheduled)
        return false;

    return true;
}

bool planner::span_lookups_equal (const planner &o) const
{
    if (m_span_lookup.size () != o.m_span_lookup.size ())
        return false;
    if (!m_span_lookup.empty ()) {
        // Iterate through indices to use the auto range-based for loop
        // semantics; otherwise need to create a "zip" function for two maps.
        for (auto const &this_it : m_span_lookup) {
            auto const other = o.m_span_lookup.find (this_it.first);
            if (other == o.m_span_lookup.end ())
                return false;
            if (this_it.first != other->first)
                return false;
            if (this_it.second->start != other->second->start)
                return false;
            if (this_it.second->last != other->second->last)
                return false;
            if (this_it.second->span_id != other->second->span_id)
                return false;
            if (this_it.second->planned != other->second->planned)
                return false;
            if (this_it.second->in_system != other->second->in_system)
                return false;
            if (this_it.second->in_system != other->second->in_system)
                return false;
            if (!scheduled_points_equal (*(this_it.second->start_p),
                                         *(other->second->start_p)))
                return false;
            if (!scheduled_points_equal (*(this_it.second->last_p),
                                         *(other->second->last_p)))
                return false;
        }
    }
    return true;
}

bool planner::avail_time_iters_equal (const planner &o) const
{
    if (m_avail_time_iter.size () != o.m_avail_time_iter.size ())
        return false;
    if (!m_avail_time_iter.empty ()) {
        // Iterate through indices to use the auto range-based for loop
        // semantics; otherwise need to create a "zip" function for two maps.
        for (auto const &this_it : m_avail_time_iter) {
            auto const other = o.m_avail_time_iter.find (this_it.first);
            if (other == o.m_avail_time_iter.end ())
                return false;
            if (this_it.first != other->first)
                return false;
            if (this_it.second && other->second) {
                if (!scheduled_points_equal (*(this_it.second),
                                             *(other->second)))
                    return false;
            } else if (this_it.second || other->second) {
                return false;
            }
        }
    }
    return true;
}

bool planner::trees_equal (const planner &o) const
{
    if (m_sched_point_tree.get_size () != o.m_sched_point_tree.get_size ())
        return false;
    if (!m_sched_point_tree.empty ()) {
        scheduled_point_t *this_pt =
                        m_sched_point_tree.get_state (m_plan_start);
        scheduled_point_t *o_pt =
                        o.m_sched_point_tree.get_state (o.m_plan_start);
        while (this_pt) {
            if (!scheduled_points_equal (*this_pt, *o_pt))
                return false;
            this_pt = m_sched_point_tree.next (this_pt);
            o_pt = o.m_sched_point_tree.next (o_pt);
        }
    }
    return true;
}


/*******************************************************************************
 *                                                                             *
 *                  Scheduled Point and Resource Update APIs                   *
 *                                                                             *
 *******************************************************************************/

static int track_points (std::map<int64_t, scheduled_point_t *> &tracker,
                         scheduled_point_t *point)
{
    // caller will rely on the fact that rc == -1 when key already exists.
    // don't need to register free */
    auto res = tracker.insert (std::pair<int64_t,
                                         scheduled_point_t *> (point->at,
                                                               point));
    return res.second? 0 : -1;
}

static void restore_track_points (planner_t *ctx)
{
    ctx->plan->restore_track_points ();
}

static void update_mintime_resource_tree (planner_t *ctx,
                                          std::list<scheduled_point_t *> &list)
{
    scheduled_point_t *point = nullptr;
    for (auto &point : list) {
        if (point->in_mt_resource_tree)
            ctx->plan->mt_tree_remove (point);
        if (point->ref_count && !(point->in_mt_resource_tree))
            ctx->plan->mt_tree_insert (point);
    }
}

static void copy_req (request_t &dest, int64_t on_or_after, uint64_t duration,
                      uint64_t resource_count)
{
    dest.on_or_after = on_or_after;
    dest.duration = duration;
    dest.count = static_cast<int64_t> (resource_count);
}

static scheduled_point_t *get_or_new_point (planner_t *ctx, int64_t at)
{
    scheduled_point_t *point = nullptr;
    try {
        if ( !(point = ctx->plan->sp_tree_search (at))) {
            scheduled_point_t *state = ctx->plan->sp_tree_get_state (at);
            point = new scheduled_point_t ();
            point->at = at;
            point->in_mt_resource_tree = 0;
            point->new_point = 1;
            point->ref_count = 0;
            point->scheduled = state->scheduled;
            point->remaining = state->remaining;
            ctx->plan->sp_tree_insert (point);
            ctx->plan->mt_tree_insert (point);
        }
    } catch (std::bad_alloc &e) {
        errno = ENOMEM;
    }
    return point;
}

static void fetch_overlap_points (planner_t *ctx, int64_t at, uint64_t duration,
                                  std::list<scheduled_point_t *> &list)
{
    scheduled_point_t *point = ctx->plan->sp_tree_get_state (at);
    while (point) {
        if (point->at >= static_cast<int64_t> (at + duration))
            break;
        else if (point->at >= at)
            list.push_back (point);
        point = ctx->plan->sp_tree_next (point);
    }
}

static int update_points_add_span (planner_t *ctx,
                                   std::list<scheduled_point_t *> &list,
                                   std::shared_ptr<span_t> &span)
{
    int rc = 0;
    for (auto &point : list) {
        point->scheduled += span->planned;
        point->remaining -= span->planned;
        if ( (point->scheduled > ctx->plan->get_total_resources ())
              || (point->remaining < 0)) {
            errno = ERANGE;
            rc = -1;
        }
    }
    return rc;
}

static int update_points_subtract_span (planner_t *ctx,
                                        std::list<scheduled_point_t *> &list,
                                        std::shared_ptr<span_t> &span)
{
    int rc = 0;
    for (auto &point : list) {
        point->scheduled -= span->planned;
        point->remaining += span->planned;
        if ( (point->scheduled < 0)
              || (point->remaining > ctx->plan->get_total_resources ())) {
            errno = ERANGE;
            rc = -1;
        }
    }
    return rc;
}

static bool span_ok (planner_t *ctx, scheduled_point_t *start_point,
                     uint64_t duration, int64_t request)
{
    bool ok = true;
    scheduled_point_t *next_point = nullptr;
    for (next_point = start_point;
         next_point != nullptr;
         next_point = ctx->plan->sp_tree_next (next_point)) {
         if (next_point->at >= (start_point->at + (int64_t)duration)) {
             ok = true;
             break;
         } else if (request > next_point->remaining) {
             ctx->plan->mt_tree_remove (start_point);
             track_points (ctx->plan->get_avail_time_iter (), start_point);
             ok = false;
             break;
         }
    }
    return ok;
}

static int64_t avail_at (planner_t *ctx, int64_t on_or_after, uint64_t duration,
                         int64_t request)
{
    int64_t at = -1;
    scheduled_point_t *start_point = nullptr;
    while ((start_point = ctx->plan->mt_tree_get_mintime (request))) {
        at = start_point->at;
        if (at < on_or_after) {
            ctx->plan->mt_tree_remove (start_point);
            track_points (ctx->plan->get_avail_time_iter (), start_point);
            at = -1;

        } else if (span_ok (ctx, start_point, duration, request)) {
            ctx->plan->mt_tree_remove (start_point);
            track_points (ctx->plan->get_avail_time_iter (), start_point);
            if (static_cast<int64_t> (at + duration) > ctx->plan->get_plan_end ())
                at = -1;
            break;
        }
    }
    return at;
}

static bool avail_during (planner_t *ctx, int64_t at, uint64_t duration,
                          const int64_t request)
{
    bool ok = true;
    if (static_cast<int64_t> (at + duration) > ctx->plan->get_plan_end ()) {
        errno = ERANGE;
        return -1;
    }

    scheduled_point_t *point = ctx->plan->sp_tree_get_state (at);
    while (point) {
        if (point->at >= (at + (int64_t)duration)) {
            ok = true;
            break;
        } else if (request > point->remaining) {
            ok = false;
            break;
        }
        point = ctx->plan->sp_tree_next (point);
    }
    return ok;
}

static scheduled_point_t *avail_resources_during (planner_t *ctx, int64_t at,
                                                  uint64_t duration)
{
    if (static_cast<int64_t> (at + duration) > ctx->plan->get_plan_end ()) {
        errno = ERANGE;
        return nullptr;
    }

    scheduled_point_t *point = ctx->plan->sp_tree_get_state (at);
    scheduled_point_t *min = point;
    while (point) {
        if (point->at >= (at + (int64_t)duration))
            break;
        else if (min->remaining > point->remaining)
          min = point;
        point = ctx->plan->sp_tree_next (point);
    }
    return min;
}


/*******************************************************************************
 *                                                                             *
 *                              Utilities                                      *
 *                                                                             *
 *******************************************************************************/

static inline void erase (planner_t *ctx)
{
    ctx->plan->erase ();
}

static inline bool not_feasable (planner_t *ctx, int64_t start_time,
                                 uint64_t duration, int64_t request)
{
    bool rc = (start_time < ctx->plan->get_plan_start ()) || (duration < 1)
              || (static_cast<int64_t> (start_time + duration - 1)
                     > ctx->plan->get_plan_end ());
    return rc;
}

static int span_input_check (planner_t *ctx, int64_t start_time,
                             uint64_t duration, int64_t request)
{
    int rc = -1;
    if (!ctx || not_feasable (ctx, start_time, duration, request)) {
        errno = EINVAL;
        goto done;
    } else if (request > ctx->plan->get_total_resources () || request < 0) {
        errno = ERANGE;
        goto done;
    }
    rc = 0;
done:
    return rc;
}

static std::shared_ptr<span_t> span_new (planner_t *ctx, int64_t start_time,
                                         uint64_t duration, uint64_t request)
{
    std::shared_ptr<span_t> span = nullptr;
    try {
        if (span_input_check (ctx, start_time, duration, (int64_t)request) == -1)
            goto done;
        ctx->plan->incr_span_counter ();
        if (ctx->plan->get_span_lookup ().find (ctx->plan->get_span_counter ())
            != ctx->plan->get_span_lookup ().end ()) {
            errno = EEXIST;
            goto done;
        }
        span = std::make_shared<span_t> ();
        span->start = start_time;
        span->last = start_time + duration;
        span->span_id = ctx->plan->get_span_counter ();
        span->planned = request;
        span->in_system = 0;
        span->start_p = nullptr;
        span->last_p = nullptr;

        // errno = EEXIST condition already checked above
        ctx->plan->span_lookup_insert (span->span_id, span);
    }
    catch (std::bad_alloc &e) {
        errno = ENOMEM;
    }

done:
    return span;
}


/*******************************************************************************
 *                                                                             *
 *                           PUBLIC PLANNER API                                *
 *                                                                             *
 *******************************************************************************/

extern "C" planner_t *planner_new (int64_t base_time, uint64_t duration,
                                   uint64_t resource_totals,
                                   const char *resource_type)
{
    planner_t *ctx = nullptr;

    if (duration < 1 || !resource_type) {
        errno = EINVAL;
        goto done;
    } else if (resource_totals > std::numeric_limits<int64_t>::max ()) {
        errno = ERANGE;
        goto done;
    }

    try {
        ctx = new planner_t ();
        ctx->plan = nullptr;
        ctx->plan = new planner (base_time, duration, resource_totals,
                                 resource_type);
    } catch (std::bad_alloc &e) {
        errno = ENOMEM;
    }

done:
    return ctx;
}

extern "C" planner_t *planner_copy (planner_t *p)
{
    planner_t *ctx = nullptr;

    try {
        ctx = new planner_t ();
        ctx->plan = nullptr;
        ctx->plan = new planner (*(p->plan));
    } catch (std::bad_alloc &e) {
        errno = ENOMEM;
    }

done:
    return ctx;
}

extern "C" planner_t *planner_new_empty ()
{
    planner_t *ctx = nullptr;

    try {
        ctx = new planner_t ();
        ctx->plan = nullptr;
        ctx->plan = new planner ();
    } catch (std::bad_alloc &e) {
        errno = ENOMEM;
    }

done:
    return ctx;
}

extern "C" int planner_reset (planner_t *ctx,
                              int64_t base_time, uint64_t duration)
{
    int rc = 0;
    if (!ctx || duration < 1) {
        errno = EINVAL;
        return -1;
    }

    erase (ctx);
    try {
        // Returns 0 or a negative int
        rc = ctx->plan->reinitialize (base_time, duration);
    } catch (std::bad_alloc &e) {
        errno = ENOMEM;
        rc = -1;
    }
    return rc;
}

extern "C" void planner_destroy (planner_t **ctx_p)
{
    if (ctx_p && *ctx_p) {
        delete (*ctx_p)->plan;
        (*ctx_p)->plan = nullptr;
        delete *ctx_p;
        *ctx_p = nullptr;
    }
}

extern "C" int64_t planner_base_time (planner_t *ctx)
{
    if (!ctx) {
        errno = EINVAL;
        return -1;
    }
    return ctx->plan->get_plan_start ();
}

extern "C" int64_t planner_duration (planner_t *ctx)
{
    if (!ctx) {
        errno = EINVAL;
        return -1;
    }
    return ctx->plan->get_plan_end () - ctx->plan->get_plan_start ();
}

extern "C" int64_t planner_resource_total (planner_t *ctx)
{
    if (!ctx) {
        errno = EINVAL;
        return -1;
    }
    return ctx->plan->get_total_resources ();
}

extern "C" const char *planner_resource_type (planner_t *ctx)
{
    if (!ctx) {
        errno = EINVAL;
        return nullptr;
    }
    return ctx->plan->get_resource_type ().c_str ();
}

extern "C" int64_t planner_avail_time_first (planner_t *ctx,
                                             int64_t on_or_after,
                                             uint64_t duration,
                                             uint64_t request)
{
    int64_t t = -1;
    if (!ctx || on_or_after < ctx->plan->get_plan_start ()
        || on_or_after >= ctx->plan->get_plan_end () || duration < 1) {
        errno = EINVAL;
        return -1;
    }
    if (static_cast<int64_t> (request) > ctx->plan->get_total_resources ()) {
        errno = ERANGE;
        return -1;
    }
    restore_track_points (ctx);
    ctx->plan->set_avail_time_iter_set (1);
    copy_req (ctx->plan->get_current_request (), on_or_after,
              duration, request);
    if ( (t = avail_at (ctx, on_or_after, duration, (int64_t)request)) == -1)
        errno = ENOENT;
    return t;
}

extern "C" int64_t planner_avail_time_next (planner_t *ctx)
{
    int64_t t = -1;
    int64_t on_or_after = -1;
    uint64_t duration = 0;
    int64_t request_count = 0;
    if (!ctx || !ctx->plan->get_avail_time_iter_set ()) {
        errno = EINVAL;
        return -1;
    }
    request_count = ctx->plan->get_current_request_const ().count;
    on_or_after = ctx->plan->get_current_request_const ().on_or_after;
    duration = ctx->plan->get_current_request_const ().duration;
    if (request_count > ctx->plan->get_total_resources ()) {
        errno = ERANGE;
        return -1;
    }
    if ( (t = avail_at (ctx, on_or_after, duration, (int64_t)request_count)) ==
          -1)
        errno = ENOENT;
    return t;
}

extern "C" int planner_avail_during (planner_t *ctx, int64_t start_time,
                                     uint64_t duration, uint64_t request)
{
    bool ok = false;
    if (!ctx || duration < 1) {
        errno = EINVAL;
        return -1;
    }
    if (static_cast<int64_t> (request) > ctx->plan->get_total_resources ()) {
        errno = ERANGE;
        return -1;
    }
    ok = avail_during (ctx, start_time, duration, (int64_t)request);
    return ok? 0 : -1;
}

extern "C" int64_t planner_avail_resources_during (planner_t *ctx,
                                                int64_t at, uint64_t duration)
{
    scheduled_point_t *min_point = nullptr;
    if (!ctx || at > ctx->plan->get_plan_end () || duration < 1) {
        errno = EINVAL;
        return -1;
    }
    min_point = avail_resources_during (ctx, at, duration);
    return min_point->remaining;
}

extern "C" int64_t planner_avail_resources_at (planner_t *ctx, int64_t at)
{
    const scheduled_point_t *state = nullptr;
    if (!ctx || at > ctx->plan->get_plan_end ()) {
        errno = EINVAL;
        return -1;
    }
    state = ctx->plan->sp_tree_get_state (at);
    return state->remaining;
}

extern "C" int64_t planner_add_span (planner_t *ctx, int64_t start_time,
                                     uint64_t duration, uint64_t request)
{
    std::shared_ptr<span_t> span = nullptr;
    scheduled_point_t *start_point = nullptr;
    scheduled_point_t *last_point = nullptr;

    if (!avail_during (ctx, start_time, duration, (int64_t)request)) {
        errno = EINVAL;
        return -1;
    }
    if ( !(span = span_new (ctx, start_time, duration, request)))
        return -1;

    restore_track_points (ctx);
    std::list<scheduled_point_t *> list;
    if ((start_point = get_or_new_point (ctx, span->start)) == nullptr)
        return -1;
    start_point->ref_count++;
    if ((last_point = get_or_new_point (ctx, span->last)) == nullptr)
        return -1;
    last_point->ref_count++;

    fetch_overlap_points (ctx, span->start, duration, list);
    update_points_add_span (ctx, list, span);

    start_point->new_point = 0;
    span->start_p = start_point;
    last_point->new_point = 0;
    span->last_p = last_point;

    update_mintime_resource_tree (ctx, list);

    list.clear ();
    span->in_system = 1;
    ctx->plan->set_avail_time_iter_set (0);

    return span->span_id;
}

extern "C" int planner_rem_span (planner_t *ctx, int64_t span_id)
{
    int rc = -1;
    uint64_t duration = 0;
    std::map<int64_t, std::shared_ptr<span_t>>::iterator it;

    if (!ctx) {
        errno = EINVAL;
        return -1;
    }
    it = ctx->plan->get_span_lookup ().find (span_id);
    if (it == ctx->plan->get_span_lookup ().end ()) {
        errno = EINVAL;
        return -1;
    }
    std::shared_ptr<span_t> &span = it->second;

    restore_track_points (ctx);
    std::list<scheduled_point_t *> list;
    duration = span->last - span->start;
    span->start_p->ref_count--;
    span->last_p->ref_count--;
    fetch_overlap_points (ctx, span->start, duration, list);
    update_points_subtract_span (ctx, list, span);
    update_mintime_resource_tree (ctx, list);
    span->in_system = 0;

    if (span->start_p->ref_count == 0) {
        ctx->plan->sp_tree_remove (span->start_p);
        if (span->start_p->in_mt_resource_tree)
            ctx->plan->mt_tree_remove (span->start_p);
        delete span->start_p;
        span->start_p = nullptr;
    }
    if (span->last_p->ref_count == 0) {
        ctx->plan->sp_tree_remove (span->last_p);
        if (span->last_p->in_mt_resource_tree)
            ctx->plan->mt_tree_remove (span->last_p);
        delete span->last_p;
        span->last_p = nullptr;
    }
    ctx->plan->span_lookup_erase (it);
    list.clear ();
    ctx->plan->set_avail_time_iter_set (0);
    rc = 0;

done:
    return rc;
}

extern "C" int64_t planner_span_first (planner_t *ctx)
{
    if (!ctx) {
        errno = EINVAL;
        return -1;
    }
    auto span_lookup_begin = ctx->plan->get_span_lookup ().begin ();
    ctx->plan->set_span_lookup_iter (span_lookup_begin);
    if (ctx->plan->get_span_lookup_iter () ==
                            ctx->plan->get_span_lookup ().end ()) {
        errno = EINVAL;
        return -1;
    }
    std::shared_ptr<span_t> &span = ctx->plan->get_span_lookup_iter ()->second;
    return span->span_id;
}

extern "C" int64_t planner_span_next (planner_t *ctx)
{
    if (!ctx) {
        errno = EINVAL;
        return -1;
    }
    ctx->plan->incr_span_lookup_iter ();
    if (ctx->plan->get_span_lookup_iter () ==
                        ctx->plan->get_span_lookup ().end ()) {
        errno = EINVAL;
        return -1;
    }
    std::shared_ptr<span_t> &span = ctx->plan->get_span_lookup_iter ()->second;
    return span->span_id;
}

extern "C" size_t planner_span_size (planner_t *ctx)
{
    if (!ctx) {
        errno = EINVAL;
        return 0;
    }
    return ctx->plan->span_lookup_get_size ();
}

extern "C" bool planner_is_active_span (planner_t *ctx, int64_t span_id)
{
    if (!ctx) {
        errno = EINVAL;
        return false;
    }
    auto it = ctx->plan->get_span_lookup ().find (span_id);
    if (ctx->plan->get_span_lookup ().find (span_id) ==
                                    ctx->plan->get_span_lookup ().end ()) {
        errno = EINVAL;
        return false;
    }
    std::shared_ptr<span_t> &span = it->second;
    return (span->in_system)? true : false;
}

extern "C" int64_t planner_span_start_time (planner_t *ctx, int64_t span_id)
{
    if (!ctx) {
        errno = EINVAL;
        return -1;
    }
    auto it = ctx->plan->get_span_lookup ().find (span_id);
    if (ctx->plan->get_span_lookup ().find (span_id) ==
                                    ctx->plan->get_span_lookup ().end ()) {
        errno = EINVAL;
        return -1;
    }
    std::shared_ptr<span_t> &span = it->second;
    return span->start;
}

extern "C" int64_t planner_span_duration (planner_t *ctx, int64_t span_id)
{
    if (!ctx) {
        errno = EINVAL;
        return -1;
    }
    auto it = ctx->plan->get_span_lookup ().find (span_id);
    if (ctx->plan->get_span_lookup ().find (span_id) ==
                                    ctx->plan->get_span_lookup ().end ()) {
        errno = EINVAL;
        return -1;
    }
    std::shared_ptr<span_t> &span = it->second;
    return (span->last - span->start);
}

extern "C" int64_t planner_span_resource_count (planner_t *ctx,
                                                int64_t span_id)
{
    if (!ctx) {
        errno = EINVAL;
        return -1;
    }
    auto it = ctx->plan->get_span_lookup ().find (span_id);
    if (ctx->plan->get_span_lookup ().find (span_id) ==
                                    ctx->plan->get_span_lookup ().end ()) {
        errno = EINVAL;
        return -1;
    }
    std::shared_ptr<span_t> &span = it->second;
    return span->planned;
}

extern "C" bool planners_equal (planner_t *lhs, planner_t *rhs)
{
    return (*(lhs->plan) == *(rhs->plan));
}

/*
 * vi: ts=4 sw=4 expandtab
 */
