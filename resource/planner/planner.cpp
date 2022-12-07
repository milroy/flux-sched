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

struct planner_ctx {
    planner_t *planner;
};

/****************************************************************************
 *                                                                          *
 *                     Public Planner Methods                               *
 *                                                                          *
 ****************************************************************************/

planner_t::planner_t ()
{
    m_total_resources = 0;
    m_resource_type = "";
    m_plan_start = 0;
    m_plan_end = 0;
    m_p0 = nullptr;
    m_p0 = new scheduled_point_t ();
    m_p0->at = 0;
    m_p0->ref_count = 1;
    m_p0->remaining = 0;
    m_avail_time_iter_set = 0;
    m_span_counter = 0;
}

planner_t::planner_t (const int64_t base_time, const uint64_t duration,
            const uint64_t resource_totals, const char *in_resource_type)
{
    m_total_resources = static_cast<int64_t> (resource_totals);
    m_resource_type = in_resource_type;
    m_plan_start = base_time;
    m_plan_end = base_time + static_cast<int64_t> (duration);
    m_p0 = nullptr;
    m_p0 = new scheduled_point_t ();
    m_p0->at = base_time;
    m_p0->ref_count = 1;
    m_p0->remaining = m_total_resources;
    m_sched_point_tree.insert (m_p0);
    m_mt_resource_tree.insert (m_p0);
    m_avail_time_iter_set = 0;
    m_span_counter = 0;
}

planner_t::planner_t (const planner_t &o)
{
    int rc = -1;

    if ( (rc = clear ()) < 0) {
        std::cout << "handle error\n";
    }

    m_total_resources = o.get_total_resources ();
    m_resource_type = o.get_resource_type ();
    m_plan_start = o.get_plan_start ();
    m_plan_end = o.get_plan_end ();
    m_current_request = o.get_current_request_const ();
    m_avail_time_iter_set = o.get_avail_time_iter_set ();
    m_span_counter = o.get_span_counter ();

    *m_p0 = *(o.get_p0 ());
    if ( (rc = copy_trees (o)) < 0) {
        std::cout << "handle error\n";
    }
    if ( (rc = copy_maps (o)) < 0) {
        std::cout << "handle error\n";
    }
}

planner_t &planner_t::operator= (const planner_t &o)
{
    int rc = -1;

    if ( (rc = clear ()) < 0) {
        std::cout << "handle error\n";
    }

    m_total_resources = o.get_total_resources ();
    m_resource_type = o.get_resource_type ();
    m_plan_start = o.get_plan_start ();
    m_plan_end = o.get_plan_end ();
    m_current_request = o.get_current_request_const ();
    m_avail_time_iter_set = o.get_avail_time_iter_set ();
    m_span_counter = o.get_span_counter ();

    *m_p0 = *(o.get_p0 ());
    if ( (rc = copy_trees (o)) < 0) {
        std::cout << "handle error\n";
    }
    if ( (rc = copy_maps (o)) < 0) {
        std::cout << "handle error\n";
    }

    return *this;
}

planner_t::~planner_t ()
{
    int rc = -1;

    if ( (rc = clear ()) < 0) {
        std::cout << "handle error\n";
    }
    // delete p0;
    // p0 = nullptr;
}

int planner_t::mt_tree_insert (scheduled_point_t *point)
{
    return m_mt_resource_tree.insert (point);
}

int planner_t::mt_tree_remove (scheduled_point_t *point)
{
    return m_mt_resource_tree.remove (point);
}

int planner_t::sp_tree_insert (scheduled_point_t *point)
{
    return m_sched_point_tree.insert (point);
}

int planner_t::sp_tree_remove (scheduled_point_t *point)
{
    return m_sched_point_tree.remove (point);
}

void planner_t::destroy_sp_tree ()
{
    m_sched_point_tree.destroy ();
}

scheduled_point_t *planner_t::sp_tree_search (int64_t at)
{
    return m_sched_point_tree.search (at);
}

scheduled_point_t *planner_t::sp_tree_get_state (int64_t at)
{
    return m_sched_point_tree.get_state (at);
}

scheduled_point_t *planner_t::sp_tree_next (const scheduled_point_t *point) const
{
    return m_sched_point_tree.next (point);
}

scheduled_point_t *planner_t::mt_tree_get_mintime (const int64_t request) const
{
    return m_mt_resource_tree.get_mintime (request);
}

void planner_t::clear_avail_time_iter ()
{
    m_avail_time_iter.clear ();
}

void planner_t::clear_span_lookup ()
{
    m_span_lookup.clear ();
}

void planner_t::span_lookup_erase (std::map<int64_t, std::shared_ptr<span_t>>::iterator &it)
{
    m_span_lookup.erase (it);
}

// std::map<int64_t, std::shared_ptr<span_t>>::iterator &planner_t::span_lookup_find (int64_t span_id) const
// {
//     auto span_it = m_span_lookup.find (span_id);
//     return span_it;
// }

// std::map<int64_t, std::shared_ptr<span_t>>::iterator &planner_t::span_lookup_get_begin () const
// {
//     return m_span_lookup.begin ();
// }

// std::map<int64_t, std::shared_ptr<span_t>>::iterator &planner_t::span_lookup_get_end () const
// {
//     return m_span_lookup.end ();
// }

size_t planner_t::span_lookup_get_size () const
{
    return m_span_lookup.size ();
}

void planner_t::span_lookup_insert (int64_t span_id, std::shared_ptr<span_t> span)
{
    m_span_lookup.insert (std::pair<int64_t, std::shared_ptr<span_t>> (span_id, span));
}

const std::map<int64_t, std::shared_ptr<span_t>> &planner_t::get_span_lookup_const () const
{
    return m_span_lookup;
}

std::map<int64_t, std::shared_ptr<span_t>> &planner_t::get_span_lookup ()
{
    return m_span_lookup;
}

const std::map<int64_t, std::shared_ptr<span_t>>::iterator planner_t::get_span_lookup_iter () const
{
    return m_span_lookup_iter;
}

void planner_t::set_span_lookup_iter (std::map<int64_t, std::shared_ptr<span_t>>::iterator &it)
{
    m_span_lookup_iter = it;
}

void planner_t::incr_span_lookup_iter ()
{
    m_span_lookup_iter++;
}

int64_t planner_t::get_total_resources () const
{
    return m_total_resources;
}

const std::string &planner_t::get_resource_type () const
{
    return m_resource_type;
}

int64_t planner_t::get_plan_start () const
{
    return m_plan_start;
}

int64_t planner_t::get_plan_end () const
{
    return m_plan_end;
}

scheduled_point_t *planner_t::get_p0 () const
{
    return m_p0;
}

std::map<int64_t, scheduled_point_t *> &planner_t::get_avail_time_iter ()
{
    return m_avail_time_iter;
}

const std::map<int64_t, scheduled_point_t *> &planner_t::get_avail_time_iter_const () const
{
    return m_avail_time_iter;
}

const request_t &planner_t::get_current_request_const () const
{
    return m_current_request;
}

request_t &planner_t::get_current_request ()
{
    return m_current_request;
}

void planner_t::set_avail_time_iter_set (int atime_iter_set)
{
    m_avail_time_iter_set = atime_iter_set;
}

const int planner_t::get_avail_time_iter_set () const
{
    return m_avail_time_iter_set;
}

void planner_t::incr_span_counter ()
{
    m_span_counter++;
}
    
const uint64_t planner_t::get_span_counter () const
{
    return m_span_counter;
}

/****************************************************************************
 *                                                                          *
 *                     Private Planner Methods                              *
 *                                                                          *
 ****************************************************************************/

int planner_t::copy_trees (const planner_t &o)
{
    int rc = 0;

    const scheduled_point_t *point = m_p0;
    scheduled_point_t *new_point = nullptr;
    while (point) {
        new_point = new scheduled_point_t ();
        new_point->at = point->at;
        new_point->in_mt_resource_tree = point->in_mt_resource_tree;
        new_point->new_point = point->new_point;
        new_point->ref_count = point->ref_count;
        new_point->scheduled = point->scheduled;
        new_point->remaining = point->remaining;
        m_sched_point_tree.insert (new_point);
        m_mt_resource_tree.insert (new_point);
        point = o.sp_tree_next (point);
    }
    return rc;
}

int planner_t::copy_maps (const planner_t &o)
{
    int rc = 0;

    for (auto const &span_it : o.get_span_lookup_const ()) {
        std::shared_ptr<span_t> new_span = std::make_shared<span_t> ();
        new_span = span_it.second;
        m_span_lookup.emplace (span_it.first, new_span);
    }
    for (auto const &avail_it : o.get_avail_time_iter_const ()) {
        scheduled_point_t *new_avail = new scheduled_point_t ();
        *new_avail = *(avail_it.second);
        m_avail_time_iter.emplace (avail_it.first, new_avail);
    }

    return rc;
}

int planner_t::clear ()
{
    int rc = 0;

    for (auto &kv : get_avail_time_iter_const ())
        m_mt_resource_tree.insert (kv.second);
    m_span_lookup.clear ();
    m_avail_time_iter.clear ();
    if (m_p0 && m_p0->in_mt_resource_tree)
        m_mt_resource_tree.remove (m_p0);
    m_sched_point_tree.destroy ();

    return rc;
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

static void restore_track_points (planner_ctx_t *ctx)
{
    scheduled_point_t *point = nullptr;
    for (auto &kv : ctx->planner->get_avail_time_iter ())
        ctx->planner->mt_tree_insert (kv.second);
    ctx->planner->clear_avail_time_iter ();
}

static void update_mintime_resource_tree (planner_ctx_t *ctx,
                                          std::list<scheduled_point_t *> &list)
{
    scheduled_point_t *point = nullptr;
    for (auto &point : list) {
        if (point->in_mt_resource_tree)
            ctx->planner->mt_tree_remove (point);
        if (point->ref_count && !(point->in_mt_resource_tree))
            ctx->planner->mt_tree_insert (point);
    }
}

static void copy_req (request_t &dest, int64_t on_or_after, uint64_t duration,
                      uint64_t resource_count)
{
    dest.on_or_after = on_or_after;
    dest.duration = duration;
    dest.count = static_cast<int64_t> (resource_count);
}

static scheduled_point_t *get_or_new_point (planner_ctx_t *ctx, int64_t at)
{
    scheduled_point_t *point = nullptr;
    try {
        if ( !(point = ctx->planner->sp_tree_search (at))) {
            scheduled_point_t *state = ctx->planner->sp_tree_get_state (at);
            point = new scheduled_point_t ();
            point->at = at;
            point->in_mt_resource_tree = 0;
            point->new_point = 1;
            point->ref_count = 0;
            point->scheduled = state->scheduled;
            point->remaining = state->remaining;
            ctx->planner->sp_tree_insert (point);
            ctx->planner->mt_tree_insert (point);
        }
    } catch (std::bad_alloc &e) {
        errno = ENOMEM;
    }
    return point;
}

static void fetch_overlap_points (planner_ctx_t *ctx, int64_t at, uint64_t duration,
                                  std::list<scheduled_point_t *> &list)
{
    scheduled_point_t *point = ctx->planner->sp_tree_get_state (at);
    while (point) {
        if (point->at >= static_cast<int64_t> (at + duration))
            break;
        else if (point->at >= at)
            list.push_back (point);
        point = ctx->planner->sp_tree_next (point);
    }
}

static int update_points_add_span (planner_ctx_t *ctx,
                                   std::list<scheduled_point_t *> &list,
                                   std::shared_ptr<span_t> &span)
{
    int rc = 0;
    for (auto &point : list) {
        point->scheduled += span->planned;
        point->remaining -= span->planned;
        if ( (point->scheduled > ctx->planner->get_total_resources ())
              || (point->remaining < 0)) {
            errno = ERANGE;
            rc = -1;
        }
    }
    return rc;
}

static int update_points_subtract_span (planner_ctx_t *ctx,
                                        std::list<scheduled_point_t *> &list,
                                        std::shared_ptr<span_t> &span)
{
    int rc = 0;
    for (auto &point : list) {
        point->scheduled -= span->planned;
        point->remaining += span->planned;
        if ( (point->scheduled < 0)
              || (point->remaining > ctx->planner->get_total_resources ())) {
            errno = ERANGE;
            rc = -1;
        }
    }
    return rc;
}

static bool span_ok (planner_ctx_t *ctx, scheduled_point_t *start_point,
                     uint64_t duration, int64_t request)
{
    bool ok = true;
    scheduled_point_t *next_point = nullptr;
    for (next_point = start_point;
         next_point != nullptr;
         next_point = ctx->planner->sp_tree_next (next_point)) {
         if (next_point->at >= (start_point->at + (int64_t)duration)) {
             ok = true;
             break;
         } else if (request > next_point->remaining) {
             ctx->planner->mt_tree_remove (start_point);
             track_points (ctx->planner->get_avail_time_iter (), start_point);
             ok = false;
             break;
         }
    }
    return ok;
}

static int64_t avail_at (planner_ctx_t *ctx, int64_t on_or_after, uint64_t duration,
                         int64_t request)
{
    int64_t at = -1;
    scheduled_point_t *start_point = nullptr;
    while ((start_point = ctx->planner->mt_tree_get_mintime (request))) {
        at = start_point->at;
        if (at < on_or_after) {
            ctx->planner->mt_tree_remove (start_point);
            track_points (ctx->planner->get_avail_time_iter (), start_point);
            at = -1;

        } else if (span_ok (ctx, start_point, duration, request)) {
            ctx->planner->mt_tree_remove (start_point);
            track_points (ctx->planner->get_avail_time_iter (), start_point);
            if (static_cast<int64_t> (at + duration) > ctx->planner->get_plan_end ())
                at = -1;
            break;
        }
    }
    return at;
}

static bool avail_during (planner_ctx_t *ctx, int64_t at, uint64_t duration,
                          const int64_t request)
{
    bool ok = true;
    if (static_cast<int64_t> (at + duration) > ctx->planner->get_plan_end ()) {
        errno = ERANGE;
        return -1;
    }

    scheduled_point_t *point = ctx->planner->sp_tree_get_state (at);
    while (point) {
        if (point->at >= (at + (int64_t)duration)) {
            ok = true;
            break;
        } else if (request > point->remaining) {
            ok = false;
            break;
        }
        point = ctx->planner->sp_tree_next (point);
    }
    return ok;
}

static scheduled_point_t *avail_resources_during (planner_ctx_t *ctx, int64_t at,
                                                  uint64_t duration)
{
    if (static_cast<int64_t> (at + duration) > ctx->planner->get_plan_end ()) {
        errno = ERANGE;
        return nullptr;
    }

    scheduled_point_t *point = ctx->planner->sp_tree_get_state (at);
    scheduled_point_t *min = point;
    while (point) {
        if (point->at >= (at + (int64_t)duration))
            break;
        else if (min->remaining > point->remaining)
          min = point;
        point = ctx->planner->sp_tree_next (point);
    }
    return min;
}


/*******************************************************************************
 *                                                                             *
 *                              Utilities                                      *
 *                                                                             *
 *******************************************************************************/

static inline void erase (planner_ctx_t *ctx)
{
    ctx->planner->clear_span_lookup ();
    ctx->planner->clear_avail_time_iter ();
    if (ctx->planner->get_p0 () && ctx->planner->get_p0 ()->in_mt_resource_tree)
        ctx->planner->mt_tree_remove (ctx->planner->get_p0 ());
    ctx->planner->destroy_sp_tree ();
}

static inline bool not_feasable (planner_ctx_t *ctx, int64_t start_time,
                                 uint64_t duration, int64_t request)
{
    bool rc = (start_time < ctx->planner->get_plan_start ()) || (duration < 1)
              || (static_cast<int64_t> (start_time + duration - 1)
                     > ctx->planner->get_plan_end ());
    return rc;
}

static int span_input_check (planner_ctx_t *ctx, int64_t start_time,
                             uint64_t duration, int64_t request)
{
    int rc = -1;
    if (!ctx || not_feasable (ctx, start_time, duration, request)) {
        errno = EINVAL;
        goto done;
    } else if (request > ctx->planner->get_total_resources () || request < 0) {
        errno = ERANGE;
        goto done;
    }
    rc = 0;
done:
    return rc;
}

static std::shared_ptr<span_t> span_new (planner_ctx_t *ctx, int64_t start_time,
                                         uint64_t duration, uint64_t request)
{
    std::shared_ptr<span_t> span = nullptr;
    try {
        if (span_input_check (ctx, start_time, duration, (int64_t)request) == -1)
            goto done;
        ctx->planner->incr_span_counter ();
        if (ctx->planner->get_span_lookup ().find (ctx->planner->get_span_counter ())
            != ctx->planner->get_span_lookup ().end ()) {
            errno = EEXIST;
            goto done;
        }
        span = std::make_shared<span_t> ();
        span->start = start_time;
        span->last = start_time + duration;
        span->span_id = ctx->planner->get_span_counter ();
        span->planned = request;
        span->in_system = 0;
        span->start_p = nullptr;
        span->last_p = nullptr;

        // errno = EEXIST condition already checked above
        ctx->planner->span_lookup_insert (span->span_id, span);
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

extern "C" planner_ctx_t *planner_new (int64_t base_time, uint64_t duration,
                                   uint64_t resource_totals,
                                   const char *resource_type)
{
    planner_ctx_t *ctx = nullptr;

    if (duration < 1 || !resource_type) {
        errno = EINVAL;
        goto done;
    } else if (resource_totals > std::numeric_limits<int64_t>::max ()) {
        errno = ERANGE;
        goto done;
    }

    try {
        ctx = new planner_ctx_t ();
        ctx->planner = new planner_t (base_time, duration, resource_totals, resource_type);
    } catch (std::bad_alloc &e) {
        errno = ENOMEM;
    }

done:
    return ctx;
}

extern "C" int planner_reset (planner_ctx_t *ctx,
                              int64_t base_time, uint64_t duration)
{
    int rc = 0;
    if (!ctx || duration < 1) {
        errno = EINVAL;
        return -1;
    }

    uint64_t resource_totals = ctx->planner->get_total_resources ();
    const char *resource_type = ctx->planner->get_resource_type ().c_str ();

    erase (ctx);
    try {
        ctx = new planner_ctx_t ();
        ctx->planner = new planner_t (base_time, duration, resource_totals, resource_type);
    } catch (std::bad_alloc &e) {
        errno = ENOMEM;
        rc = -1;
    }
    return rc;
}

extern "C" void planner_destroy (planner_ctx_t **ctx_p)
{
    if (ctx_p && *ctx_p) {
        restore_track_points (*ctx_p);
        erase (*ctx_p);
        delete *ctx_p;
        *ctx_p = nullptr;
    }
}

extern "C" int64_t planner_base_time (planner_ctx_t *ctx)
{
    if (!ctx) {
        errno = EINVAL;
        return -1;
    }
    return ctx->planner->get_plan_start ();
}

extern "C" int64_t planner_duration (planner_ctx_t *ctx)
{
    if (!ctx) {
        errno = EINVAL;
        return -1;
    }
    return ctx->planner->get_plan_end () - ctx->planner->get_plan_start ();
}

extern "C" int64_t planner_resource_total (planner_ctx_t *ctx)
{
    if (!ctx) {
        errno = EINVAL;
        return -1;
    }
    return ctx->planner->get_total_resources ();
}

extern "C" const char *planner_resource_type (planner_ctx_t *ctx)
{
    if (!ctx) {
        errno = EINVAL;
        return nullptr;
    }
    return ctx->planner->get_resource_type ().c_str ();
}

extern "C" int64_t planner_avail_time_first (planner_ctx_t *ctx,
                                             int64_t on_or_after,
                                             uint64_t duration,
                                             uint64_t request)
{
    int64_t t = -1;
    if (!ctx || on_or_after < ctx->planner->get_plan_start ()
        || on_or_after >= ctx->planner->get_plan_end () || duration < 1) {
        errno = EINVAL;
        return -1;
    }
    if (static_cast<int64_t> (request) > ctx->planner->get_total_resources ()) {
        errno = ERANGE;
        return -1;
    }
    restore_track_points (ctx);
    ctx->planner->set_avail_time_iter_set (1);
    copy_req (ctx->planner->get_current_request (), on_or_after, duration, request);
    if ( (t = avail_at (ctx, on_or_after, duration, (int64_t)request)) == -1)
        errno = ENOENT;
    return t;
}

extern "C" int64_t planner_avail_time_next (planner_ctx_t *ctx)
{
    int64_t t = -1;
    int64_t on_or_after = -1;
    uint64_t duration = 0;
    int64_t request_count = 0;
    if (!ctx || !ctx->planner->get_avail_time_iter_set ()) {
        errno = EINVAL;
        return -1;
    }
    request_count = ctx->planner->get_current_request_const ().count;
    on_or_after = ctx->planner->get_current_request_const ().on_or_after;
    duration = ctx->planner->get_current_request_const ().duration;
    if (request_count > ctx->planner->get_total_resources ()) {
        errno = ERANGE;
        return -1;
    }
    if ( (t = avail_at (ctx, on_or_after, duration, (int64_t)request_count)) ==
          -1)
        errno = ENOENT;
    return t;
}

extern "C" int planner_avail_during (planner_ctx_t *ctx, int64_t start_time,
                                     uint64_t duration, uint64_t request)
{
    bool ok = false;
    if (!ctx || duration < 1) {
        errno = EINVAL;
        return -1;
    }
    if (static_cast<int64_t> (request) > ctx->planner->get_total_resources ()) {
        errno = ERANGE;
        return -1;
    }
    ok = avail_during (ctx, start_time, duration, (int64_t)request);
    return ok? 0 : -1;
}

extern "C" int64_t planner_avail_resources_during (planner_ctx_t *ctx,
                                                   int64_t at, uint64_t duration)
{
    scheduled_point_t *min_point = nullptr;
    if (!ctx || at > ctx->planner->get_plan_end () || duration < 1) {
        errno = EINVAL;
        return -1;
    }
    min_point = avail_resources_during (ctx, at, duration);
    return min_point->remaining;
}

extern "C" int64_t planner_avail_resources_at (planner_ctx_t *ctx, int64_t at)
{
    const scheduled_point_t *state = nullptr;
    if (!ctx || at > ctx->planner->get_plan_end ()) {
        errno = EINVAL;
        return -1;
    }
    state = ctx->planner->sp_tree_get_state (at);
    return state->remaining;
}

extern "C" int64_t planner_add_span (planner_ctx_t *ctx, int64_t start_time,
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
    ctx->planner->set_avail_time_iter_set (0);

    return span->span_id;
}

extern "C" int planner_rem_span (planner_ctx_t *ctx, int64_t span_id)
{
    int rc = -1;
    uint64_t duration = 0;
    std::map<int64_t, std::shared_ptr<span_t>>::iterator it;

    if (!ctx) {
        errno = EINVAL;
        return -1;
    }
    it = ctx->planner->get_span_lookup ().find (span_id);
    if (it == ctx->planner->get_span_lookup ().end ()) {
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
        ctx->planner->sp_tree_remove (span->start_p);
        if (span->start_p->in_mt_resource_tree)
            ctx->planner->mt_tree_remove (span->start_p);
        delete span->start_p;
        span->start_p = nullptr;
    }
    if (span->last_p->ref_count == 0) {
        ctx->planner->sp_tree_remove (span->last_p);
        if (span->last_p->in_mt_resource_tree)
            ctx->planner->mt_tree_remove (span->last_p);
        delete span->last_p;
        span->last_p = nullptr;
    }
    ctx->planner->span_lookup_erase (it);
    list.clear ();
    ctx->planner->set_avail_time_iter_set (0);
    rc = 0;

done:
    return rc;
}

extern "C" int64_t planner_span_first (planner_ctx_t *ctx)
{
    if (!ctx) {
        errno = EINVAL;
        return -1;
    }
    auto span_lookup_begin = ctx->planner->get_span_lookup ().begin ();
    ctx->planner->set_span_lookup_iter (span_lookup_begin);
    if (ctx->planner->get_span_lookup_iter () == ctx->planner->get_span_lookup ().end ()) {
        errno = EINVAL;
        return -1;
    }
    std::shared_ptr<span_t> &span = ctx->planner->get_span_lookup_iter ()->second;
    return span->span_id;
}

extern "C" int64_t planner_span_next (planner_ctx_t *ctx)
{
    if (!ctx) {
        errno = EINVAL;
        return -1;
    }
    ctx->planner->incr_span_lookup_iter ();
    if (ctx->planner->get_span_lookup_iter () == ctx->planner->get_span_lookup ().end ()) {
        errno = EINVAL;
        return -1;
    }
    std::shared_ptr<span_t> &span = ctx->planner->get_span_lookup_iter ()->second;
    return span->span_id;
}

extern "C" size_t planner_span_size (planner_ctx_t *ctx)
{
    if (!ctx) {
        errno = EINVAL;
        return 0;
    }
    return ctx->planner->span_lookup_get_size ();
}

extern "C" bool planner_is_active_span (planner_ctx_t *ctx, int64_t span_id)
{
    if (!ctx) {
        errno = EINVAL;
        return false;
    }
    auto it = ctx->planner->get_span_lookup ().find (span_id);
    if (ctx->planner->get_span_lookup ().find (span_id) == ctx->planner->get_span_lookup ().end ()) {
        errno = EINVAL;
        return false;
    }
    std::shared_ptr<span_t> &span = it->second;
    return (span->in_system)? true : false;
}

extern "C" int64_t planner_span_start_time (planner_ctx_t *ctx, int64_t span_id)
{
    if (!ctx) {
        errno = EINVAL;
        return -1;
    }
    auto it = ctx->planner->get_span_lookup ().find (span_id);
    if (ctx->planner->get_span_lookup ().find (span_id) == ctx->planner->get_span_lookup ().end ()) {
        errno = EINVAL;
        return -1;
    }
    std::shared_ptr<span_t> &span = it->second;
    return span->start;
}

extern "C" int64_t planner_span_duration (planner_ctx_t *ctx, int64_t span_id)
{
    if (!ctx) {
        errno = EINVAL;
        return -1;
    }
    auto it = ctx->planner->get_span_lookup ().find (span_id);
    if (ctx->planner->get_span_lookup ().find (span_id) == ctx->planner->get_span_lookup ().end ()) {
        errno = EINVAL;
        return -1;
    }
    std::shared_ptr<span_t> &span = it->second;
    return (span->last - span->start);
}

extern "C" int64_t planner_span_resource_count (planner_ctx_t *ctx, int64_t span_id)
{
    if (!ctx) {
        errno = EINVAL;
        return -1;
    }
    auto it = ctx->planner->get_span_lookup ().find (span_id);
    if (ctx->planner->get_span_lookup ().find (span_id) == ctx->planner->get_span_lookup ().end ()) {
        errno = EINVAL;
        return -1;
    }
    std::shared_ptr<span_t> &span = it->second;
    return span->planned;
}

/*
 * vi: ts=4 sw=4 expandtab
 */
