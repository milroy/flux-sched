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

planner_t &planner_t::operator= (const planner_t &o)
{
    scheduled_point_t *sp_point = nullptr;
    for (auto &kv : avail_time_iter)
        mt_resource_tree.insert (kv.second);
    span_lookup.clear ();
    avail_time_iter.clear ();
    if (p0 && p0->in_mt_resource_tree)
        mt_resource_tree.remove (p0);
    sched_point_tree.destroy ();

    total_resources = o.total_resources;
    resource_type = o.resource_type;
    plan_start = o.plan_start;
    plan_end = o.plan_end;
    if (!p0)
        p0 = new scheduled_point_t ();
    *p0 = *(o.p0);
    // p0->at = o.p0->at;
    // p0->ref_count = o.p0->ref_count;
    // p0->remaining = o.p0->remaining;
    // p0->point_rb = o.p0->point_rb; /* BST node for scheduled point tree */
    // p0->resource_rb = o.p0->resource_rb;  /* BST node for min-time resource tree */           /* Resource-state changing time */
    // p0->in_mt_resource_tree = o.p0->in_mt_resource_tree;     /* 1 when inserted in min-time resource tree */
    // p0->new_point = o.p0->new_point;               /* 1 when this point is newly created */         /* reference counter */
    // p0->scheduled = o.p0->scheduled;           /* scheduled quantity at this point */
    const scheduled_point_t *point = p0;
    scheduled_point_t *new_point = nullptr;
    while (point) {
        new_point = new scheduled_point_t ();
        new_point->at = point->at;
        new_point->in_mt_resource_tree = point->in_mt_resource_tree;
        new_point->new_point = point->new_point;
        new_point->ref_count = point->ref_count;
        new_point->scheduled = point->scheduled;
        new_point->remaining = point->remaining;
        sched_point_tree.insert (new_point);
        mt_resource_tree.insert (new_point);
        point = o.sched_point_tree.next (point);
    }
    for (auto const &span_it : o.span_lookup) {
        //std::shared_ptr<span_t> new_second = std::make_shared<span_t> (span_it.second);
        span_lookup[span_it.first] = span_it.second;
    }
    for (auto const &avail_it : o.avail_time_iter) {
        //scheduled_point_t *new_second2 = new scheduled_point_t ();
        //new_second2 = avail_it.second;
        avail_time_iter[avail_it.first] = avail_it.second;
    }
    current_request = o.current_request;
    avail_time_iter_set = o.avail_time_iter_set;
    span_counter = o.span_counter;
    return *this;
}

planner_t::planner_t ()
{
    total_resources = 0;
    resource_type = "";
    plan_start = 0;
    plan_end = 0;
    //p0 = nullptr;
    p0 = new scheduled_point_t ();
    p0->at = 0;
    p0->ref_count = 1;
    p0->remaining = 0;
    //sched_point_tree.insert (p0);
    //mt_resource_tree.insert (p0);
    avail_time_iter_set = 0;
    span_counter = 0;
}

planner_t::planner_t (const int64_t base_time, const uint64_t duration,
            const uint64_t resource_totals, const char *in_resource_type)
{
    total_resources = static_cast<int64_t> (resource_totals);
    resource_type = in_resource_type;
    plan_start = base_time;
    plan_end = base_time + static_cast<int64_t> (duration);
    p0 = new scheduled_point_t ();
    p0->at = base_time;
    p0->ref_count = 1;
    p0->remaining = total_resources;
    sched_point_tree.insert (p0);
    mt_resource_tree.insert (p0);
    avail_time_iter_set = 0;
    span_counter = 0;
}

planner_t::planner_t (const planner_t &o)
{
    scheduled_point_t *sp_point = nullptr;
    for (auto &kv : avail_time_iter)
        mt_resource_tree.insert (kv.second);
    span_lookup.clear ();
    avail_time_iter.clear ();
    if (p0 && p0->in_mt_resource_tree)
        mt_resource_tree.remove (p0);
    sched_point_tree.destroy ();

    total_resources = o.total_resources;
    resource_type = o.resource_type;
    plan_start = o.plan_start;
    plan_end = o.plan_end;
    if (!p0)
        p0 = new scheduled_point_t ();
    *p0 = *(o.p0);
    // p0->at = o.p0->at;
    // p0->ref_count = o.p0->ref_count;
    // p0->remaining = o.p0->remaining;
    // p0->point_rb = o.p0->point_rb; /* BST node for scheduled point tree */
    // p0->resource_rb = o.p0->resource_rb;  /* BST node for min-time resource tree */           /* Resource-state changing time */
    // p0->in_mt_resource_tree = o.p0->in_mt_resource_tree;     /* 1 when inserted in min-time resource tree */
    // p0->new_point = o.p0->new_point;               /* 1 when this point is newly created */         /* reference counter */
    // p0->scheduled = o.p0->scheduled;           /* scheduled quantity at this point */
    const scheduled_point_t *point = p0;
    scheduled_point_t *new_point = nullptr;
    while (point) {
        new_point = new scheduled_point_t ();
        new_point->at = point->at;
        new_point->in_mt_resource_tree = point->in_mt_resource_tree;
        new_point->new_point = point->new_point;
        new_point->ref_count = point->ref_count;
        new_point->scheduled = point->scheduled;
        new_point->remaining = point->remaining;
        sched_point_tree.insert (new_point);
        mt_resource_tree.insert (new_point);
        point = o.sched_point_tree.next (point);
    }
    for (auto const &span_it : o.span_lookup) {
        //std::shared_ptr<span_t> new_second = std::make_shared<span_t> (span_it.second);
        span_lookup[span_it.first] = span_it.second;
    }
    for (auto const &avail_it : o.avail_time_iter) {
        //scheduled_point_t *new_second2 = new scheduled_point_t ();
        //new_second2 = avail_it.second;
        avail_time_iter[avail_it.first] = avail_it.second;
    }
    current_request = o.current_request;
    avail_time_iter_set = o.avail_time_iter_set;
    span_counter = o.span_counter;
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
    scheduled_point_t *point = nullptr;
    for (auto &kv : ctx->avail_time_iter)
        ctx->mt_resource_tree.insert (kv.second);
    ctx->avail_time_iter.clear ();
}

static void update_mintime_resource_tree (planner_t *ctx,
                                          std::list<scheduled_point_t *> &list)
{
    scheduled_point_t *point = nullptr;
    for (auto &point : list) {
        if (point->in_mt_resource_tree)
            ctx->mt_resource_tree.remove (point);
        if (point->ref_count && !(point->in_mt_resource_tree))
            ctx->mt_resource_tree.insert (point);
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
        if ( !(point = ctx->sched_point_tree.search (at))) {
            scheduled_point_t *state = ctx->sched_point_tree.get_state (at);
            point = new scheduled_point_t ();
            point->at = at;
            point->in_mt_resource_tree = 0;
            point->new_point = 1;
            point->ref_count = 0;
            point->scheduled = state->scheduled;
            point->remaining = state->remaining;
            ctx->sched_point_tree.insert (point);
            ctx->mt_resource_tree.insert (point);
        }
    } catch (std::bad_alloc &e) {
        errno = ENOMEM;
    }
    return point;
}

static void fetch_overlap_points (planner_t *ctx, int64_t at, uint64_t duration,
                                  std::list<scheduled_point_t *> &list)
{
    scheduled_point_t *point = ctx->sched_point_tree.get_state (at);
    while (point) {
        if (point->at >= static_cast<int64_t> (at + duration))
            break;
        else if (point->at >= at)
            list.push_back (point);
        point = ctx->sched_point_tree.next (point);
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
        if ( (point->scheduled > ctx->total_resources)
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
              || (point->remaining > ctx->total_resources)) {
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
         next_point = ctx->sched_point_tree.next (next_point)) {
         if (next_point->at >= (start_point->at + (int64_t)duration)) {
             ok = true;
             break;
         } else if (request > next_point->remaining) {
             ctx->mt_resource_tree.remove (start_point);
             track_points (ctx->avail_time_iter, start_point);
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
    while ((start_point = ctx->mt_resource_tree.get_mintime (request))) {
        at = start_point->at;
        if (at < on_or_after) {
            ctx->mt_resource_tree.remove (start_point);
            track_points (ctx->avail_time_iter, start_point);
            at = -1;

        } else if (span_ok (ctx, start_point, duration, request)) {
            ctx->mt_resource_tree.remove (start_point);
            track_points (ctx->avail_time_iter, start_point);
            if (static_cast<int64_t> (at + duration) > ctx->plan_end)
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
    if (static_cast<int64_t> (at + duration) > ctx->plan_end) {
        errno = ERANGE;
        return -1;
    }

    scheduled_point_t *point = ctx->sched_point_tree.get_state (at);
    while (point) {
        if (point->at >= (at + (int64_t)duration)) {
            ok = true;
            break;
        } else if (request > point->remaining) {
            ok = false;
            break;
        }
        point = ctx->sched_point_tree.next (point);
    }
    return ok;
}

static scheduled_point_t *avail_resources_during (planner_t *ctx, int64_t at,
                                                  uint64_t duration)
{
    if (static_cast<int64_t> (at + duration) > ctx->plan_end) {
        errno = ERANGE;
        return nullptr;
    }

    scheduled_point_t *point = ctx->sched_point_tree.get_state (at);
    scheduled_point_t *min = point;
    while (point) {
        if (point->at >= (at + (int64_t)duration))
            break;
        else if (min->remaining > point->remaining)
          min = point;
        point = ctx->sched_point_tree.next (point);
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
    ctx->span_lookup.clear ();
    ctx->avail_time_iter.clear ();
    if (ctx->p0 && ctx->p0->in_mt_resource_tree)
        ctx->mt_resource_tree.remove (ctx->p0);
    ctx->sched_point_tree.destroy ();
}

static inline bool not_feasable (planner_t *ctx, int64_t start_time,
                                 uint64_t duration, int64_t request)
{
    bool rc = (start_time < ctx->plan_start) || (duration < 1)
              || (static_cast<int64_t> (start_time + duration - 1)
                     > ctx->plan_end);
    return rc;
}

static int span_input_check (planner_t *ctx, int64_t start_time,
                             uint64_t duration, int64_t request)
{
    int rc = -1;
    if (!ctx || not_feasable (ctx, start_time, duration, request)) {
        errno = EINVAL;
        goto done;
    } else if (request > ctx->total_resources || request < 0) {
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
        ctx->span_counter++;
        if (ctx->span_lookup.find (ctx->span_counter)
            != ctx->span_lookup.end ()) {
            errno = EEXIST;
            goto done;
        }
        span = std::make_shared<span_t> ();
        span->start = start_time;
        span->last = start_time + duration;
        span->span_id = ctx->span_counter;
        span->planned = request;
        span->in_system = 0;
        span->start_p = nullptr;
        span->last_p = nullptr;

        // errno = EEXIST condition already checked above
        ctx->span_lookup.insert (std::pair<int64_t, std::shared_ptr<span_t>> (
                                     span->span_id, span));
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
        ctx = new planner_t (base_time, duration, resource_totals, resource_type);
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

    uint64_t resource_totals = ctx->total_resources;
    const char *resource_type = ctx->resource_type.c_str ();

    erase (ctx);
    try {
        ctx = new planner_t (base_time, duration, resource_totals, resource_type);
    } catch (std::bad_alloc &e) {
        errno = ENOMEM;
        rc = -1;
    }
    return rc;
}

extern "C" void planner_destroy (planner_t **ctx_p)
{
    if (ctx_p && *ctx_p) {
        restore_track_points (*ctx_p);
        erase (*ctx_p);
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
    return ctx->plan_start;
}

extern "C" int64_t planner_duration (planner_t *ctx)
{
    if (!ctx) {
        errno = EINVAL;
        return -1;
    }
    return ctx->plan_end - ctx->plan_start;
}

extern "C" int64_t planner_resource_total (planner_t *ctx)
{
    if (!ctx) {
        errno = EINVAL;
        return -1;
    }
    return ctx->total_resources;
}

extern "C" const char *planner_resource_type (planner_t *ctx)
{
    if (!ctx) {
        errno = EINVAL;
        return nullptr;
    }
    return ctx->resource_type.c_str ();
}

extern "C" int64_t planner_avail_time_first (planner_t *ctx,
                                             int64_t on_or_after,
                                             uint64_t duration,
                                             uint64_t request)
{
    int64_t t = -1;
    if (!ctx || on_or_after < ctx->plan_start
        || on_or_after >= ctx->plan_end || duration < 1) {
        errno = EINVAL;
        return -1;
    }
    if (static_cast<int64_t> (request) > ctx->total_resources) {
        errno = ERANGE;
        return -1;
    }
    restore_track_points (ctx);
    ctx->avail_time_iter_set = 1;
    copy_req (ctx->current_request, on_or_after, duration, request);
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
    if (!ctx || !ctx->avail_time_iter_set) {
        errno = EINVAL;
        return -1;
    }
    request_count = ctx->current_request.count;
    on_or_after = ctx->current_request.on_or_after;
    duration = ctx->current_request.duration;
    if (request_count > ctx->total_resources) {
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
    if (static_cast<int64_t> (request) > ctx->total_resources) {
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
    if (!ctx || at > ctx->plan_end || duration < 1) {
        errno = EINVAL;
        return -1;
    }
    min_point = avail_resources_during (ctx, at, duration);
    return min_point->remaining;
}

extern "C" int64_t planner_avail_resources_at (planner_t *ctx, int64_t at)
{
    scheduled_point_t *state = nullptr;
    if (!ctx || at > ctx->plan_end) {
        errno = EINVAL;
        return -1;
    }
    state = ctx->sched_point_tree.get_state (at);
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
    ctx->avail_time_iter_set = 0;

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
    it = ctx->span_lookup.find (span_id);
    if (it == ctx->span_lookup.end ()) {
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
        ctx->sched_point_tree.remove (span->start_p);
        if (span->start_p->in_mt_resource_tree)
            ctx->mt_resource_tree.remove (span->start_p);
        delete span->start_p;
        span->start_p = nullptr;
    }
    if (span->last_p->ref_count == 0) {
        ctx->sched_point_tree.remove (span->last_p);
        if (span->last_p->in_mt_resource_tree)
            ctx->mt_resource_tree.remove (span->last_p);
        delete span->last_p;
        span->last_p = nullptr;
    }
    ctx->span_lookup.erase (it);
    list.clear ();
    ctx->avail_time_iter_set = 0;
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
    ctx->span_lookup_iter  = ctx->span_lookup.begin ();
    if (ctx->span_lookup_iter == ctx->span_lookup.end ()) {
        errno = EINVAL;
        return -1;
    }
    std::shared_ptr<span_t> &span = ctx->span_lookup_iter->second;
    return span->span_id;
}

extern "C" int64_t planner_span_next (planner_t *ctx)
{
    if (!ctx) {
        errno = EINVAL;
        return -1;
    }
    ctx->span_lookup_iter++;
    if (ctx->span_lookup_iter == ctx->span_lookup.end ()) {
        errno = EINVAL;
        return -1;
    }
    std::shared_ptr<span_t> &span = ctx->span_lookup_iter->second;
    return span->span_id;
}

extern "C" size_t planner_span_size (planner_t *ctx)
{
    if (!ctx) {
        errno = EINVAL;
        return 0;
    }
    return ctx->span_lookup.size ();
}

extern "C" bool planner_is_active_span (planner_t *ctx, int64_t span_id)
{
    if (!ctx) {
        errno = EINVAL;
        return false;
    }
    auto it = ctx->span_lookup.find (span_id);
    if (ctx->span_lookup.find (span_id) == ctx->span_lookup.end ()) {
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
    auto it = ctx->span_lookup.find (span_id);
    if (ctx->span_lookup.find (span_id) == ctx->span_lookup.end ()) {
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
    auto it = ctx->span_lookup.find (span_id);
    if (ctx->span_lookup.find (span_id) == ctx->span_lookup.end ()) {
        errno = EINVAL;
        return -1;
    }
    std::shared_ptr<span_t> &span = it->second;
    return (span->last - span->start);
}

extern "C" int64_t planner_span_resource_count (planner_t *ctx, int64_t span_id)
{
    if (!ctx) {
        errno = EINVAL;
        return -1;
    }
    auto it = ctx->span_lookup.find (span_id);
    if (ctx->span_lookup.find (span_id) == ctx->span_lookup.end ()) {
        errno = EINVAL;
        return -1;
    }
    std::shared_ptr<span_t> &span = it->second;
    return span->planned;
}

/*
 * vi: ts=4 sw=4 expandtab
 */
