/*****************************************************************************\
 * Copyright 2023 Lawrence Livermore National Security, LLC
 * (c.f. AUTHORS, NOTICE.LLNS, LICENSE)
 *
 * This file is part of the Flux resource manager framework.
 * For details, see https://github.com/flux-framework.
 *
 * SPDX-License-Identifier: LGPL-3.0
\*****************************************************************************/

#include <cstdlib>
#include <cerrno>
#include <cstring>
#include <limits>
#include <vector>
#include <map>

#include "planner_multi.h"


/****************************************************************************
 *                                                                          *
 *              Planner Multi and Resource Update APIs                      *
 *                                                                          *
 ****************************************************************************/

static void fill_iter_request (planner_multi_t *ctx, struct request_multi *iter,
                               int64_t at, uint64_t duration,
                               const uint64_t *resources, size_t len)
{
    size_t i;
    iter->on_or_after = at;
    iter->duration = duration;
    for (i = 0; i < len; ++i)
        iter->counts[i] = resources[i];
}

extern "C" planner_multi_t *planner_multi_new (
                                int64_t base_time, uint64_t duration,
                                const uint64_t *resource_totals,
                                const char **resource_types, size_t len)
{
    size_t i = 0;
    planner_multi_t *ctx = nullptr;

    if (duration < 1 || !resource_totals || !resource_types) {
        errno = EINVAL;
        goto error;
    } else {
        for (i = 0; i < len; ++i) {
            if (resource_totals[i] > std::numeric_limits<int64_t>::max ()) {
                errno = ERANGE;
                goto error;
            }
        }
    }

    try {
        ctx = new planner_multi_t ();
        ctx->plan_multi = nullptr;
        ctx->plan_multi = new planner_multi (base_time, duration, resource_totals,
                                             resource_types, len);
    } catch (std::bad_alloc &e) {
        goto nomem_error;
    }
    return ctx;

nomem_error:
    errno = ENOMEM;
    planner_multi_destroy (&ctx);
error:
    return ctx;
}

extern "C" planner_multi_t *planner_multi_empty ()
{
    planner_multi_t *ctx = nullptr;

    try {
        ctx = new planner_multi_t ();
        ctx->plan_multi = nullptr;
        ctx->plan_multi = new planner_multi ();
    } catch (std::bad_alloc &e) {
        goto nomem_error;
    }
    return ctx;

nomem_error:
    errno = ENOMEM;
    planner_multi_destroy (&ctx);
    return ctx;
}

extern "C" planner_multi_t *planner_multi_copy (planner_multi_t *mp)
{
    planner_multi_t *ctx = nullptr;

    try {
        ctx = new planner_multi_t ();
        ctx->plan_multi = nullptr;
        ctx->plan_multi = new planner_multi (*(mp->plan_multi));
    } catch (std::bad_alloc &e) {
        errno = ENOMEM;
        goto nomem_error;
    }
    return ctx;

nomem_error:
    errno = ENOMEM;
    planner_multi_destroy (&ctx);
    return ctx;
}

extern "C" int64_t planner_multi_base_time (planner_multi_t *ctx)
{
    if (!ctx) {
        errno = EINVAL;
        return -1;
    }
    return planner_base_time (ctx->plan_multi->get_planners_at (0));
}

extern "C" int64_t planner_multi_duration (planner_multi_t *ctx)
{
    if (!ctx) {
        errno = EINVAL;
        return -1;
    }
    return planner_duration (ctx->plan_multi->get_planners_at (0));
}

extern "C" size_t planner_multi_resources_len (planner_multi_t *ctx)
{
    if (!ctx) {
        errno = EINVAL;
        return 0;
    }
    return ctx->plan_multi->get_planners_size ();
}

extern "C" const char **planner_multi_resource_types (planner_multi_t *ctx)
{
    if (!ctx) {
        errno = EINVAL;
        return nullptr;
    }
    return &(ctx->plan_multi->m_resource_types[0]);
}

extern "C" const uint64_t *planner_multi_resource_totals (planner_multi_t *ctx)
{
    if (!ctx) {
        errno = EINVAL;
        return nullptr;
    }
    return &(ctx->plan_multi->m_resource_totals[0]);
}

extern "C" int64_t planner_multi_resource_total_at (planner_multi_t *ctx,
                                                    unsigned int i)
{
    int64_t rc = -1;
    if (ctx) {
        if (i >= ctx->plan_multi->get_planners_size ()) {
            errno = EINVAL;
            goto done;
        }
        rc = planner_resource_total (ctx->plan_multi->get_planners_at (i));
    }
done:
    return rc;
}

extern "C" int64_t planner_multi_resource_total_by_type (
                       planner_multi_t *ctx, const char *resource_type)
{
    size_t i = 0;
    int64_t rc = -1;
    if (!ctx || !resource_type)
        goto done;
    for (i = 0; i < ctx->plan_multi->get_planners_size (); i++) {
        if ( !(strcmp (ctx->plan_multi->get_resource_types_at (i), resource_type))) {
            rc = planner_resource_total (ctx->plan_multi->get_planners_at (i));
            break;
        }
    }
    if (i == ctx->plan_multi->get_planners_size ())
        errno = EINVAL;
done:
    return rc;
}

extern "C" int planner_multi_reset (planner_multi_t *ctx,
                                    int64_t base_time, uint64_t duration)
{
    size_t i = 0;
    int rc = -1;
    if (!ctx || duration < 1) {
        errno = EINVAL;
        goto done;
    }

    for (i = 0; i < ctx->plan_multi->get_planners_size (); ++i)
        if (planner_reset (ctx->plan_multi->get_planners_at (i), base_time, duration) == -1)
            goto done;

    rc = 0;
done:
    return rc;
}

extern "C" void planner_multi_destroy (planner_multi_t **ctx_p)
{
    if (ctx_p && *ctx_p) {
        delete (*ctx_p)->plan_multi;
        (*ctx_p)->plan_multi = nullptr;
        delete *ctx_p;
        *ctx_p = nullptr;
    }
}

extern "C" planner_t *planner_multi_planner_at (planner_multi_t *ctx,
                                                unsigned int i)
{
    planner_t *planner = nullptr;
    if (!ctx || i >= ctx->plan_multi->get_planners_size ()) {
        errno = EINVAL;
        goto done;
    }
    planner = ctx->plan_multi->get_planners_at (i);
done:
    return planner;
}

extern "C" int64_t planner_multi_avail_time_first (
                       planner_multi_t *ctx, int64_t on_or_after,
                       uint64_t duration,
                       const uint64_t *resource_requests, size_t len)
{
    size_t i = 0;
    int unmet = 0;
    int64_t t = -1;

    if (!ctx || !resource_requests || ctx->plan_multi->get_planners_size () < 1
         || ctx->plan_multi->get_planners_size () != len) {
        errno = EINVAL;
        goto done;
    }

    fill_iter_request (ctx, &(ctx->plan_multi->get_iter ()),
                       on_or_after, duration, resource_requests, len);

    if ((t = planner_avail_time_first (ctx->plan_multi->get_planners_at (0), on_or_after,
                                       duration, resource_requests[0])) == -1)
        goto done;

    do {
        unmet = 0;
        for (i = 1; i < ctx->plan_multi->get_planners_size (); ++i) {
            if ((unmet = planner_avail_during (ctx->plan_multi->get_planners_at (i),
                                               t, duration,
                                               resource_requests[i])) == -1)
                break;
        }
    } while (unmet && (t = planner_avail_time_next (ctx->plan_multi->get_planners_at (0))) != -1);

done:
    return t;
}

extern "C" int64_t planner_multi_avail_time_next (planner_multi_t *ctx)
{
    size_t i = 0;
    int unmet = 0;
    int64_t t = -1;

    if (!ctx) {
        errno = EINVAL;
        goto done;
    }

    do {
        unmet = 0;
        if ((t = planner_avail_time_next (ctx->plan_multi->get_planners_at (0))) == -1)
            break;
        for (i = 1; i < ctx->plan_multi->get_planners_size (); ++i) {
            if ((unmet = planner_avail_during (ctx->plan_multi->get_planners_at (i), t,
                                               ctx->plan_multi->get_iter ().duration,
                                               ctx->plan_multi->get_iter ().counts[i])) == -1)
                break;
        }
    } while (unmet);

done:
    return t;
}

extern "C" int64_t planner_multi_avail_resources_at (
                       planner_multi_t *ctx, int64_t at, unsigned int i)
{
    if (!ctx || i >= ctx->plan_multi->get_planners_size ()) {
        errno = EINVAL;
        return -1;
    }
    return planner_avail_resources_at (ctx->plan_multi->get_planners_at (i), at);
}

extern "C" int planner_multi_avail_resources_array_at (
                   planner_multi_t *ctx, int64_t at,
                   int64_t *resource_counts, size_t len)
{
    size_t i = 0;
    int64_t rc = 0;
    if (!ctx || !resource_counts || ctx->plan_multi->get_planners_size () != len) {
        errno = EINVAL;
        return -1;
    }
    for (i = 0; i < ctx->plan_multi->get_planners_size (); ++i) {
        rc = planner_avail_resources_at (ctx->plan_multi->get_planners_at (i), at);
        if (rc == -1)
            break;
        resource_counts[i] = rc;
    }
    return (rc == -1)? -1 : 0;
}

extern "C" int planner_multi_avail_during (
                   planner_multi_t *ctx, int64_t at, uint64_t duration,
                   const uint64_t *resource_requests, size_t len)
{
    size_t i = 0;
    int rc = 0;
    if (!ctx || !resource_requests || ctx->plan_multi->get_planners_size () != len) {
        errno = EINVAL;
        return -1;
    }
    for (i = 0; i < ctx->plan_multi->get_planners_size (); ++i) {
        rc = planner_avail_during (ctx->plan_multi->get_planners_at (i), at, duration,
                                   resource_requests[i]);
        if (rc == -1)
            break;
    }
    return rc;
}

extern "C" int planner_multi_avail_resources_array_during (
                   planner_multi_t *ctx, int64_t at,
                   uint64_t duration, int64_t *resource_counts, size_t len)
{
    size_t i = 0;
    int64_t rc = 0;
    if (!ctx || !resource_counts
        || ctx->plan_multi->get_planners_size () < 1 || ctx->plan_multi->get_planners_size () != len) {
        errno = EINVAL;
        return -1;
    }
    for (i = 0; i < ctx->plan_multi->get_planners_size (); ++i) {
        rc = planner_avail_resources_during (ctx->plan_multi->get_planners_at (i), at, duration);
        if (rc == -1)
            break;
        resource_counts[i] = rc;
    }
    return (rc == -1)? -1 : 0;
}

extern "C" int64_t planner_multi_add_span (
                       planner_multi_t *ctx, int64_t start_time,
                       uint64_t duration,
                       const uint64_t *resource_requests, size_t len)
{
    size_t i = 0;
    int64_t span = -1;
    int64_t mspan = -1;

    if (!ctx || !resource_requests || len != ctx->plan_multi->get_planners_size ())
        return -1;

    mspan = ctx->plan_multi->get_span_counter ();
    auto res = ctx->plan_multi->get_span_lookup ().insert (
                        std::pair<int64_t, std::vector<int64_t>> (
                            mspan, std::vector<int64_t> ()));
    if (!res.second) {
        errno = EEXIST;
        return -1;
    }

    ctx->plan_multi->incr_span_counter ();

    for (i = 0; i < len; ++i) {
        if ( (span = planner_add_span (ctx->plan_multi->get_planners_at (i),
                                       start_time, duration,
                                       resource_requests[i])) == -1) {
            ctx->plan_multi->get_span_lookup ().erase (mspan);
            return -1;
        }
        ctx->plan_multi->get_span_lookup ()[mspan].push_back (span);
    }
    return mspan;
}

extern "C" int planner_multi_rem_span (planner_multi_t *ctx, int64_t span_id)
{
    size_t i;
    int rc = -1;

    if (!ctx || span_id < 0) {
        errno = EINVAL;
        return -1;
    }
    auto it = ctx->plan_multi->get_span_lookup ().find (span_id);
    if (it == ctx->plan_multi->get_span_lookup ().end ()) {
        errno = ENOENT;
        goto done;
    }
    for (i = 0; i < it->second.size (); ++i) {
        if (planner_rem_span (ctx->plan_multi->get_planners_at (i), it->second[i]) == -1)
            goto done;
    }
    ctx->plan_multi->get_span_lookup ().erase (it);
    rc  = 0;
done:
    return rc;
}

int64_t planner_multi_span_first (planner_multi_t *ctx)
{
    int64_t rc = -1;
    std::map<uint64_t, std::vector<int64_t>>::iterator tmp_it = ctx->plan_multi->get_span_lookup ().begin ();
    if (!ctx) {
        errno = EINVAL;
        goto done;
    }
    ctx->plan_multi->set_span_lookup_iter (tmp_it);
    if (ctx->plan_multi->get_span_lookup_iter () == ctx->plan_multi->get_span_lookup ().end ()) {
        errno = ENOENT;
        goto done;

    }
    rc = ctx->plan_multi->get_span_lookup_iter ()->first;
done:
    return rc;
}

extern "C" int64_t planner_multi_span_next (planner_multi_t *ctx)
{
    int64_t rc = -1;
    if (!ctx) {
        errno = EINVAL;
        goto done;
    }
    ctx->plan_multi->incr_span_lookup_iter ();
    if (ctx->plan_multi->get_span_lookup_iter () == ctx->plan_multi->get_span_lookup ().end ()) {
        errno = ENOENT;
        goto done;

    }
    rc = ctx->plan_multi->get_span_lookup_iter ()->first;
done:
    return rc;
}

extern "C" size_t planner_multi_span_size (planner_multi_t *ctx)
{
   if (!ctx) {
        errno = EINVAL;
        return 0;
    }
    return ctx->plan_multi->get_span_lookup ().size ();
}

extern "C" bool planner_multis_equal (planner_multi_t *lhs,
                                      planner_multi_t *rhs)
{
    return (*(lhs->plan_multi) == *(rhs->plan_multi));
}

/*
 * vi: ts=4 sw=4 expandtab
 */
