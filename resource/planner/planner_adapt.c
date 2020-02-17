/*****************************************************************************\
 *  Copyright (c) 2014 Lawrence Livermore National Security, LLC.  Produced at
 *  the Lawrence Livermore National Laboratory (cf, AUTHORS, DISCLAIMER.LLNS).
 *  LLNL-CODE-658032 All rights reserved.
 *
 *  This file is part of the Flux resource manager framework.
 *  For details, see https://github.com/flux-framework.
 *
 *  This program is free software; you can redistribute it and/or modify it
 *  under the terms of the GNU General Public License as published by the Free
 *  Software Foundation; either version 2 of the license, or (at your option)
 *  any later version.
 *
 *  Flux is distributed in the hope that it will be useful, but WITHOUT
 *  ANY WARRANTY; without even the IMPLIED WARRANTY OF MERCHANTABILITY or
 *  FITNESS FOR A PARTICULAR PURPOSE.  See the terms and conditions of the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License along
 *  with this program; if not, write to the Free Software Foundation, Inc.,
 *  59 Temple Place, Suite 330, Boston, MA 02111-1307 USA.
 *  See also:  http://www.gnu.org/licenses/
\*****************************************************************************/

#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <czmq.h>

#include "src/common/libutil/xzmalloc.h"
#include "planner_adapt.h"

struct request {
    int64_t on_or_after;
    uint64_t duration;
    int64_t *counts;
};

struct planner_adapt {
    uint64_t total_resources;
    char *resource_type;
    char **job_types;
    size_t size;
    struct request iter;
    zhashx_t *planner_lookup;
};

static void planner_free_wrap (void *o)
{
    planner_t *planner = (planner_t *)o;
    if (planner)
        planner_destroy (&planner);
}

planner_adapt_t *planner_adapt_new (int64_t base_time, uint64_t duration,
                                    const uint64_t total_resources,
                                    const char *resource_type,
                                    const char **job_types,
                                    size_t len)
{
    planner_adapt_t *ctx = NULL;

    if (duration < 1 || !total_resources || !resource_type
        || !job_types) {
        errno = EINVAL;
        goto done;
    }

    ctx = xzmalloc (sizeof (*ctx));
    ctx->total_resources = total_resources;
    ctx->resource_type = xstrdup (resource_type);
    ctx->job_types = xzmalloc (len * sizeof (*(ctx->job_types)));
    ctx->size = len;

    ctx->planner_lookup = zhashx_new ();
    for (int i = 0; i < len; ++i) {
        ctx->job_types[i] = xstrdup (job_types[i]);
        zhashx_insert (ctx->planner_lookup, ctx->job_types[i],
                       planner_new (base_time, duration,
                                    total_resources, resource_type));
        zhashx_freefn (ctx->planner_lookup, ctx->job_types[i], planner_free_wrap);
    }

done:
    return ctx;
}

int64_t planner_adapt_base_time (planner_adapt_t *ctx)
{
    if (!ctx) {
        errno = EINVAL;
        return -1;
    }

    planner_t *planner = NULL;
    if (!(planner = zhashx_lookup (ctx->planner_lookup, "rigid"))) {
        errno = EINVAL;
        return -1;
    }

    return planner_base_time (planner);
}

int64_t planner_adapt_duration (planner_adapt_t *ctx)
{
    if (!ctx) {
        errno = EINVAL;
        return -1;
    }

   planner_t *planner = NULL;
    if (!(planner = zhashx_lookup (ctx->planner_lookup, "rigid"))) {
        errno = EINVAL;
        return -1;
    }
    
    return planner_duration (planner);
}

size_t planner_adapt_resources_len (planner_adapt_t *ctx)
{
    if (!ctx) {
        errno = EINVAL;
        return 0;
    }
    return ctx->size;
}

const char *planner_adapt_resource_type (planner_adapt_t *ctx)
{
    if (!ctx) {
        errno = EINVAL;
        return NULL;
    }
    return (const char *)ctx->resource_type;
}

const char **planner_adapt_job_types (planner_adapt_t *ctx)
{
    if (!ctx) {
        errno = EINVAL;
        return NULL;
    }
    return (const char **)ctx->job_types;
}

const uint64_t planner_adapt_total_resources (planner_adapt_t *ctx)
{
    if (!ctx) {
        errno = EINVAL;
        return -1;
    }
    return (const uint64_t)ctx->total_resources;
}

void planner_adapt_destroy (planner_adapt_t **ctx_p)
{
    int i = 0;
    if (ctx_p && *ctx_p) {
        for (i = 0; i < (*ctx_p)->size; ++i) {
            free ((*ctx_p)->job_types[i]);
        }
        if ((*ctx_p)->resource_type)
            free ((*ctx_p)->resource_type);
        free ((*ctx_p)->job_types);
        free ((*ctx_p)->iter.counts);
        zhashx_destroy (&((*ctx_p)->planner_lookup));
        free (*ctx_p);
        *ctx_p = NULL;
    }
}

int planner_adapt_avail_resources_during (planner_adapt_t *ctx, int64_t at,
                                                uint64_t duration,
                                                const char *jobtype)
{
    int avail = 0, rigid_avail = 0, elastic_avail = 0;
    int rtotal = 0;
    if (!ctx || !jobtype || ctx->size < 1) {
        errno = EINVAL;
        return -1;
    }

    planner_t *rigid_planner = NULL;
    if (!(rigid_planner = zhashx_lookup (ctx->planner_lookup, "rigid"))) {
        errno = EINVAL;
        return -1;
    }

    planner_t *elastic_planner = NULL;
    if (!(elastic_planner = zhashx_lookup (ctx->planner_lookup, "elastic"))) {
        errno = EINVAL;
        return -1;
    }
    // number of rigid and elastic resources must be equal
    rtotal = planner_resource_total (rigid_planner);

    if (rtotal == 0)
        return 0;
    else if (rtotal == -1)
        return -1;

    // need to generalize this beyond array
    // first element: rigid job type
    rigid_avail = planner_avail_resources_during (rigid_planner, at,
                                           duration);
    if (rigid_avail == 0)
        return 0;

    // second element: elastic job type
    elastic_avail = planner_avail_resources_during (elastic_planner, at,
                                           duration);
    if (rigid_avail == -1 && elastic_avail == -1)
        return -1;

    rigid_avail = (rigid_avail == -1)? rtotal : rigid_avail;
    if (strcmp (jobtype, "rigid") == 0) {
        avail = rigid_avail;
    }
    else if (strcmp (jobtype, "elastic") == 0) {
        elastic_avail = (elastic_avail == -1)? 0 : elastic_avail;
        avail = rigid_avail + elastic_avail - rtotal;
    }
    else
        return -1;
    
    return avail;
}

int64_t planner_adapt_add_span (planner_adapt_t *ctx, int64_t start_time,
                                uint64_t duration,
                                const uint64_t resource_request,
                                const char *jobtype)
{
    if (!ctx || !resource_request || !jobtype)
        return -1;

    planner_t *planner = NULL;
    if (!(planner = zhashx_lookup (ctx->planner_lookup, jobtype))) {
        errno = EINVAL;
        return -1;
    }

    return planner_add_span (planner, start_time, duration,
                             resource_request);

}

int planner_adapt_rem_span (planner_adapt_t *ctx, int64_t span_id,
                               const char *jobtype)
{
    if (!ctx || !jobtype || span_id < 0) {
        errno = EINVAL;
        return -1;
    }

    planner_t *planner = NULL;
    if (!(planner = zhashx_lookup (ctx->planner_lookup, jobtype))) {
        errno = EINVAL;
        return -1;
    }

    return planner_rem_span (planner, span_id);
}

int planner_adapt_running_at (planner_t *ctx, int64_t span_id,
                                 int64_t at, const char *jobtype)
{
    if (!ctx) {
        errno = EINVAL;
        goto done;
    }
    planner_t *planner = NULL;
    if (!(planner = zhashx_lookup (ctx->planner_lookup, jobtype))) {
        errno = EINVAL;
        return -1;
    }

    return planner_span_running_at (planner, span_id, at);
}

/*
 * vi: ts=4 sw=4 expandtab
 */
