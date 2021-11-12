/*****************************************************************************\
 * Copyright 2019 Lawrence Livermore National Security, LLC
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
#include <flux/core.h>
#include "resource/hlapi/bindings/c/reapi_cli.h"
}

#include <cstdlib>
#include <cstdint>
#include <cerrno>
#include "resource/hlapi/bindings/c++/reapi_cli.hpp"
#include "resource/hlapi/bindings/c++/reapi_cli_impl.hpp"

using namespace Flux;
using namespace Flux::resource_model;
using namespace Flux::resource_model::detail;

struct reapi_cli_ctx {
    flux_t *h;
    resource_context_t *resource_ctx;
    std::string err_msg;
};

extern "C" reapi_cli_ctx_t *reapi_cli_new ()
{
    reapi_cli_ctx_t *ctx = nullptr;

    try {
        ctx = new reapi_cli_ctx_t;
    }
    catch (const std::bad_alloc &e) {
        ctx->err_msg = __FUNCTION__;
        ctx->err_msg += "Error allocating memory: " + std::string (e.what ())
                         + "\n";
        errno = ENOMEM;
        goto out;
    }

    ctx->h = nullptr;
    ctx->resource_ctx = nullptr;
    ctx->err_msg = "";

out:
    return ctx;
}

extern "C" void reapi_cli_destroy (reapi_cli_ctx_t *ctx)
{
    int saved_errno = errno;
    delete ctx;
    errno = saved_errno;
}

extern "C" int reapi_cli_initialize (reapi_cli_ctx_t *ctx, const char *rgraph,
                                     const char *options)
{
    int rc = -1;

    if ( !(ctx->resource_ctx = reapi_cli_t::initialize (rgraph, options))) {
        errno = EINVAL;
        goto out;
    }
    rc = 0;

out:
    return rc;    
}

extern "C" int reapi_cli_match_allocate (reapi_cli_ctx_t *ctx,
                   bool orelse_reserve, const char *jobspec,
                   uint64_t *jobid, bool *reserved,
                   char **R, int64_t *at, double *ov)
{
    int rc = -1;
    std::string R_buf = "";
    char *R_buf_c = nullptr;
    job_lifecycle_t st;

    if (!ctx || !ctx->resource_ctx) {
        errno = EINVAL;
        goto out;
    }

    *jobid = ctx->resource_ctx->jobid_counter;
    if ((rc = reapi_cli_t::match_allocate (&ctx->resource_ctx, orelse_reserve,
                                           jobspec, *jobid, *reserved,
                                           R_buf, *at, *ov)) < 0) {
        goto out;
    }
    if ( !(R_buf_c = strdup (R_buf.c_str ()))) {
        ctx->err_msg = __FUNCTION__;
        ctx->err_msg += "Error duplicating string\n";
        rc = -1;
        goto out;
    }
    (*R) = R_buf_c;
    *reserved = (at != 0)? true : false;
    st = (*reserved)? 
                job_lifecycle_t::RESERVED : job_lifecycle_t::ALLOCATED;
    if (reserved)
        ctx->resource_ctx->reservations[*jobid] = *jobid;
    else
        ctx->resource_ctx->allocations[*jobid] = *jobid;
    ctx->resource_ctx->jobs[*jobid] = std::make_shared<job_info_t> (*jobid, st, *at,
                                                      "", "", *ov);
    ctx->resource_ctx->jobid_counter++;

out:
    return rc;
}

extern "C" int reapi_cli_update_allocate (reapi_cli_ctx_t *ctx,
                   const uint64_t jobid, const char *R, int64_t *at,
                   double *ov, const char **R_out)
{
    int rc = -1;
    std::string R_buf = "";
    const char *R_buf_c = NULL;
    if (!ctx || !ctx->h || !R) {
        errno = EINVAL;
        goto out;
    }
    if ( (rc = reapi_cli_t::update_allocate (ctx->h,
                                             jobid, R, *at, *ov, R_buf)) < 0) {
        goto out;
    }
    if ( !(R_buf_c = strdup (R_buf.c_str ()))) {
        rc = -1;
        goto out;
    }
    *R_out = R_buf_c;
out:
    return rc;
}

extern "C" int reapi_cli_cancel (reapi_cli_ctx_t *ctx,
                                 const uint64_t jobid, bool noent_ok)
{
    if (!ctx || !ctx->h) {
        errno = EINVAL;
        return -1;
    }
    return reapi_cli_t::cancel (ctx->h, jobid, noent_ok);
}

extern "C" int reapi_cli_info (reapi_cli_ctx_t *ctx, const uint64_t jobid,
                               bool *reserved, int64_t *at, double *ov)
{
    if (!ctx || !ctx->h) {
        errno = EINVAL;
        return -1;
    }
    return reapi_cli_t::info (ctx->h, jobid, *reserved, *at, *ov);
}

extern "C" int reapi_cli_stat (reapi_cli_ctx_t *ctx, int64_t *V,
                               int64_t *E, int64_t *J, double *load,
                               double *min, double *max, double *avg)
{
    if (!ctx || !ctx->h) {
        errno = EINVAL;
        return -1;
    }
    return reapi_cli_t::stat (ctx->h, *V, *E, *J, *load, *min, *max, *avg);
}

extern "C" int reapi_cli_set_handle (reapi_cli_ctx_t *ctx, void *handle)
{
    if (!ctx) {
        errno = EINVAL;
        return -1;
    }
    ctx->h = (flux_t *)handle;
    return 0;
}

extern "C" void *reapi_cli_get_handle (reapi_cli_ctx_t *ctx)
{
    if (!ctx) {
        errno = EINVAL;
        return NULL;
    }
    return ctx->h;
}

extern "C" char *reapi_cli_get_err_msg (reapi_cli_ctx_t *ctx)
{
    std::string err_buf = "";

    err_buf = reapi_cli_t::get_err_message () + ctx->err_msg;

    return strdup (err_buf.c_str ());
}

extern "C" void reapi_cli_clear_err_msg (reapi_cli_ctx_t *ctx)
{
    reapi_cli_t::clear_err_message ();
    ctx->err_msg = "";
}

/*
 * vi:tabstop=4 shiftwidth=4 expandtab
 */
