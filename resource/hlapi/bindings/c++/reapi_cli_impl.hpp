/*****************************************************************************\
 * Copyright 2019 Lawrence Livermore National Security, LLC
 * (c.f. AUTHORS, NOTICE.LLNS, LICENSE)
 *
 * This file is part of the Flux resource manager framework.
 * For details, see https://github.com/flux-framework.
 *
 * SPDX-License-Identifier: LGPL-3.0
\*****************************************************************************/

#ifndef REAPI_CLI_IMPL_HPP
#define REAPI_CLI_IMPL_HPP

extern "C" {
#if HAVE_CONFIG_H
#include "config.h"
#endif
#include <flux/core.h>
}

#include <jansson.h>
#include <boost/algorithm/string.hpp>
#include "resource/hlapi/bindings/c++/reapi_cli.hpp"
#include "resource/readers/resource_reader_factory.hpp"

namespace Flux {
namespace resource_model {
namespace detail {

static std::string m_err_msg = "";

const int NOT_YET_IMPLEMENTED = -1;

static std::shared_ptr<f_resource_graph_t> create_filtered_graph (
            resource_context_t * &resource_ctx)
{
    std::shared_ptr<f_resource_graph_t> fg = nullptr;

    resource_graph_t &g = resource_ctx->db->resource_graph;
    vtx_infra_map_t vmap = get (&resource_pool_t::idata, g);
    edg_infra_map_t emap = get (&resource_relation_t::idata, g);
    const multi_subsystemsS &filter = resource_ctx->matcher->subsystemsS ();
    subsystem_selector_t<vtx_t, f_vtx_infra_map_t> vtxsel (vmap, filter);
    subsystem_selector_t<edg_t, f_edg_infra_map_t> edgsel (emap, filter);

    try {
        fg = std::make_shared<f_resource_graph_t> (g, edgsel, vtxsel);
    } 
    catch (std::bad_alloc &e) {
        errno = ENOMEM;
        m_err_msg += __FUNCTION__;
        m_err_msg += "Error allocating memory: " + std::string (e.what ())
                      + "\n";
        fg = nullptr;
    }

    return fg;
}

static int subsystem_exist (resource_context_t * &resource_ctx,
                            std::string n)
{
    int rc = 0;
    if (resource_ctx->db->metadata.roots.find (n) 
            == resource_ctx->db->metadata.roots.end ())
        rc = -1;
    return rc;
}

static int set_subsystems_use (resource_context_t * &resource_ctx,
                               std::string n)
{
    int rc = 0;
    resource_ctx->matcher->set_matcher_name (n);
    dfu_match_cb_t &matcher = *(resource_ctx->matcher);
    const std::string &matcher_type = matcher.matcher_name ();

    if (boost::iequals (matcher_type, std::string ("CA"))) {
        if ( (rc = subsystem_exist (resource_ctx, "containment")) == 0)
            matcher.add_subsystem ("containment", "*");
    } else if (boost::iequals (matcher_type, std::string ("IBA"))) {
        if ( (rc = subsystem_exist (resource_ctx, "ibnet")) == 0)
            matcher.add_subsystem ("ibnet", "*");
    } else if (boost::iequals (matcher_type, std::string ("IBBA"))) {
        if ( (rc = subsystem_exist (resource_ctx, "ibnetbw")) == 0)
            matcher.add_subsystem ("ibnetbw", "*");
    } else if (boost::iequals (matcher_type, std::string ("PFS1BA"))) {
        if ( (rc = subsystem_exist (resource_ctx, "pfs1bw")) == 0)
            matcher.add_subsystem ("pfs1bw", "*");
    } else if (boost::iequals (matcher_type, std::string ("PA"))) {
        if ( (rc = subsystem_exist (resource_ctx, "power")) == 0)
            matcher.add_subsystem ("power", "*");
    } else if (boost::iequals (matcher_type, std::string ("C+PFS1BA"))) {
        if ( (rc = subsystem_exist (resource_ctx, "containment")) == 0)
            matcher.add_subsystem ("containment", "contains");
        if ( !rc && (rc = subsystem_exist (resource_ctx, "pfs1bw")) == 0)
            matcher.add_subsystem ("pfs1bw", "*");
    } else if (boost::iequals (matcher_type, std::string ("C+IBA"))) {
        if ( (rc = subsystem_exist (resource_ctx, "containment")) == 0)
            matcher.add_subsystem ("containment", "contains");
        if ( !rc && (rc = subsystem_exist (resource_ctx, "ibnet")) == 0)
            matcher.add_subsystem ("ibnet", "connected_up");
    } else if (boost::iequals (matcher_type, std::string ("C+PA"))) {
        if ( (rc = subsystem_exist (resource_ctx, "containment")) == 0)
            matcher.add_subsystem ("containment", "*");
        if ( !rc && (rc = subsystem_exist (resource_ctx, "power")) == 0)
            matcher.add_subsystem ("power", "draws_from");
    } else if (boost::iequals (matcher_type, std::string ("IB+IBBA"))) {
        if ( (rc = subsystem_exist (resource_ctx, "ibnet")) == 0)
            matcher.add_subsystem ("ibnet", "connected_down");
        if ( !rc && (rc = subsystem_exist (resource_ctx, "ibnetbw")) == 0)
            matcher.add_subsystem ("ibnetbw", "*");
    } else if (boost::iequals (matcher_type, std::string ("C+P+IBA"))) {
        if ( (rc = subsystem_exist (resource_ctx, "containment")) == 0)
            matcher.add_subsystem ("containment", "contains");
        if ( (rc = subsystem_exist (resource_ctx, "power")) == 0)
            matcher.add_subsystem ("power", "draws_from");
        if ( !rc && (rc = subsystem_exist (resource_ctx, "ibnet")) == 0)
            matcher.add_subsystem ("ibnet", "connected_up");
    } else if (boost::iequals (matcher_type, std::string ("V+PFS1BA"))) {
        if ( (rc = subsystem_exist (resource_ctx, "virtual1")) == 0)
            matcher.add_subsystem ("virtual1", "*");
        if ( !rc && (rc = subsystem_exist (resource_ctx, "pfs1bw")) == 0)
            matcher.add_subsystem ("pfs1bw", "*");
    } else if (boost::iequals (matcher_type, std::string ("VA"))) {
        if ( (rc = subsystem_exist (resource_ctx, "virtual1")) == 0)
            matcher.add_subsystem ("virtual1", "*");
    } else if (boost::iequals (matcher_type, std::string ("ALL"))) {
        if ( (rc = subsystem_exist (resource_ctx, "containment")) == 0)
            matcher.add_subsystem ("containment", "*");
        if ( !rc && (rc = subsystem_exist (resource_ctx, "ibnet")) == 0)
            matcher.add_subsystem ("ibnet", "*");
        if ( !rc && (rc = subsystem_exist (resource_ctx, "ibnetbw")) == 0)
            matcher.add_subsystem ("ibnetbw", "*");
        if ( !rc && (rc = subsystem_exist (resource_ctx, "pfs1bw")) == 0)
            matcher.add_subsystem ("pfs1bw", "*");
        if ( (rc = subsystem_exist (resource_ctx, "power")) == 0)
            matcher.add_subsystem ("power", "*");
    } else {
        rc = -1;
    }
    return rc;
}

static int set_resource_ctx_params (resource_context_t * &resource_ctx,
                                    const std::string &options)
{
    int rc = -1;
    json_t *tmp_json = NULL, *opt_json = NULL;
    json_error_t json_err;

    // Set default values
    resource_ctx->perf.min = DBL_MAX;
    resource_ctx->perf.max = 0.0f;
    resource_ctx->perf.accum = 0.0f;
    resource_ctx->params.load_file = "conf/default";
    resource_ctx->params.load_format = "jgf";
    resource_ctx->params.load_allowlist = "";
    resource_ctx->params.matcher_name = "CA";
    resource_ctx->params.matcher_policy = "first";
    resource_ctx->params.o_fname = "";
    resource_ctx->params.r_fname = "";
    resource_ctx->params.o_fext = "dot";
    resource_ctx->params.match_format = "jgf";
    resource_ctx->params.o_format = emit_format_t::GRAPHVIZ_DOT;
    resource_ctx->params.prune_filters = "ALL:core";
    resource_ctx->params.reserve_vtx_vec = 0;
    resource_ctx->params.elapse_time = false;
    resource_ctx->params.disable_prompt = false;

    if ( !(opt_json = json_loads (options.c_str (), 0, &json_err))) {
        errno = ENOMEM;
        m_err_msg += __FUNCTION__;
        m_err_msg += "Error loading options\n";
        goto out;
    }

    // Override defaults if present in options argument
    if ( (tmp_json = json_object_get (opt_json, "load_format"))) {
        resource_ctx->params.load_format = json_string_value (tmp_json);
        if (!resource_ctx->params.load_format.c_str ()) { 
            errno = EINVAL;
            m_err_msg += __FUNCTION__;
            m_err_msg += "Error loading load_format\n";
            json_decref (tmp_json);
            json_decref (opt_json);
            goto out;
        }
    }
    if ( (tmp_json = json_object_get (opt_json, "load_allowlist"))) {
        resource_ctx->params.load_allowlist = json_string_value (tmp_json);
        if (!resource_ctx->params.load_allowlist.c_str ()) { 
            errno = EINVAL;
            m_err_msg += __FUNCTION__;
            m_err_msg += "Error loading load_allowlist\n";
            json_decref (tmp_json);
            json_decref (opt_json);
            goto out;
        }
    }
    if ( (tmp_json = json_object_get (opt_json, "matcher_name"))) {
        resource_ctx->params.matcher_name = json_string_value (tmp_json);
        if (!resource_ctx->params.matcher_name.c_str ()) { 
            errno = EINVAL;
            m_err_msg += __FUNCTION__;
            m_err_msg += "Error loading matcher_name\n";
            json_decref (tmp_json);
            json_decref (opt_json);
            goto out;
        }
    }
    if ( (tmp_json = json_object_get (opt_json, "matcher_policy"))) {
        resource_ctx->params.matcher_policy = json_string_value (tmp_json);
        if (!resource_ctx->params.matcher_policy.c_str ()) { 
            errno = EINVAL;
            m_err_msg += __FUNCTION__;
            m_err_msg += "Error loading matcher_policy\n";
            json_decref (tmp_json);
            json_decref (opt_json);
            goto out;
        }
    }
    if ( (tmp_json = json_object_get (opt_json, "match_format"))) {
        resource_ctx->params.match_format = json_string_value (tmp_json);
        if (!resource_ctx->params.match_format.c_str ()) { 
            errno = EINVAL;
            m_err_msg += __FUNCTION__;
            m_err_msg += "Error loading match_format\n";
            json_decref (tmp_json);
            json_decref (opt_json);
            goto out;
        }
    }
    if ( (tmp_json = json_object_get (opt_json, "prune_filters"))) {
        resource_ctx->params.prune_filters = json_string_value (tmp_json);
        if (!resource_ctx->params.prune_filters.c_str ()) { 
            errno = EINVAL;
            m_err_msg += __FUNCTION__;
            m_err_msg += "Error loading prune_filters\n";
            json_decref (tmp_json);
            json_decref (opt_json);
            goto out;
        }
    }
    if ( (tmp_json = json_object_get (opt_json, "reserve_vtx_vec")))
        // No need for check here; returns 0 on failure
        resource_ctx->params.reserve_vtx_vec = json_integer_value (tmp_json);

    rc = 0;

out:
    return rc;
}

resource_context_t * reapi_cli_t::initialize (const std::string &rgraph,
                                              const std::string &options)
{
    resource_context_t *resource_ctx = nullptr;
    std::stringstream buffer{};
    std::shared_ptr<resource_reader_base_t> rd;
    match_format_t format;

    try {
        resource_ctx = new resource_context_t;
    } 
    catch (std::bad_alloc &e) {
        errno = ENOMEM;
        goto out;
    }
    try {
        resource_ctx->db = std::make_shared<resource_graph_db_t> ();
    } 
    catch (std::bad_alloc &e) {
        errno = ENOMEM;
        goto out;
    }

    if (set_resource_ctx_params (resource_ctx, options) < 0) {
        m_err_msg += __FUNCTION__;
        m_err_msg += "ERROR: can't set resource graph parameters\n";
        goto out;
    }

    if ( !(resource_ctx->matcher = create_match_cb (
                                      resource_ctx->params.matcher_policy))) {
        resource_ctx = nullptr;
        goto out;
    }

    if (resource_ctx->params.reserve_vtx_vec != 0)
        resource_ctx->db->resource_graph.m_vertices.reserve (
            resource_ctx->params.reserve_vtx_vec);
    if ( (rd = create_resource_reader (
                               resource_ctx->params.load_format)) == nullptr) {
        goto out;
    }
    if (resource_ctx->params.load_allowlist != "") {
        if (rd->set_allowlist (resource_ctx->params.load_allowlist) < 0) {
            m_err_msg += __FUNCTION__;
            m_err_msg += "ERROR: can't set allowlist\n";
        }
        if (!rd->is_allowlist_supported ())
            std::cout << "WARN: allowlist unsupported" << "\n";
    }

    if (resource_ctx->db->load (rgraph, rd) != 0) {
        m_err_msg += __FUNCTION__;
        m_err_msg += "ERROR: " + rd->err_message () + "\n";
        m_err_msg += "ERROR: error generating resources\n";
        resource_ctx = nullptr;
        goto out;
    }

    if (set_subsystems_use (resource_ctx, 
                            resource_ctx->params.matcher_name) != 0) {
        m_err_msg += __FUNCTION__;
        m_err_msg += "ERROR: can't set subsystem\n";
        goto out;
    }

    if ( !(resource_ctx->fgraph = create_filtered_graph (resource_ctx))) {
        resource_ctx = nullptr;
        goto out;
    }

    resource_ctx->jobid_counter = 1;
    if (resource_ctx->params.prune_filters != ""
        && resource_ctx->matcher->set_pruning_types_w_spec (
                                        resource_ctx->matcher->dom_subsystem (),
                                        resource_ctx->params.prune_filters)
                                        < 0) {
        resource_ctx = nullptr;
        goto out;
    }

    try {
        resource_ctx->traverser = std::make_shared<dfu_traverser_t> ();
    } 
    catch (std::bad_alloc &e) {
        resource_ctx = nullptr;
        m_err_msg += __FUNCTION__;
        m_err_msg += "Error allocating memory: " + std::string (e.what ())
                      + "\n";
        goto out;
    }

    if (resource_ctx->traverser->initialize (resource_ctx->fgraph, 
                                             resource_ctx->db,
                                             resource_ctx->matcher) != 0) {
        m_err_msg += __FUNCTION__;
        m_err_msg += "ERROR: can't initialize traverser\n";
        resource_ctx = nullptr;
        goto out;
    }

    format = match_writers_factory_t::
                        get_writers_type (resource_ctx->params.match_format);
    if ( !(resource_ctx->writers = match_writers_factory_t::create (format))) {
        m_err_msg += __FUNCTION__;
        m_err_msg += "ERROR: can't create match writer\n";
        resource_ctx = nullptr;
        goto out;
    }

out:
    return resource_ctx;
}

int reapi_cli_t::match_allocate (void *h, bool orelse_reserve,
                                 const std::string &jobspec,
                                 const uint64_t jobid, bool &reserved,
                                 std::string &R, int64_t &at, double &ov)
{
    return NOT_YET_IMPLEMENTED;
}

int reapi_cli_t::update_allocate (void *h, const uint64_t jobid,
                                  const std::string &R, int64_t &at, double &ov,
                                  std::string &R_out)
{
    return NOT_YET_IMPLEMENTED;
}

int reapi_cli_t::match_allocate_multi (void *h, bool orelse_reserve,
                                       const char *jobs,
                                       queue_adapter_base_t *adapter)
{
    return NOT_YET_IMPLEMENTED;
}

int reapi_cli_t::cancel (void *h, const int64_t jobid, bool noent_ok)
{
    return NOT_YET_IMPLEMENTED;
}

int reapi_cli_t::info (void *h, const int64_t jobid,
                       bool &reserved, int64_t &at, double &ov)
{
    return NOT_YET_IMPLEMENTED;
}

int reapi_cli_t::stat (void *h, int64_t &V, int64_t &E,int64_t &J,
                       double &load, double &min, double &max, double &avg)
{
    return NOT_YET_IMPLEMENTED;
}

const std::string &reapi_cli_t::get_err_message ()
{
    return m_err_msg;
}

void reapi_cli_t::clear_err_message ()
{
    m_err_msg = "";
}


} // namespace Flux::resource_model::detail
} // namespace Flux::resource_model
} // namespace Flux

#endif // REAPI_CLI_IMPL_HPP

/*
 * vi:tabstop=4 shiftwidth=4 expandtab
 */
