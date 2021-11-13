/*****************************************************************************\
 * Copyright 2019 Lawrence Livermore National Security, LLC
 * (c.f. AUTHORS, NOTICE.LLNS, LICENSE)
 *
 * This file is part of the Flux resource manager framework.
 * For details, see https://github.com/flux-framework.
 *
 * SPDX-License-Identifier: LGPL-3.0
\*****************************************************************************/

#ifndef REAPI_CLI_HPP
#define REAPI_CLI_HPP

extern "C" {
#if HAVE_CONFIG_H
#include "config.h"
#endif
#include <flux/core.h>
#include <cstdint>
#include <cerrno>
}

#include <fstream>
#include <memory>
#include "resource/hlapi/bindings/c++/reapi.hpp"
#include "resource/jobinfo/jobinfo.hpp"
#include "resource/policies/dfu_match_policy_factory.hpp"
#include "resource/traversers/dfu.hpp"

namespace Flux {
namespace resource_model {
namespace detail {

enum class emit_format_t { GRAPHVIZ_DOT, GRAPH_ML, };

struct match_perf_t {
    double min;                 /* Min match time */
    double max;                 /* Max match time */
    double accum;               /* Total match time accumulated */
};

struct resource_params_t {
    std::string load_file;      /* load file name */
    std::string load_format;    /* load reader format */
    std::string load_allowlist; /* load resource allowlist */
    std::string matcher_name;   /* Matcher name */
    std::string matcher_policy; /* Matcher policy name */
    std::string o_fname;        /* Output file to dump the filtered graph */
    std::ofstream r_out;        /* Output file stream for emitted R */
    std::string r_fname;        /* Output file to dump the emitted R */
    std::string o_fext;         /* File extension */
    std::string prune_filters;  /* Raw prune-filter specification */
    std::string match_format;   /* Format to emit a matched resources */
    emit_format_t o_format;
    bool elapse_time;           /* Print elapse time */
    bool disable_prompt;        /* Disable resource-query> prompt */
    bool flux_hwloc;            /* get hwloc info from flux instance */
    size_t reserve_vtx_vec;     /* Allow for reserving vertex vector size */
};

struct resource_context_t {
    resource_params_t params;        /* Parameters for resource graph context */
    uint64_t jobid_counter;      /* Hold the current jobid value */
    std::shared_ptr<dfu_match_cb_t> matcher; /* Match callback object */
    std::shared_ptr<dfu_traverser_t> traverser; /* Graph traverser object */
    std::shared_ptr<resource_graph_db_t> db;    /* Resource graph data store */
    std::shared_ptr<f_resource_graph_t> fgraph; /* Filtered graph */
    std::shared_ptr<match_writers_t> writers;  /* Vertex/Edge writers */
    match_perf_t perf;           /* Match performance stats */
    std::map<uint64_t, std::shared_ptr<job_info_t>> jobs; /* Jobs table */
    std::map<uint64_t, uint64_t> allocations;  /* Allocation table */
    std::map<uint64_t, uint64_t> reservations; /* Reservation table */
};

class reapi_cli_t : public reapi_t {
private:
    static std::string m_err_msg;

public:
    static resource_context_t * initialize (const std::string &rgraph,
                                            const std::string &options);
    static int match_allocate (void *h, bool orelse_reserve,
                               const std::string &jobspec,
                               const uint64_t jobid, bool &reserved,
                               std::string &R, int64_t &at, double &ov);
    static int match_allocate_multi (void *h, bool orelse_reserve,
                                     const char *jobs,
                                     queue_adapter_base_t *adapter);
    static int update_allocate (void *h, const uint64_t jobid,
                                const std::string &R, int64_t &at, double &ov,
                                std::string &R_out);
    static int cancel (void *h, const uint64_t jobid, bool noent_ok);
    static int info (void *h, const int64_t jobid,
                     bool &reserved, int64_t &at, double &ov);
    static int stat (void *h, int64_t &V, int64_t &E,int64_t &J,
                     double &load, double &min, double &max, double &avg);
    static const std::string &get_err_message ();
    static void clear_err_message ();
};


} // namespace Flux::resource_model::detail
} // namespace Flux::resource_model
} // namespace Flux

#endif // REAPI_MODULE_HPP

/*
 * vi:tabstop=4 shiftwidth=4 expandtab
 */
