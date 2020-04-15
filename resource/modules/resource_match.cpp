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

#include <cstdint>
#include <sstream>
#include <cerrno>
#include <map>
#include <cinttypes>
#include <Python.h>
#include <dlfcn.h>

extern "C" {
#if HAVE_CONFIG_H
#include "config.h"
#endif
#include <flux/core.h>
#include <jansson.h>
#include "src/common/libutil/shortjansson.h"
}

#include "resource/schema/resource_graph.hpp"
#include "resource/readers/resource_reader_factory.hpp"
#include "resource/traversers/dfu.hpp"
#include "resource/jobinfo/jobinfo.hpp"
#include "resource/policies/dfu_match_policy_factory.hpp"

using namespace Flux::resource_model;

/******************************************************************************
 *                                                                            *
 *                Resource Matching Service Module Context                    *
 *                                                                            *
 ******************************************************************************/

struct resource_args_t {
    std::string load_file;          /* load file name */
    std::string load_format;        /* load reader format */
    std::string load_whitelist;     /* load resource whitelist */
    std::string match_subsystems;
    std::string match_policy;
    std::string prune_filters;
    std::string match_format;
    int reserve_vtx_vec;           /* Allow for reserving vertex vector size */
};

struct match_perf_t {
    double load;                   /* Graph load time */
    uint64_t njobs;                /* Total match count */
    double min;                    /* Min match time */
    double max;                    /* Max match time */
    double accum;                  /* Total match time accumulated */
};

struct resource_ctx_t {
    ~resource_ctx_t ();
    flux_t *h;                     /* Flux handle */
    flux_msg_handler_t **handlers; /* Message handlers */
    resource_args_t args;          /* Module load options */
    std::shared_ptr<dfu_match_cb_t> matcher; /* Match callback object */
    std::shared_ptr<dfu_traverser_t> traverser; /* Graph traverser object */
    std::shared_ptr<resource_graph_db_t> db;    /* Resource graph data store */
    std::shared_ptr<f_resource_graph_t> fgraph; /* Filtered graph */
    std::shared_ptr<match_writers_t> writers;   /* Vertex/Edge writers */
    match_perf_t perf;             /* Match performance stats */
    std::map<uint64_t, std::shared_ptr<job_info_t>> jobs; /* Jobs table */
    std::map<uint64_t, uint64_t> allocations;  /* Allocation table */
    std::map<uint64_t, uint64_t> reservations; /* Reservation table */
};

resource_ctx_t::~resource_ctx_t ()
{
    flux_msg_handler_delvec (handlers);
}

/******************************************************************************
 *                                                                            *
 *                          Request Handler Prototypes                        *
 *                                                                            *
 ******************************************************************************/

static void match_request_cb (flux_t *h, flux_msg_handler_t *w,
                              const flux_msg_t *msg, void *arg);

static void cancel_request_cb (flux_t *h, flux_msg_handler_t *w,
                               const flux_msg_t *msg, void *arg);

static void info_request_cb (flux_t *h, flux_msg_handler_t *w,
                             const flux_msg_t *msg, void *arg);

static void stat_request_cb (flux_t *h, flux_msg_handler_t *w,
                             const flux_msg_t *msg, void *arg);

static void next_jobid_request_cb (flux_t *h, flux_msg_handler_t *w,
                                   const flux_msg_t *msg, void *arg);

static void set_property_request_cb (flux_t *h, flux_msg_handler_t *w,
                                   const flux_msg_t *msg, void *arg);

static void get_property_request_cb (flux_t *h, flux_msg_handler_t *w,
                                   const flux_msg_t *msg, void *arg);

static void grow_request_cb (flux_t *h, flux_msg_handler_t *w,
                                   const flux_msg_t *msg, void *arg);

static void shrink_request_cb (flux_t *h, flux_msg_handler_t *w,
                               const flux_msg_t *msg, void *arg);

static void detach_request_cb (flux_t *h, flux_msg_handler_t *w,
                               const flux_msg_t *msg, void *arg);

static void dump_graph_request_cb (flux_t *h, flux_msg_handler_t *w,
                               const flux_msg_t *msg, void *arg);

static const struct flux_msg_handler_spec htab[] = {
    { FLUX_MSGTYPE_REQUEST, "resource.match", match_request_cb, 0},
    { FLUX_MSGTYPE_REQUEST, "resource.cancel", cancel_request_cb, 0},
    { FLUX_MSGTYPE_REQUEST, "resource.info", info_request_cb, 0},
    { FLUX_MSGTYPE_REQUEST, "resource.stat", stat_request_cb, 0},
    { FLUX_MSGTYPE_REQUEST, "resource.next_jobid", next_jobid_request_cb, 0},
    { FLUX_MSGTYPE_REQUEST, "resource.set_property", set_property_request_cb,
      0},
    { FLUX_MSGTYPE_REQUEST, "resource.get_property", get_property_request_cb,
      0},
    { FLUX_MSGTYPE_REQUEST, "resource.grow", grow_request_cb, 0},
    { FLUX_MSGTYPE_REQUEST, "resource.shrink", shrink_request_cb, 0},
    { FLUX_MSGTYPE_REQUEST, "resource.detach", detach_request_cb, 0},
    { FLUX_MSGTYPE_REQUEST, "resource.dump_graph", dump_graph_request_cb, 0},
    FLUX_MSGHANDLER_TABLE_END
};

static double get_elapse_time (timeval &st, timeval &et)
{
    double ts1 = (double)st.tv_sec + (double)st.tv_usec/1000000.0f;
    double ts2 = (double)et.tv_sec + (double)et.tv_usec/1000000.0f;
    return ts2 - ts1;
}

/******************************************************************************
 *                                                                            *
 *                   Module Initialization Routines                           *
 *                                                                            *
 ******************************************************************************/

static void set_default_args (resource_args_t &args)
{
    args.load_file = "";
    args.load_format = "hwloc";
    args.load_whitelist = "";
    args.match_subsystems = "containment";
    args.match_policy = "high";
    args.prune_filters = "ALL:core";
    args.match_format = "rv1_nosched";
    args.reserve_vtx_vec = 0;
}

static std::shared_ptr<resource_ctx_t> getctx (flux_t *h)
{
    void *d = NULL;
    std::shared_ptr<resource_ctx_t> ctx = nullptr;

    if ( (d = flux_aux_get (h, "resource")) != NULL)
        ctx = *(static_cast<std::shared_ptr<resource_ctx_t> *>(d));
    if (!ctx) {
        try {
            ctx = std::make_shared<resource_ctx_t> ();
            ctx->traverser = std::make_shared<dfu_traverser_t> ();
            ctx->db = std::make_shared<resource_graph_db_t> ();
        } catch (std::bad_alloc &e) {
            errno = ENOMEM;
            goto done;
        }
        ctx->h = h;
        ctx->handlers = NULL;
        set_default_args (ctx->args);
        ctx->perf.load = 0.0f;
        ctx->perf.njobs = 0;
        ctx->perf.min = DBL_MAX;
        ctx->perf.max = 0.0f;
        ctx->perf.accum = 0.0f;
        ctx->matcher = nullptr; /* Cannot be allocated at this point */
        ctx->fgraph = nullptr;  /* Cannot be allocated at this point */
        ctx->writers = nullptr; /* Cannot be allocated at this point */
    }

done:
    return ctx;
}

static int process_args (std::shared_ptr<resource_ctx_t> &ctx,
                         int argc, char **argv)
{
    int rc = 0;
    resource_args_t &args = ctx->args;
    std::string dflt = "";

    for (int i = 0; i < argc; i++) {
        if (!strncmp ("load-file=", argv[i], sizeof ("load-file"))) {
            args.load_file = strstr (argv[i], "=") + 1;
        } else if (!strncmp ("load-format=", argv[i], sizeof ("load-format"))) {
            dflt = args.load_format;
            args.load_format = strstr (argv[i], "=") + 1;
            if (!known_resource_reader (args.load_format)) {
                flux_log (ctx->h, LOG_ERR,
                          "%s: unknown resource reader (%s)! use default (%s).",
                          __FUNCTION__,
                           args.load_format.c_str (), dflt.c_str ());
                args.load_format = dflt;
            }
            args.load_format = strstr (argv[i], "=") + 1;
        } else if (!strncmp ("load-whitelist=",
                             argv[i], sizeof ("load-whitelist"))) {
            args.load_whitelist = strstr (argv[i], "=") + 1;
        } else if (!strncmp ("subsystems=", argv[i], sizeof ("subsystems"))) {
            dflt = args.match_subsystems;
            args.match_subsystems = strstr (argv[i], "=") + 1;
        } else if (!strncmp ("policy=", argv[i], sizeof ("policy"))) {
            dflt = args.match_policy;
            args.match_policy = strstr (argv[i], "=") + 1;
            if (!known_match_policy (args.match_policy)) {
                flux_log (ctx->h, LOG_ERR,
                          "%s: unknown match policy (%s)! use default (%s).",
                           __FUNCTION__,
                           args.match_policy.c_str (), dflt.c_str ());
                args.match_policy = dflt;
            }
        } else if (!strncmp ("prune-filters=",
                             argv[i], sizeof ("prune-filters"))) {
            std::string token = strstr (argv[i], "=") + 1;
            if(token.find_first_not_of(' ') != std::string::npos) {
                args.prune_filters += ",";
                args.prune_filters += token;
            }
        } else if (!strncmp ("match-format=",
                             argv[i], sizeof ("match-format"))) {
            dflt = args.match_format;
            args.match_format = strstr (argv[i], "=") + 1;
            if (!known_match_format (args.match_format)) {
                args.match_format = dflt;
                flux_log (ctx->h, LOG_ERR,
                          "%s: unknown match format (%s)! use default (%s).",
                          __FUNCTION__,
                          args.match_format.c_str (), dflt.c_str ());
                args.match_format = dflt;
            }
        } else if (!strncmp ("reserve-vtx-vec=",
                             argv[i], sizeof ("reserve-vtx-vec"))) {
            args.reserve_vtx_vec = atoi (strstr (argv[i], "=") + 1);
            if ( args.reserve_vtx_vec <= 0 || args.reserve_vtx_vec > 2000000) {
                flux_log (ctx->h, LOG_ERR,
                          "%s: out of range specified for reserve-vtx-vec (%d)",
                          __FUNCTION__, args.reserve_vtx_vec);
                args.reserve_vtx_vec = 0;
            }
        } else {
            rc = -1;
            errno = EINVAL;
            flux_log (ctx->h, LOG_ERR, "%s: unknown option `%s'",
                      __FUNCTION__, argv[i]);
        }
    }

    return rc;
}

static std::shared_ptr<resource_ctx_t> init_module (flux_t *h,
                                                    int argc, char **argv)
{
    std::shared_ptr<resource_ctx_t> ctx = nullptr;
    uint32_t rank = 1;

    if (!(ctx = getctx (h))) {
        flux_log (h, LOG_ERR, "%s: can't allocate the context",
                  __FUNCTION__);
        return nullptr;
    }
    if (flux_get_rank (h, &rank) < 0) {
        flux_log (h, LOG_ERR, "%s: can't determine rank",
                  __FUNCTION__);
        goto error;
    }
    if (rank) {
        flux_log (h, LOG_ERR, "%s: resource module must only run on rank 0",
                  __FUNCTION__);
        goto error;
    }
    process_args (ctx, argc, argv);
    if (flux_msg_handler_addvec (h, htab, (void *)h, &ctx->handlers) < 0) {
        flux_log_error (h, "%s: error registering resource event handler",
                        __FUNCTION__);
        goto error;
    }
    return ctx;

error:
    return nullptr;
}


/******************************************************************************
 *                                                                            *
 *              Resource Graph and Traverser Initialization                   *
 *                                                                            *
 ******************************************************************************/

/* Block until value of 'key' becomes non-NULL.
 * It is an EPROTO error if value is type other than json_type_string.
 * On success returns value, otherwise NULL with errno set.
 */
static json_t *get_string_blocking (flux_t *h, const char *key)
{
    flux_future_t *f = NULL;
    const char *json_str;
    json_t *o = NULL;
    int saved_errno;

    if (!(f = flux_kvs_lookup (h, NULL, FLUX_KVS_WAITCREATE, key))) {
        saved_errno = errno;
        goto error;
    }

    if (flux_kvs_lookup_get (f, &json_str) < 0) {
        saved_errno = errno;
        goto error;
    }

    if (!json_str || !(o = Jfromstr (json_str))
                  || !json_is_string (o)) {
        saved_errno = EPROTO;
        goto error;
    }

    flux_future_destroy (f);
    return o;
error:
    flux_future_destroy (f);
    Jput (o);
    errno = saved_errno;
    return NULL;
}

static int populate_resource_db_file (std::shared_ptr<resource_ctx_t> &ctx,
                                      std::shared_ptr<resource_reader_base_t> rd)
{
    int rc = -1;
    std::ifstream in_file;
    std::stringstream buffer{};

    in_file.open (ctx->args.load_file.c_str (), std::ifstream::in);
    if (!in_file.good ()) {
        errno = EIO;
        flux_log (ctx->h, LOG_ERR, "%s: opening %s",
                  __FUNCTION__, ctx->args.load_file.c_str ());
        goto done;
    }
    buffer << in_file.rdbuf ();
    in_file.close ();
    if ( (rc = ctx->db->load (buffer.str (), rd)) < 0) {
        flux_log (ctx->h, LOG_ERR, "%s: reader: %s",
                  __FUNCTION__, rd->err_message ().c_str ());
        goto done;
    }
    rc = 0;

done:
    return rc;
}

static int populate_resource_db_kvs (std::shared_ptr<resource_ctx_t> &ctx,
                                     std::shared_ptr<resource_reader_base_t> rd)
{
    int n = -1;
    int rc = -1;
    char k[64] = {0};
    uint32_t rank = 0;
    uint32_t size = 0;
    json_t *o = NULL;
    flux_t *h = ctx->h;
    const char *hwloc_xml = NULL;
    resource_graph_db_t &db = *(ctx->db);
    vtx_t v = boost::graph_traits<resource_graph_t>::null_vertex ();

    if (flux_get_size (h, &size) == -1) {
        flux_log (h, LOG_ERR, "%s: flux_get_size", __FUNCTION__);
        goto done;
    }

    // For 0th rank -- special case to use rd->unpack
    rank = 0;
    n = snprintf (k, sizeof (k), "resource.hwloc.xml.%" PRIu32 "", rank);
    if ((n < 0) || ((unsigned int) n > sizeof (k))) {
        errno = ENOMEM;
        goto done;
    }
    o = get_string_blocking (h, k);
    hwloc_xml = json_string_value (o);
    if ( (rc = db.load (hwloc_xml, rd, rank)) < 0) {
        flux_log (ctx->h, LOG_ERR, "%s: reader: %s",
                  __FUNCTION__,  rd->err_message ().c_str ());
        goto done;
    }
    Jput (o);
    if (db.metadata.roots.find ("containment") == db.metadata.roots.end ()) {
        flux_log (ctx->h, LOG_ERR, "%s: cluster vertex is unavailable",
                  __FUNCTION__);
        goto done;
    }
    v = db.metadata.roots.at ("containment");

    // For the rest of the ranks -- general case
    for (rank=1; rank < size; rank++) {
        n = snprintf (k, sizeof (k), "resource.hwloc.xml.%" PRIu32 "", rank);
        if ((n < 0) || ((unsigned int) n > sizeof (k))) {
          errno = ENOMEM;
          goto done;
        }
        o = get_string_blocking (h, k);
        hwloc_xml = json_string_value (o);
        if ( (rc = db.load (hwloc_xml, rd, v, rank)) < 0) {
            flux_log (ctx->h, LOG_ERR, "%s: reader: %s",
                      __FUNCTION__,  rd->err_message ().c_str ());
            goto done;
        }
        Jput (o);
    }
    rc = 0;

done:
    return rc;
}

static int populate_resource_db (std::shared_ptr<resource_ctx_t> &ctx)
{
    int rc = -1;
    double elapse;
    struct timeval st, et;
    std::shared_ptr<resource_reader_base_t> rd;

//    if (ctx->args.reserve_vtx_vec != 0)
//        ctx->db->resource_graph.m_vertices.reserve (ctx->args.reserve_vtx_vec);
    if ( (rd = create_resource_reader (ctx->args.load_format)) == nullptr) {
        flux_log (ctx->h, LOG_ERR, "%s: can't create load reader",
                  __FUNCTION__);
        goto done;
    }
    if (ctx->args.load_whitelist != "") {
        if (rd->set_whitelist (ctx->args.load_whitelist) < 0)
            flux_log (ctx->h, LOG_ERR, "%s: setting whitelist", __FUNCTION__);
        if (!rd->is_whitelist_supported ())
            flux_log (ctx->h, LOG_WARNING, "%s: whitelist unsupported",
                      __FUNCTION__);
    }

    gettimeofday (&st, NULL);
    if (ctx->args.load_file != "") {
        if (populate_resource_db_file (ctx, rd) < 0) {
            flux_log (ctx->h, LOG_ERR, "%s: error loading resources from file",
                      __FUNCTION__);
            goto done;
        }
        flux_log (ctx->h, LOG_INFO, "%s: loaded resources from %s",
                  __FUNCTION__,  ctx->args.load_file.c_str ());
    } else {
        if (populate_resource_db_kvs (ctx, rd) < 0) {
            flux_log (ctx->h, LOG_ERR, "%s: loading resources from the KVS",
                      __FUNCTION__);
            goto done;
        }
        flux_log (ctx->h, LOG_INFO,
                  "%s: loaded resources from hwloc in the KVS",
                  __FUNCTION__);
    }
    gettimeofday (&et, NULL);
    ctx->perf.load = get_elapse_time (st, et);
    rc = 0;

done:
    return rc;
}

static int select_subsystems (std::shared_ptr<resource_ctx_t> &ctx)
{
    /*
     * Format of match_subsystems
     * subsystem1[:relation1[:relation2...]],subsystem2[...
     */
    int rc = 0;
    std::stringstream ss (ctx->args.match_subsystems);
    subsystem_t subsystem;
    std::string token;

    while (getline (ss, token, ',')) {
        size_t found = token.find_first_of (":");
        if (found == std::string::npos) {
            subsystem = token;
            if (!ctx->db->known_subsystem (subsystem)) {
                rc = -1;
                errno = EINVAL;
                goto done;
            }
            ctx->matcher->add_subsystem (subsystem, "*");
        } else {
            subsystem = token.substr (0, found);
            if (!ctx->db->known_subsystem (subsystem)) {
                rc = -1;
                errno = EINVAL;
                goto done;
            }
            std::stringstream relations (token.substr (found+1,
                                                       std::string::npos));
            std::string relation;
            while (getline (relations, relation, ':'))
                ctx->matcher->add_subsystem (subsystem, relation);
        }
    }

done:
    return rc;
}

static std::shared_ptr<f_resource_graph_t> create_filtered_graph (
                                               std::shared_ptr<
                                                   resource_ctx_t> &ctx)
{
    std::shared_ptr<f_resource_graph_t> fg = nullptr;
    resource_graph_t &g = ctx->db->resource_graph;

    try {
        // Set vertex and edge maps
        vtx_infra_map_t vmap = get (&resource_pool_t::idata, g);
        edg_infra_map_t emap = get (&resource_relation_t::idata, g);

        // Set vertex and edge filters based on subsystems to use
        const multi_subsystemsS &filter = ctx->matcher->subsystemsS ();
        subsystem_selector_t<vtx_t, f_vtx_infra_map_t> vtxsel (vmap, filter);
        subsystem_selector_t<edg_t, f_edg_infra_map_t> edgsel (emap, filter);
        fg = std::make_shared<f_resource_graph_t> (g, edgsel, vtxsel);
    } catch (std::bad_alloc &e) {
        errno = ENOMEM;
        fg = nullptr;
    }

    return fg;
}

static int init_resource_graph (std::shared_ptr<resource_ctx_t> &ctx)
{
    int rc = 0;

    // Select the appropriate matcher based on CLI policy.
    if ( !(ctx->matcher = create_match_cb (ctx->args.match_policy))) {
        flux_log (ctx->h, LOG_ERR, "%s: can't create match callback",
                  __FUNCTION__);
        return -1;

    }
    if ( (rc = populate_resource_db (ctx)) != 0) {
        flux_log (ctx->h, LOG_ERR,
                  "%s: can't populate graph resource database",
                  __FUNCTION__);
        return rc;
    }
    if ( (rc = select_subsystems (ctx)) != 0) {
        flux_log (ctx->h, LOG_ERR, "%s: error processing subsystems %s",
                  __FUNCTION__, ctx->args.match_subsystems.c_str ());
        return rc;
    }
    if ( !(ctx->fgraph = create_filtered_graph (ctx)))
        return -1;

    // Create a writers object for matched vertices and edges
    match_format_t format = match_writers_factory_t::
                                get_writers_type (ctx->args.match_format);
    if ( !(ctx->writers = match_writers_factory_t::create (format)))
        return -1;

    if (ctx->args.prune_filters != ""
        && ctx->matcher->set_pruning_types_w_spec (ctx->matcher->dom_subsystem (),
                                                   ctx->args.prune_filters) < 0) {
        flux_log (ctx->h, LOG_ERR, "%s: error setting pruning types with: %s",
                  __FUNCTION__, ctx->args.prune_filters.c_str ());
        return -1;
    }

    // Initialize the DFU traverser
    if (ctx->traverser->initialize (ctx->fgraph, ctx->db, ctx->matcher) < 0) {
        flux_log (ctx->h, LOG_ERR, "%s: traverser initialization",
                  __FUNCTION__);
        return -1;

    }
    return 0;
}


/******************************************************************************
 *                                                                            *
 *                        Request Handler Routines                            *
 *                                                                            *
 ******************************************************************************/

static void update_match_perf (std::shared_ptr<resource_ctx_t> &ctx,
                               double elapse)
{
    ctx->perf.njobs++;
    ctx->perf.min = (ctx->perf.min > elapse)? elapse : ctx->perf.min;
    ctx->perf.max = (ctx->perf.max < elapse)? elapse : ctx->perf.max;
    ctx->perf.accum += elapse;
}

static inline std::string get_status_string (int64_t now, int64_t at)
{
    return (at == now)? "ALLOCATED" : "RESERVED";
}

static int track_schedule_info (std::shared_ptr<resource_ctx_t> &ctx,
                                int64_t id, int64_t now, int64_t at,
                                const std::string &jspec,
                                std::stringstream &R, double elapse)
{
    job_lifecycle_t state = job_lifecycle_t::INIT;

    if (id < 0 || now < 0 || at < 0) {
        errno = EINVAL;
        return -1;
    }

    state = (at == now)? job_lifecycle_t::ALLOCATED : job_lifecycle_t::RESERVED;
    try {
        ctx->jobs[id] = std::make_shared<job_info_t> (id, state, at, "",
                                                      jspec, R.str (), elapse);
    } catch (std::bad_alloc &e) {
        errno = ENOMEM;
        return -1;
    }

    if (at == now)
        ctx->allocations[id] = id;
    else
        ctx->reservations[id] = id;

    return 0;
}

static int run (std::shared_ptr<resource_ctx_t> &ctx, int64_t jobid,
                const char *cmd, const std::string &jstr, int64_t *at)
{
    int rc = 0;
    Flux::Jobspec::Jobspec j {jstr};
    dfu_traverser_t &tr = *(ctx->traverser);

    if (std::string ("allocate") == cmd || std::string ("grow") == cmd)
        rc = tr.run (j, ctx->writers, match_op_t::MATCH_ALLOCATE, jobid, at);
    else if (std::string ("allocate_with_satisfiability") == cmd)
        rc = tr.run (j, ctx->writers, match_op_t::
                     MATCH_ALLOCATE_W_SATISFIABILITY, jobid, at);
    else if (std::string ("allocate_orelse_reserve") == cmd)
        rc = tr.run (j, ctx->writers, match_op_t::MATCH_ALLOCATE_ORELSE_RESERVE,
                     jobid, at);
   return rc;
}

static int run_create_ec2 (std::shared_ptr<resource_ctx_t> &ctx,
                const std::string &jstr, std::string &subgraph)
{
    PyObject *module_name, *module, *dict, *python_class, *object;
    PyObject *args, *set_root, *set_jobspec, *request_instances, *ec2_to_jgf; 
    PyObject *jgf;
    vtx_t root_v = boost::graph_traits<resource_graph_t>::null_vertex ();
    std::string root = "";

#if HAVE_PYTHON_MAJOR != 3
    std::cerr << "EC2 API built with Python != 3 not supported" << std::endl;
    return -1;
#endif

#if HAVE_PYTHON_MINOR == 6
    #define PYTHON_SO "/usr/lib/python3.6/config-3.6m-x86_64-linux-gnu/libpython3.6.so"
#elif HAVE_PYTHON_MINOR == 7
    #define PYTHON_SO = "/usr/lib/python3.7/config-3.7m-x86_64-linux-gnu/libpython3.7.so"
#else
    std::cerr << "Unsupported Python version for EC2 API" << std::endl;
    return -1;
#endif

    if (!dlopen (PYTHON_SO, RTLD_LAZY | RTLD_GLOBAL)) {
          std::cerr << "Failed to open libpython .so" << std::endl;
          return -1;
    } 

    // Adapted from https://stackoverflow.com/questions/39813301/
    // creating-a-python-object-in-c-and-calling-its-method
    Py_Initialize ();
    PyRun_SimpleString ("import sys");
    PyRun_SimpleString ("sys.path.insert(0, 't/scripts/')");

    module_name = PyUnicode_FromString ("ec2api");
    module = PyImport_Import (module_name);
    if (module == nullptr) {
        PyErr_Print ();
        std::cerr << "Failed to import ec2api" << std::endl;
        return -1;
    }
    Py_DECREF (module_name);

    dict = PyModule_GetDict (module);
    if (dict == nullptr) {
        PyErr_Print ();
        std::cerr << "Failed to get the dictionary" << std::endl;
        return -1;
    }
    Py_DECREF (module);
    // Builds the name of a callable class
    python_class = PyDict_GetItemString (dict, "Ec2Comm");
    if (python_class == nullptr) {
        PyErr_Print ();
        std::cerr << "Fails to get the Python class" << std::endl;
        return -1;
    }
    Py_DECREF (dict);

    // Creates an instance of the class
    if (PyCallable_Check (python_class)) {
        object = PyObject_CallObject (python_class, NULL);
        Py_DECREF (python_class);
    } else {
        std::cout << "Can't instantiate the Python class" << std::endl;
        Py_DECREF (python_class);
        return -1;
    }

    root_v = ctx->db->metadata.roots.at ("containment");
    root = ctx->db->resource_graph[root_v].name;
    std::cout << "setting root: " << root << std::endl;
    set_root = PyObject_CallMethod (object, "set_root", "(s)", root.c_str ());
    if (!set_root) {
        PyErr_Print ();
        std::cerr << "Fails to set root" << std::endl;
        return -1;
    }
    Py_DECREF (set_root);

    set_jobspec = PyObject_CallMethod (object, "set_jobspec", "(s)",
                                        jstr.c_str ());
    if (!set_jobspec) {
        PyErr_Print ();
        std::cerr << "Fails to set jobspec" << std::endl;
        return -1;
    }
    Py_DECREF (set_jobspec);
    std::cout << "succeeded setting root and jobspec" << std::endl;

    request_instances = PyObject_CallMethod (object, 
                                            "request_instances", NULL);
    if (!request_instances) {
        PyErr_Print ();
        std::cerr << "Fails to request instances" << std::endl;
        return -1;
    }
    Py_DECREF (request_instances);
    std::cout << "succeeded requesting instances" << std::endl;

    ec2_to_jgf = PyObject_CallMethod (object, "ec2_to_jgf", NULL);
    if (!ec2_to_jgf) {
        PyErr_Print ();
        std::cerr << "Fails to convert to JGF" << std::endl;
        return -1;
    }
    Py_DECREF (ec2_to_jgf);

    jgf = PyObject_CallMethod (object, "get_jgf", NULL);
    if (!jgf) {
        PyErr_Print ();
        std::cerr << "Fails to get JGF" << std::endl;
        return -1;
    }

    std::cout << "got jgf" << std::endl;
    subgraph = PyUnicode_AsUTF8 (jgf);
    std::cout << subgraph << std::endl;

    Py_DECREF (jgf);
    Py_DECREF (object);
    Py_Finalize ();

    return 0;
}

static int run_attach (std::shared_ptr<resource_ctx_t> &ctx, const int64_t jobid,
                      const std::string &subgraph, const int64_t at,
                      const uint64_t duration)
{
    int rc = -1;
    dfu_traverser_t &tr = *(ctx->traverser);
    std::shared_ptr<resource_reader_base_t> rd;
    vtx_t root = boost::graph_traits<resource_graph_t>::null_vertex ();

    std::map<subsystem_t, vtx_t>::const_iterator it =
        ctx->db->metadata.roots.find ("containment");

    if ( (rd = create_resource_reader ("jgf")) == nullptr) {
        flux_log (ctx->h, LOG_ERR, "%s: can't create grow reader",
                  __FUNCTION__);
        goto done;
    }

    if (it == ctx->db->metadata.roots.end ()) {
        std::cerr << "ERROR: unsupported subsys for attach " << std::endl;
        goto done;
    }
    root = it->second;
    if ( (rd->unpack_at (ctx->db->resource_graph, ctx->db->metadata, 
                         root, subgraph, -1)) != 0) {
        std::cerr << "ERROR: can't attach JGF subgraph " << std::endl;
        std::cerr << "ERROR: " << rd->err_message ();
        goto done;
    }

    if ( (rc = tr.run (subgraph, ctx->writers, rd, jobid, at, duration)) != 0) {
        std::cerr << "ERROR: traverser run () returned error " << std::endl;
        if (tr.err_message () != "") {
            std::cerr << "ERROR: " << tr.err_message ();
            tr.clear_err_message ();
            goto done;
        }
    }

    rc = 0;
done:
    return rc;
}

static int run_grow (std::shared_ptr<resource_ctx_t> &ctx,
                      const int64_t jobid, 
                      const std::string &subgraph)
{
    int rc = -1;
    const char *result = NULL;
    const char *child_uri = NULL;
    flux_t *child_h = NULL;
    flux_future_t *f = NULL;

    if ( (rc = run_attach (ctx, jobid, subgraph, 0, 3600)) != 0) {
        flux_log_error (ctx->h, "%s: can't grow job", 
                        __FUNCTION__);
        goto done;
    }

    if ((child_uri = flux_attr_get (ctx->h, "child-uri-0"))) {
        std::cout << "child URI: " << child_uri << " \n";
        std::cout << "my URI: " << flux_attr_get (ctx->h, "local-uri") << " \n";
        if (!(child_h = flux_open (child_uri, 0))) {
            flux_log_error (ctx->h, "%s: can't get child handle", __FUNCTION__);
            errno = EPROTO;
            rc = -1;
            goto done;
        }

        if (!(f = flux_rpc_pack (child_h, "resource.grow", FLUX_NODEID_ANY, 0,
                                     "{s:I s:s}", "jobid", jobid, 
                                     "subgraph", subgraph.c_str ()))) {
            flux_close (child_h);
            flux_future_destroy (f);
            errno = EPROTO;
            rc = -1;
            goto done;
        }
        if (flux_rpc_get_unpack (f, "{s:s}", "result", &result) < 0) {
            flux_close (child_h);
            flux_future_destroy (f);
            errno = EPROTO;
            rc = -1;
            goto done;
        }
        std::cout << "child result: " << result << " \n";
        flux_close (child_h);
        flux_future_destroy (f);
    }

    rc = 0;
done:
    return rc;
}

static int run_detach (std::shared_ptr<resource_ctx_t> &ctx,
                      const std::string &path, const int64_t jobid,
                      const std::string &subgraph, bool up)
{
    int rc = -1;
    std::shared_ptr<resource_reader_base_t> rd;
    const char *relative_uri = NULL;
    const char *result = NULL;
    flux_t *relative_h = NULL;
    flux_future_t *f = NULL;
    bool detach = true;

    if ( (rd = create_resource_reader ("jgf")) == nullptr) {
        flux_log_error (ctx->h, "%s ERROR: can't create detach reader",  __FUNCTION__);
        goto done;
    }
    if ( (rc = rd->detach (ctx->db->resource_graph, ctx->db->metadata, 
                           subgraph)) != 0) {
        flux_log_error (ctx->h, "%s ERROR: can't detach JGF subgraph",  __FUNCTION__);
        flux_log_error (ctx->h, "%s ERROR: detach reader: %s",  __FUNCTION__, rd->err_message ().c_str ());
        goto done;
    }

    // Application must decide whether to push shrink up or down the tree, 
    // whether to change the detach bool, and fetch the jobid from
    // Flux attrs.
    if (up) {
        if ((relative_uri = flux_attr_get (ctx->h, "parent-uri"))) {
            std::cout << "parent URI: " << relative_uri << " \n";
        }
        else
            goto done;
    }
    else {
        if ((relative_uri = flux_attr_get (ctx->h, "child-uri-0"))) { // TODO: generalize for jobid != 0
            std::cout << "child URI: " << relative_uri << " \n";
        }
        else
            goto done;
    }
    std::cout << "my URI: " << flux_attr_get (ctx->h, "local-uri") << " \n";
    if (!(relative_h = flux_open (relative_uri, 0))) {
        flux_log_error (ctx->h, "%s: can't get relative handle", __FUNCTION__);
        errno = EPROTO;
        rc = -1;
        goto done;
    }

    if (detach) {
        if (!(f = flux_rpc_pack (relative_h, "resource.detach", FLUX_NODEID_ANY, 0,
                                     "{s:s s:I s:s s:b}", "path", path.c_str (), 
                                     "jobid", jobid, "subgraph", subgraph.c_str (),
                                     "up", up))) {
            flux_close (relative_h);
            flux_future_destroy (f);
            errno = EPROTO;
            rc = -1;
            goto done;
        }
        if (flux_rpc_get_unpack (f, "{s:s}", "result", &result) < 0) {
            flux_close (relative_h);
            flux_future_destroy (f);
            errno = EPROTO;
            rc = -1;
            goto done;
        }
    }
    else { // just shrink
        if (!(f = flux_rpc_pack (relative_h, "resource.shrink", FLUX_NODEID_ANY, 0,
                                     "{s:s s:I s:b s:b}", "path", path.c_str (), 
                                     "jobid", jobid, "detach", false,
                                     "up", up))) {
            flux_close (relative_h);
            flux_future_destroy (f);
            errno = EPROTO;
            rc = -1;
            goto done;
        }
        if (flux_rpc_get_unpack (f, "{s:s}", "result", &result) < 0) {
            flux_close (relative_h);
            flux_future_destroy (f);
            errno = EPROTO;
            rc = -1;
            goto done;
        }
    }
    std::cout << "Parent result: " << result << " \n";
    flux_close (relative_h);
    flux_future_destroy (f);

    rc = 0;
done:
    return rc;
}

static int run_shrink (std::shared_ptr<resource_ctx_t> &ctx,
                      const std::string &path, const int64_t jobid,
                      bool detach, bool up)
{
    int rc = -1;
    dfu_traverser_t &tr = *(ctx->traverser);
    vtx_t shrink_root = boost::graph_traits<resource_graph_t>::null_vertex ();
    std::stringstream o;
    const char *result = NULL;
    const char *relative_uri = NULL;
    flux_t *relative_h = NULL;
    flux_future_t *f = NULL;

    std::map<std::string, vtx_t>::const_iterator it =
        ctx->db->metadata.by_path.find (path);
    if (it == ctx->db->metadata.by_path.end ()) {
        flux_log_error (ctx->h, "%s ERROR: can't find shrink root", 
                        __FUNCTION__);
        goto done;
    }

    shrink_root = it->second;
    if ((rc = tr.shrink (shrink_root, ctx->writers, jobid)) != 0) {
        flux_log_error (ctx->h, "%s ERROR: shrink traverser: %s", 
                        __FUNCTION__, tr.err_message ().c_str ());
        flux_log_error (ctx->h, "%s ERROR: shrink traverser: %s", 
                        __FUNCTION__, strerror (errno));
        tr.clear_err_message ();
        goto done;
    }
    if ((rc = ctx->writers->emit (o)) < 0) {
        flux_log_error (ctx->h, "%s ERROR: shrink writer emit: %s", 
                        __FUNCTION__, strerror (errno));
        goto done;
    }

    if (detach) {
        if ( (rc = run_detach (ctx, path, jobid, o.str (), up)) != 0) {
            flux_log_error (ctx->h, "%s: can't shrink-detach JGF subgraph", 
                            __FUNCTION__);
            goto done;
        }
    } 
    else {
        // Application must decide whether to push shrink up the tree, 
        // whether to change the detach bool, and fetch the jobid from
        // Flux attrs.
        if (up) {
            if ((relative_uri = flux_attr_get (ctx->h, "parent-uri"))) {
                std::cout << "parent URI: " << relative_uri << " \n";
            }
            else
                goto done;
        }
        else {
            if ((relative_uri = flux_attr_get (ctx->h, "child-uri-0"))) { // TODO: generalize for jobid != 0
                std::cout << "child URI: " << relative_uri << " \n";
            }
            else
                goto done;
        }
        std::cout << "my URI: " << flux_attr_get (ctx->h, "local-uri") << " \n";
        if (!(relative_h = flux_open (relative_uri, 0))) {
            flux_log_error (ctx->h, "%s: can't get relative handle", __FUNCTION__);
            errno = EPROTO;
            rc = -1;
            goto done;
        }

        if (!(f = flux_rpc_pack (relative_h, "resource.shrink", FLUX_NODEID_ANY, 0,
                                     "{s:s s:I s:b s:b}", "path", path.c_str (), 
                                     "jobid", jobid, "detach", false,
                                     "up", up))) {
            flux_close (relative_h);
            flux_future_destroy (f);
            errno = EPROTO;
            rc = -1;
            goto done;
        }
        if (flux_rpc_get_unpack (f, "{s:s}", "result", &result) < 0) {
            flux_close (relative_h);
            flux_future_destroy (f);
            errno = EPROTO;
            rc = -1;
            goto done;
        }
        std::cout << "Parent result: " << result << " \n";
        flux_close (relative_h);
        flux_future_destroy (f);
    }

    rc = 0;
done:
    return rc;
}

static int run_match (std::shared_ptr<resource_ctx_t> &ctx, int64_t jobid,
                      const char *cmd, const std::string &jstr, int64_t *now,
                      int64_t *at, double *ov, std::stringstream &o)
{
    int rc = 0;
    double elapse = 0.0f;
    struct timeval start;
    struct timeval end;

    flux_t *parent_h = NULL;
    flux_future_t *f = NULL;
    int64_t tmp_jobid = 0;
    double tmp_ov = 0.0f;
    int64_t at_tmp = 0;
    uint32_t nodeid = FLUX_NODEID_ANY;
    int len = 0;
    const char *parent_uri = NULL;
    const char *rset = NULL;
    const char *status = NULL;
    std::string root = "";
    std::string subgraph = "";
    vtx_t root_v = boost::graph_traits<resource_graph_t>::null_vertex ();

    gettimeofday (&start, NULL);

    if (strcmp ("allocate", cmd) != 0
        && strcmp ("allocate_orelse_reserve", cmd) != 0
        && strcmp ("allocate_with_satisfiability", cmd) != 0
        && strcmp ("grow", cmd) != 0) {
        errno = EINVAL;
        flux_log_error (ctx->h, "%s: unknown cmd: %s", __FUNCTION__, cmd);
        goto done;
    }

    *at = *now = (int64_t)start.tv_sec;
    if ((rc = run (ctx, jobid, cmd, jstr, at)) < 0) {
        if (strcmp ("grow", cmd) != 0)
            goto done;

        std::cout << "my URI: " << flux_attr_get (ctx->h, "local-uri") << " \n";
        if (!(parent_uri = flux_attr_get (ctx->h, "parent-uri"))) {
            // Try EC2
            if (run_create_ec2 (ctx, jstr, subgraph) < 0) {
                errno = ENODEV;
                goto done;
            }
            o << subgraph; // to stringstream
        }
        else {
            if (!(parent_h = flux_open (parent_uri, 0))) {
                flux_log_error (ctx->h, "%s: can't get parent handle", __FUNCTION__);
                errno = ENODEV;
                goto done;
            }

            len = strlen (parent_uri);
            // Check if parent is child of remote instance
            if (strcmp (&parent_uri[len - 7], "0/local") != 0)
                nodeid = 0;  // Send RPC to root.  It's running resource.

            if (!(f = flux_rpc_pack (parent_h, "resource.match", nodeid, 0,
                                         "{s:s s:I s:s}",
                                         "cmd", cmd, "jobid", jobid,
                                         "jobspec", jstr.c_str ()))) {
                    flux_close (parent_h);
                    flux_future_destroy (f);
                    errno = ENODEV;
                    goto done;
            }
            if (flux_rpc_get_unpack (f, "{s:I s:s s:f s:s s:I}",
                                     "jobid", &tmp_jobid, "status", &status,
                                     "overhead", &tmp_ov, "R", &rset, "at", &at_tmp) < 0) {
                flux_close (parent_h);
                flux_future_destroy (f);
                errno = ENODEV;
                goto done;
            }
            o << rset; // back to stringstream
            flux_close (parent_h);
            flux_future_destroy (f);
        }
        if ((rc = run_attach (ctx, jobid, o.str (), *at, 3600)) < 0) {
            flux_log_error (ctx->h, "%s: can't attach JGF", __FUNCTION__);
            goto done;
        }
    } 
    else {
        if ((rc = ctx->writers->emit (o)) < 0) {
            flux_log_error (ctx->h, "%s: writer can't emit", __FUNCTION__);
            goto done;
        }
    }

    gettimeofday (&end, NULL);
    *ov = get_elapse_time (start, end);
    update_match_perf (ctx, *ov);
    if (strcmp ("grow", cmd) != 0) {
        if ((rc = track_schedule_info (ctx, jobid, *now, *at, jstr, o, *ov)) != 0) {
            errno = EINVAL;
            flux_log_error (ctx->h, "%s: can't add job info (id=%jd)",
                            __FUNCTION__, (intmax_t)jobid);
            goto done;
        }
    }

done:
    return rc;
}

static inline bool is_existent_jobid (
                       const std::shared_ptr<resource_ctx_t> &ctx,
                       uint64_t jobid)
{
    return (ctx->jobs.find (jobid) != ctx->jobs.end ())? true : false;
}

static int run_remove (std::shared_ptr<resource_ctx_t> &ctx, int64_t jobid)
{
    int rc = -1;
    dfu_traverser_t &tr = *(ctx->traverser);

    if ((rc = tr.remove (jobid)) < 0) {
        if (is_existent_jobid (ctx, jobid)) {
           // When this condition arises, we will be less likely
           // to be able to reuse this jobid. Having the errored job
           // in the jobs map will prevent us from reusing the jobid
           // up front.  Note that a same jobid can be reserved and
           // removed multiple times by the upper queuing layer
           // as part of providing advanced queueing policies
           // (e.g., conservative backfill).
           std::shared_ptr<job_info_t> info = ctx->jobs[jobid];
           info->state = job_lifecycle_t::ERROR;
        }
        goto out;
    }
    if (is_existent_jobid (ctx, jobid))
        ctx->jobs.erase (jobid);

    rc = 0;
out:
    return rc;
}

static void match_request_cb (flux_t *h, flux_msg_handler_t *w,
                              const flux_msg_t *msg, void *arg)
{
    int64_t at = 0;
    int64_t now = 0;
    int64_t jobid = -1;
    double ov = 0.0f;
    std::string status = "";
    const char *cmd = NULL;
    const char *js_str = NULL;
    std::stringstream R;

    std::shared_ptr<resource_ctx_t> ctx = getctx ((flux_t *)arg);
    if (flux_request_unpack (msg, NULL, "{s:s s:I s:s}", "cmd", &cmd,
                             "jobid", &jobid, "jobspec", &js_str) < 0)
        goto error;
    if (is_existent_jobid (ctx, jobid) && (strcmp (cmd, "grow") != 0)) {
        errno = EINVAL;
        flux_log_error (h, "%s: existent job (%jd).",
                        __FUNCTION__, (intmax_t)jobid);
        goto error;
    }
    if (run_match (ctx, jobid, cmd, js_str, &now, &at, &ov, R) < 0) {
        if (errno != EBUSY && errno != ENODEV)
            flux_log_error (ctx->h,
                            "%s: match failed due to match error (id=%jd)",
                            __FUNCTION__, (intmax_t)jobid);
        goto error;
    }

    status = get_status_string (now, at);

    if (flux_respond_pack (h, msg, "{s:I s:s s:f s:s s:I}",
                                   "jobid", jobid,
                                   "status", status.c_str (),
                                   "overhead", ov,
                                   "R", R.str ().c_str (),
                                   "at", at) < 0)
        flux_log_error (h, "%s", __FUNCTION__);
    return;

error:
    if (flux_respond_error (h, msg, errno, NULL) < 0)
        flux_log_error (h, "%s: flux_respond_error", __FUNCTION__);
}

static void shrink_request_cb (flux_t *h, flux_msg_handler_t *w,
                               const flux_msg_t *msg, void *arg)
{
    int64_t jobid = -1;
    const char *path = NULL;
    const char *detach = NULL;
    const char *up = NULL;
    bool bup = true;
    bool bdetach = false;

    std::shared_ptr<resource_ctx_t> ctx = getctx ((flux_t *)arg);
    if (flux_request_unpack (msg, NULL, "{s:s s:I s:s s:s}", "path", &path,
                             "jobid", &jobid, "detach", &detach,
                             "up", &up) < 0)
        goto error;
    if (!is_existent_jobid (ctx, jobid)) {
        errno = EINVAL;
        flux_log_error (h, "%s: nonexistent job (%jd).",
                        __FUNCTION__, (intmax_t)jobid);
        goto error;
    }

    // TODO: figure out why jobid is always zero, bools 
    // unpacked wrong
    if (strcmp (detach, "true") == 0)
        bdetach = true;
    if (strcmp (up, "false") == 0)
        bup = false;

    if (run_shrink (ctx, path, jobid, detach, up) < 0) {
        goto error;
    }

    if (flux_respond_pack (h, msg, "{s:s}", "result", "Success") < 0)
        flux_log_error (h, "%s", __FUNCTION__);

    return;

error:
    if (flux_respond_error (h, msg, errno, NULL) < 0)
        flux_log_error (h, "%s: flux_respond_error", __FUNCTION__);
}

static void detach_request_cb (flux_t *h, flux_msg_handler_t *w,
                               const flux_msg_t *msg, void *arg)
{
    int64_t jobid = -1;
    const char *path = NULL;
    const char *subgraph = NULL;
    const char *up = NULL;
    bool bup = true;
    const char *success = "Success";

    std::shared_ptr<resource_ctx_t> ctx = getctx ((flux_t *)arg);
    if (flux_request_unpack (msg, NULL, "{s:s s:I s:s s:s}", "path", &path,
                             "jobid", &jobid, "subgraph", &subgraph,
                             "up", &up) < 0)
        goto error;
    if (!is_existent_jobid (ctx, jobid)) {
        errno = EINVAL;
        flux_log_error (h, "%s: nonexistent job (%jd).",
                        __FUNCTION__, (intmax_t)jobid);
        goto error;
    }

    // TODO: figure out why jobid is always zero, bools 
    // unpacked wrong
    if (strcmp (up, "false") == 0)
        bup = false;

    if (run_detach (ctx, path, jobid, subgraph, up) < 0) {
        goto error;
    }

    if (flux_respond_pack (h, msg, "{s:s}", "result", success) < 0)
        flux_log_error (h, "%s", __FUNCTION__);

    return;

error:
    if (flux_respond_error (h, msg, errno, NULL) < 0)
        flux_log_error (h, "%s: flux_respond_error", __FUNCTION__);
}

static void cancel_request_cb (flux_t *h, flux_msg_handler_t *w,
                               const flux_msg_t *msg, void *arg)
{
    std::shared_ptr<resource_ctx_t> ctx = getctx ((flux_t *)arg);
    int64_t jobid = -1;

    if (flux_request_unpack (msg, NULL, "{s:I}", "jobid", &jobid) < 0)
        goto error;
    if (ctx->allocations.find (jobid) != ctx->allocations.end ())
        ctx->allocations.erase (jobid);
    else if (ctx->reservations.find (jobid) != ctx->reservations.end ())
        ctx->reservations.erase (jobid);
    else {
        errno = ENOENT;
        flux_log (h, LOG_DEBUG, "%s: nonexistent job (id=%jd)",
                  __FUNCTION__, (intmax_t)jobid);
        goto error;
    }

    if (run_remove (ctx, jobid) < 0) {
        flux_log_error (h, "%s: remove fails due to match error (id=%jd)",
                        __FUNCTION__, (intmax_t)jobid);
        goto error;
    }
    if (flux_respond_pack (h, msg, "{}") < 0)
        flux_log_error (h, "%s", __FUNCTION__);

    return;

error:
    if (flux_respond_error (h, msg, errno, NULL) < 0)
        flux_log_error (h, "%s: flux_respond_error", __FUNCTION__);
}

static void info_request_cb (flux_t *h, flux_msg_handler_t *w,
                             const flux_msg_t *msg, void *arg)
{
    std::shared_ptr<resource_ctx_t> ctx = getctx ((flux_t *)arg);
    int64_t jobid = -1;
    std::shared_ptr<job_info_t> info = NULL;
    std::string status = "";

    if (flux_request_unpack (msg, NULL, "{s:I}", "jobid", &jobid) < 0)
        goto error;
    if (!is_existent_jobid (ctx, jobid)) {
        errno = ENOENT;
        flux_log (h, LOG_DEBUG, "%s: nonexistent job (id=%jd)",
                  __FUNCTION__,  (intmax_t)jobid);
        goto error;
    }

    info = ctx->jobs[jobid];
    get_jobstate_str (info->state, status);
    if (flux_respond_pack (h, msg, "{s:I s:s s:I s:f}",
                                   "jobid", jobid,
                                   "status", status.c_str (),
                                   "at", info->scheduled_at,
                                   "overhead", info->overhead) < 0)
        flux_log_error (h, "%s", __FUNCTION__);

    return;

error:
    if (flux_respond_error (h, msg, errno, NULL) < 0)
        flux_log_error (h, "%s: flux_respond_error", __FUNCTION__);
}

static void stat_request_cb (flux_t *h, flux_msg_handler_t *w,
                             const flux_msg_t *msg, void *arg)
{
    std::shared_ptr<resource_ctx_t> ctx = getctx ((flux_t *)arg);
    double avg = 0.0f;
    double min = 0.0f;

    if (ctx->perf.njobs) {
        avg = ctx->perf.accum / (double)ctx->perf.njobs;
        min = ctx->perf.min;
    }
    if (flux_respond_pack (h, msg, "{s:I s:I s:f s:I s:f s:f s:f}",
                                   "V", num_vertices (ctx->db->resource_graph),
                                   "E", num_edges (ctx->db->resource_graph),
                                   "load-time", ctx->perf.load,
                                   "njobs", ctx->perf.njobs,
                                   "min-match", min,
                                   "max-match", ctx->perf.max,
                                   "avg-match", avg) < 0)
        flux_log_error (h, "%s", __FUNCTION__);
}

static void dump_graph_request_cb (flux_t *h, flux_msg_handler_t *w,
                             const flux_msg_t *msg, void *arg)
{
    std::shared_ptr<resource_ctx_t> ctx = getctx ((flux_t *)arg);

    std::stringstream o;
    f_vtx_iterator_t vi, v_end;
    f_edg_iterator_t ei, e_end;
    f_resource_graph_t fg = *(ctx->fgraph);
    const char *exe = NULL;

    if (flux_request_unpack (msg, NULL, "{s:s}", "execute", &exe) < 0)
        goto error;

    for (tie (vi, v_end) = vertices (fg); vi != v_end; ++vi) {
        if (ctx->writers->emit_vtx ("", fg, *vi, 1, false) < 0)
            goto error;
    }

    for (tie (ei, e_end) = edges (fg); ei != e_end; ++ei) {
        if (ctx->writers->emit_edg ("", fg, *ei) < 0)
            goto error;
    }

    if (ctx->writers->emit (o) < 0) {
        goto error;
    }

    std::cout << o.str () << std::endl;
    std::cout << "Number of vertices in graph: " 
              << boost::num_vertices (ctx->db->resource_graph)
              << std::endl;
    std::cout << "Number of edges in graph: "
              <<  boost::num_edges (ctx->db->resource_graph)
              <<  std::endl;

    if (flux_respond_pack (h, msg, "{s:s}",
                                   "execute", "exe") < 0)
        flux_log_error (h, "%s", __FUNCTION__);

    return;

error:
    if (flux_respond_error (h, msg, errno, NULL) < 0)
        flux_log_error (h, "%s: flux_respond_error", __FUNCTION__);
}

static inline int64_t next_jobid (const std::map<uint64_t,
                                            std::shared_ptr<job_info_t>> &m)
{
    int64_t jobid = -1;
    if (m.empty ())
        jobid = 0;
    else if (m.rbegin ()->first < INT64_MAX)
        jobid = m.rbegin ()->first + 1;
    return jobid;
}

/* Needed for testing only */
static void next_jobid_request_cb (flux_t *h, flux_msg_handler_t *w,
                                   const flux_msg_t *msg, void *arg)
{
    std::shared_ptr<resource_ctx_t> ctx = getctx ((flux_t *)arg);
    int64_t jobid = -1;

    if ((jobid = next_jobid (ctx->jobs)) < 0) {
        errno = ERANGE;
        goto error;
    }
    if (flux_respond_pack (h, msg, "{s:I}", "jobid", jobid) < 0)
        flux_log_error (h, "%s", __FUNCTION__);

    return;

error:
    if (flux_respond_error (h, msg, errno, NULL) < 0)
        flux_log_error (h, "%s: flux_respond_error", __FUNCTION__);
}

static void set_property_request_cb (flux_t *h, flux_msg_handler_t *w,
                                   const flux_msg_t *msg, void *arg)
{
    const char *rp = NULL, *kv = NULL;
    std::string resource_path = "", keyval = "";
    std::string property_key = "", property_value = "";
    size_t pos;
    std::shared_ptr<resource_ctx_t> ctx = getctx ((flux_t *)arg);
    std::map<std::string, vtx_t>::const_iterator it;
    std::pair<std::map<std::string, std::string>::iterator, bool> ret;
    vtx_t v;

    if (flux_request_unpack (msg, NULL, "{s:s s:s}",
                                        "sp_resource_path", &rp,
                                        "sp_keyval", &kv) < 0)
        goto error;

    resource_path = rp;
    keyval = kv;

    pos = keyval.find ('=');

    if (pos == 0 || (pos == keyval.size () - 1) || pos == std::string::npos) {
        errno = EINVAL;
        flux_log_error (h, "%s: Incorrect format.", __FUNCTION__);
        flux_log_error (h, "%s: Use set-property <resource> PROPERTY=VALUE",
                        __FUNCTION__);
        goto error;
    }

    property_key = keyval.substr (0, pos);
    property_value = keyval.substr (pos + 1);

    it = ctx->db->metadata.by_path.find (resource_path);

    if (it == ctx->db->metadata.by_path.end ()) {
        errno = ENOENT;
        flux_log_error (h, "%s: Couldn't find %s in resource graph.",
                        __FUNCTION__, resource_path.c_str ());
        goto error;
     }

    v = it->second;

    ret = ctx->db->resource_graph[v].properties.insert (
        std::pair<std::string, std::string> (property_key,property_value));

    if (ret.second == false) {
        ctx->db->resource_graph[v].properties.erase (property_key);
        ctx->db->resource_graph[v].properties.insert (
            std::pair<std::string, std::string> (property_key,property_value));
    }

    if (flux_respond_pack (h, msg, "{}") < 0)
        flux_log_error (h, "%s", __FUNCTION__);

    return;

error:
    if (flux_respond_error (h, msg, errno, NULL) < 0)
        flux_log_error (h, "%s: flux_respond_error", __FUNCTION__);
}

static void get_property_request_cb (flux_t *h, flux_msg_handler_t *w,
                                     const flux_msg_t *msg, void *arg)
{
    const char *rp = NULL, *gp_key = NULL;
    std::string resource_path = "", property_key = "";
    std::shared_ptr<resource_ctx_t> ctx = getctx ((flux_t *)arg);
    std::map<std::string, vtx_t>::const_iterator it;
    std::map<std::string, std::string>::const_iterator p_it;
    vtx_t v;
    std::string resp_value = "";

    if (flux_request_unpack (msg, NULL, "{s:s s:s}",
                                        "gp_resource_path", &rp,
                                        "gp_key", &gp_key) < 0)
        goto error;

    resource_path = rp;
    property_key = gp_key;

    it = ctx->db->metadata.by_path.find (resource_path);

    if (it == ctx->db->metadata.by_path.end ()) {
        errno = ENOENT;
        flux_log_error (h, "%s: Couldn't find %s in resource graph.",
                        __FUNCTION__, resource_path.c_str ());
        goto error;
     }

    v = it->second;

    for (p_it = ctx->db->resource_graph[v].properties.begin ();
         p_it != ctx->db->resource_graph[v].properties.end (); p_it++) {

         if (property_key.compare (p_it->first) == 0)
             resp_value = p_it->second;
     }

     if (resp_value.empty ()) {
         errno = ENOENT;
         flux_log_error (h, "%s: Property %s was not found for resource %s.",
                         __FUNCTION__, property_key.c_str (),
                          resource_path.c_str ());
         goto error;
     }

     if (flux_respond_pack (h, msg, "{s:s}", "value", resp_value.c_str ()) < 0)
         flux_log_error (h, "%s", __FUNCTION__);

     return;

error:
    if (flux_respond_error (h, msg, errno, NULL) < 0)
        flux_log_error (h, "%s: flux_respond_error", __FUNCTION__);
}

static void grow_request_cb (flux_t *h, flux_msg_handler_t *w,
                              const flux_msg_t *msg, void *arg)
{
    int64_t jobid = -1;
    const char *success = "Success";
    const char *cmd = NULL;
    const char *subgraph = NULL;

    std::shared_ptr<resource_ctx_t> ctx = getctx ((flux_t *)arg);
    if (flux_request_unpack (msg, NULL, "{s:I s:s}",
                            "jobid", &jobid, "subgraph", &subgraph) < 0)
        goto error;

    if (!is_existent_jobid (ctx, jobid)) {
        errno = EINVAL;
        flux_log_error (h, "%s: nonexistent job (%jd).",
                        __FUNCTION__, (intmax_t)jobid);
        goto error;
    }

    if (run_grow (ctx, jobid, subgraph) < 0) {
        goto error;
    }

    if (flux_respond_pack (h, msg, "{s:s}", "result", success) < 0)
        flux_log_error (h, "%s", __FUNCTION__);

    return;

error:
    if (flux_respond_error (h, msg, errno, NULL) < 0)
        flux_log_error (h, "%s: flux_respond_error", __FUNCTION__);
}

/******************************************************************************
 *                                                                            *
 *                               Module Main                                  *
 *                                                                            *
 ******************************************************************************/

extern "C" int mod_main (flux_t *h, int argc, char **argv)
{
    int rc = -1;
    try {
        std::shared_ptr<resource_ctx_t> ctx = nullptr;
        uint32_t rank = 1;

        if ( !(ctx = init_module (h, argc, argv))) {
            flux_log (h, LOG_ERR, "%s: can't initialize resource module",
                      __FUNCTION__);
            goto done;
        }
        // Because mod_main is always active, the following is safe.
        flux_aux_set (h, "resource", &ctx, NULL);
        flux_log (h, LOG_DEBUG, "%s: resource module starting", __FUNCTION__);

        if ( (rc = init_resource_graph (ctx)) != 0) {
            flux_log (h, LOG_ERR,
                      "%s: can't initialize resource graph database",
                      __FUNCTION__);
            goto done;
        }
        flux_log (h, LOG_DEBUG, "%s: resource graph database loaded",
                  __FUNCTION__);

        if (( rc = flux_reactor_run (flux_get_reactor (h), 0)) < 0) {
            flux_log (h, LOG_ERR, "%s: flux_reactor_run: %s",
                      __FUNCTION__, strerror (errno));
            goto done;
        }
    }
    catch (std::exception &e) {
        flux_log_error (h, "%s: %s", __FUNCTION__, e.what ());
    }

done:
    return rc;
}

MOD_NAME ("resource");

/*
 * vi:tabstop=4 shiftwidth=4 expandtab
 */
