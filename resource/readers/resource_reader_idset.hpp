/*****************************************************************************\
 * Copyright 2026 Lawrence Livermore National Security, LLC
 * (c.f. AUTHORS, NOTICE.LLNS, LICENSE)
 *
 * This file is part of the Flux resource manager framework.
 * For details, see https://github.com/flux-framework.
 *
 * SPDX-License-Identifier: LGPL-3.0
\*****************************************************************************/

#ifndef RESOURCE_READER_IDSET_HPP
#define RESOURCE_READER_IDSET_HPP

#include "resource/readers/resource_reader_jgf.hpp"

namespace Flux {
namespace resource_model {

/*! IDSET resource reader class.
 *  Reads a JSON array of vertex IDs and expands to full resource graph.
 */
class resource_reader_idset_t : public resource_reader_jgf_t {
   public:
    virtual ~resource_reader_idset_t ();

    /*! Update resource graph g with idset array.
     *
     * \param g      resource graph
     * \param m      resource graph meta data
     * \param str    JSON array of vertex ID strings
     * \param jobid  jobid of str
     * \param at     start time of this job
     * \param dur    duration of this job
     * \param rsv    true if this update is for a reservation.
     * \param sequence_number
     *               traversal token to be used by traverser
     * \return       0 on success; non-zero integer on an error
     */
    int update (resource_graph_t &g,
                resource_graph_metadata_t &m,
                const std::string &str,
                int64_t jobid,
                int64_t at,
                uint64_t dur,
                bool rsv,
                uint64_t sequence_number) override;

   private:
    int fetch_additional_edges (resource_graph_t &g,
                                resource_graph_metadata_t &m,
                                std::map<std::string, vmap_val_t> &vmap,
                                fetch_helper_t &root,
                                std::vector<fetch_helper_t> &additional_vertices,
                                uint64_t sequence_number) override;

    int update_additional_edges (resource_graph_t &g,
                                 resource_graph_metadata_t &m,
                                 std::map<std::string, vmap_val_t> &vmap,
                                 fetch_helper_t &fetcher,
                                 uint64_t sequence_number);

    int fetch_additional_vertices (resource_graph_t &g,
                                   resource_graph_metadata_t &m,
                                   fetch_helper_t &fetcher,
                                   std::vector<fetch_helper_t> &additional_vertices) override;

    int recursively_collect_vertices (resource_graph_t &g,
                                      vtx_t v,
                                      std::vector<fetch_helper_t> &additional_vertices);
};

}  // namespace resource_model
}  // namespace Flux

#endif  // RESOURCE_READER_IDSET_HPP

/*
 * vi:tabstop=4 shiftwidth=4 expandtab
 */
