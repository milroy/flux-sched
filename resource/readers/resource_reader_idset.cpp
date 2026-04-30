extern "C" {
#if HAVE_CONFIG_H
#include "config.h"
#endif
#include <flux/idset.h>
#include <jansson.h>
}

#include "resource/readers/resource_reader_idset.hpp"

using namespace Flux;
using namespace Flux::resource_model;

resource_reader_idset_t::~resource_reader_idset_t ()
{
}

int resource_reader_idset_t::fetch_additional_vertices (
    resource_graph_t &g,
    resource_graph_metadata_t &m,
    fetch_helper_t &fetcher,
    std::vector<fetch_helper_t> &additional_vertices)
{
    int rc = -1;
    std::map<std::string, vmap_val_t> empty_vmap{};  // so `find_vtx` doesn't error out
    vtx_t v = boost::graph_traits<resource_graph_t>::null_vertex ();
    if (!fetcher.exclusive)  // vertex isn't exclusive, nothing to do
        return 0;

    if ((rc = resource_reader_jgf_t::find_vtx (g, m, empty_vmap, fetcher, v)) != 0)
        return rc;

    return recursively_collect_vertices (g, v, additional_vertices);
}

int resource_reader_idset_t::recursively_collect_vertices (
    resource_graph_t &g,
    vtx_t v,
    std::vector<fetch_helper_t> &additional_vertices)
{
    static const subsystem_t containment_sub{"containment"};
    f_out_edg_iterator_t ei, ei_end;
    vtx_t target;

    if (v == boost::graph_traits<resource_graph_t>::null_vertex ()) {
        return -1;
    }

    for (boost::tie (ei, ei_end) = boost::out_edges (v, g); ei != ei_end; ++ei) {
        if (g[*ei].subsystem != containment_sub)
            continue;
        target = boost::target (*ei, g);

        fetch_helper_t vertex_copy;
        vertex_copy.type = g[target].type.c_str ();
        vertex_copy.basename = g[target].basename.c_str ();
        vertex_copy.size = g[target].size;
        vertex_copy.uniq_id = g[target].uniq_id;
        vertex_copy.rank = g[target].rank;
        vertex_copy.status = g[target].status;
        vertex_copy.id = g[target].id;
        vertex_copy.name = g[target].name;
        vertex_copy.properties = g[target].properties;
        vertex_copy.paths = g[target].paths;
        vertex_copy.unit = g[target].unit.c_str ();
        vertex_copy.exclusive = 1;  // must be exclusive as part of exclusive sub-tree
        if (resource_reader_jgf_t::apply_defaults (vertex_copy, g[target].name.c_str ()) < 0)
            return -1;

        additional_vertices.push_back (vertex_copy);
        if (recursively_collect_vertices (g, target, additional_vertices) < 0) {
            return -1;
        }
    }
    return 0;
}

int resource_reader_idset_t::fetch_additional_edges (
    resource_graph_t &g,
    resource_graph_metadata_t &m,
    std::map<std::string, vmap_val_t> &vmap,
    fetch_helper_t &root,
    std::vector<fetch_helper_t> &additional_vertices,
    uint64_t sequence_number)
{
    if (additional_vertices.size () > 0
        && update_additional_edges (g, m, vmap, root, sequence_number) < 0) {
        return -1;
    }
    // iterate through again to add edges
    for (auto &fetcher : additional_vertices) {
        if (update_additional_edges (g, m, vmap, fetcher, sequence_number) < 0) {
            return -1;
        }
    }
    return 0;
}

int resource_reader_idset_t::update_additional_edges (
    resource_graph_t &g,
    resource_graph_metadata_t &m,
    std::map<std::string, vmap_val_t> &vmap,
    fetch_helper_t &fetcher,
    uint64_t sequence_number)
{
    vtx_t v;
    std::map<std::string, vmap_val_t> empty_vmap{};  // so `find_vtx` doesn't error out
    std::string vertex_id = std::to_string (fetcher.uniq_id);
    fetcher.vertex_id = vertex_id.c_str ();
    if (resource_reader_jgf_t::find_vtx (g, m, empty_vmap, fetcher, v) != 0
        || v == boost::graph_traits<resource_graph_t>::null_vertex ())
        return -1;
    fetcher.vertex_id = nullptr;
    f_out_edg_iterator_t ei, ei_end;
    for (boost::tie (ei, ei_end) = boost::out_edges (v, g); ei != ei_end; ++ei) {
        if (g[*ei].subsystem != containment_sub)
            continue;
        std::string target_str = std::to_string (boost::target (*ei, g));
        if (update_src_edge (g, m, vmap, vertex_id, sequence_number) < 0
            || update_tgt_edge (g, m, vmap, vertex_id, target_str, sequence_number) < 0) {
            return -1;
        }
    }
    return 0;
}

int resource_reader_idset_t::update (resource_graph_t &g,
                                     resource_graph_metadata_t &m,
                                     const std::string &str,
                                     int64_t jobid,
                                     int64_t at,
                                     uint64_t dur,
                                     bool rsv,
                                     uint64_t sequence_number)
{
    int rc = -1;
    json_error_t err;
    json_t *idset_array = NULL;
    json_t *jgf_graph = NULL;
    json_t *jgf_nodes = NULL;
    json_t *jgf_edges = NULL;
    char *jgf_str = NULL;
    std::set<std::string> seen_ids;  // Track IDs to avoid duplicates

    // Parse the idset array
    if (!(idset_array = json_loads (str.c_str (), 0, &err))) {
        errno = EINVAL;
        goto done;
    }

    if (!json_is_array (idset_array)) {
        json_decref (idset_array);
        errno = EINVAL;
        goto done;
    }

    // Create JGF structure
    if (!(jgf_nodes = json_array ())) {
        json_decref (idset_array);
        errno = ENOMEM;
        goto done;
    }

    if (!(jgf_edges = json_array ())) {
        json_decref (idset_array);
        json_decref (jgf_nodes);
        errno = ENOMEM;
        goto done;
    }

    // For each vertex ID in the array, look up the vertex and create full JGF node
    size_t index;
    json_t *value;

    json_array_foreach (idset_array, index, value) {
        const char *id_str = json_string_value (value);
        if (!id_str) {
            json_decref (idset_array);
            json_decref (jgf_nodes);
            json_decref (jgf_edges);
            errno = EINVAL;
            goto done;
        }

        // Skip duplicates
        if (seen_ids.find (id_str) != seen_ids.end ()) {
            continue;
        }
        seen_ids.insert (id_str);

        // Convert ID string to uniq_id
        int64_t uniq_id = std::strtoll (id_str, NULL, 10);

        // Find the vertex in the graph with this uniq_id
        vtx_t v = boost::graph_traits<resource_graph_t>::null_vertex ();
        f_vtx_iterator_t vi, vi_end;
        for (boost::tie (vi, vi_end) = boost::vertices (g); vi != vi_end; ++vi) {
            if (g[*vi].uniq_id == uniq_id) {
                v = *vi;
                break;
            }
        }

        if (v == boost::graph_traits<resource_graph_t>::null_vertex ()) {
            json_decref (idset_array);
            json_decref (jgf_nodes);
            json_decref (jgf_edges);
            errno = EINVAL;
            goto done;
        }

        // Determine if this vertex is an exclusive root by checking if its children are in idset
        // If children are NOT in idset, this is an exclusive root; otherwise it's not exclusive
        bool is_exclusive = true;  // Assume exclusive unless proven otherwise
        static const subsystem_t containment_sub{"containment"};
        f_out_edg_iterator_t ei, ei_end;
        for (boost::tie (ei, ei_end) = boost::out_edges (v, g); ei != ei_end; ++ei) {
            if (g[*ei].subsystem != containment_sub)
                continue;
            vtx_t child = boost::target (*ei, g);
            std::string child_id_str = std::to_string (g[child].uniq_id);
            if (seen_ids.find (child_id_str) != seen_ids.end ()) {
                // Child is in idset, so this vertex is not an exclusive root
                is_exclusive = false;
                break;
            }
        }

        // Create full JGF node with metadata from the graph vertex
        json_t *metadata = json_object ();
        json_object_set_new (metadata, "type", json_string (g[v].type.c_str ()));
        if (is_exclusive) {
            json_object_set_new (metadata, "exclusive", json_true ());
        }

        // Add paths
        json_t *paths = json_object ();
        for (auto &kv : g[v].paths) {
            json_object_set_new (paths, kv.first.c_str (), json_string (kv.second.c_str ()));
        }
        json_object_set_new (metadata, "paths", paths);

        // Add all required fields for proper vertex identification
        if (!g[v].name.empty ()) {
            json_object_set_new (metadata, "name", json_string (g[v].name.c_str ()));
        }
        if (g[v].rank >= 0) {
            json_object_set_new (metadata, "rank", json_integer (g[v].rank));
        }
        if (!g[v].basename.empty ()) {
            json_object_set_new (metadata, "basename", json_string (g[v].basename.c_str ()));
        }
        json_object_set_new (metadata, "uniq_id", json_integer (g[v].uniq_id));
        json_object_set_new (metadata, "id", json_integer (g[v].id));

        json_t *node = json_object ();
        json_object_set_new (node, "id", json_string (id_str));
        json_object_set_new (node, "metadata", metadata);

        if (json_array_append_new (jgf_nodes, node) < 0) {
            json_decref (idset_array);
            json_decref (jgf_nodes);
            json_decref (jgf_edges);
            errno = ENOMEM;
            goto done;
        }
    }

    // Create the JGF structure
    if (!(jgf_graph = json_pack ("{s:{s:o s:o}}", "graph", "nodes", jgf_nodes, "edges", jgf_edges))) {
        json_decref (idset_array);
        errno = ENOMEM;
        goto done;
    }

    // Convert to string
    if (!(jgf_str = json_dumps (jgf_graph, JSON_COMPACT))) {
        json_decref (idset_array);
        json_decref (jgf_graph);
        errno = ENOMEM;
        goto done;
    }

    // Call parent class update with the JGF string
    rc = resource_reader_jgf_t::update (g, m, std::string (jgf_str), jobid, at, dur, rsv, sequence_number);

    json_decref (idset_array);
    json_decref (jgf_graph);
    free (jgf_str);

done:
    return rc;
}
