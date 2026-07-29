/*****************************************************************************\
 * Copyright 2023 Lawrence Livermore National Security, LLC
 * (c.f. AUTHORS, NOTICE.LLNS, LICENSE)
 *
 * This file is part of the Flux resource manager framework.
 * For details, see https://github.com/flux-framework.
 *
 * SPDX-License-Identifier: LGPL-3.0
\*****************************************************************************/

#ifndef PLANNER_MULTI_HPP
#define PLANNER_MULTI_HPP

#include "planner.hpp"
#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <boost/multi_index_container.hpp>
#include <boost/multi_index/member.hpp>
#include <boost/multi_index/random_access_index.hpp>
#include <boost/multi_index/hashed_index.hpp>

struct request_multi {
    int64_t on_or_after = 0;
    uint64_t duration = 0;
    std::unordered_map<std::string, int64_t> counts;
};

struct planner_multi_meta {
    std::string resource_type;
    mutable uint64_t resource_total;  // Not an index; can mutate
    planner_t *planner;
};

/* tags for accessing the corresponding indices of planner_multi_meta */
struct idx {};
struct res_type {};

template<typename T>
struct polyfill_allocator : std::allocator<T> {
    using std::allocator<T>::allocator;
    template<typename U>
    struct rebind {
        using other = polyfill_allocator<U>;
    };
    using pointer = T *;
    using const_pointer = T const *;
    using reference = T &;
    using const_reference = T const &;
};

using boost::multi_index_container;
using namespace boost::multi_index;
typedef multi_index_container<
    planner_multi_meta,  // container data
    indexed_by<          // list of indexes
        random_access<   // analogous to vector
            tag<idx>     // index nametag
            >,
        hashed_unique<      // unordered_set-like; faster than ordered_unique in testing
            tag<res_type>,  // index nametag
            member<planner_multi_meta, std::string, &planner_multi_meta::resource_type>  // index's
                                                                                         // key
            >>,
    polyfill_allocator<planner_multi_meta>>
    multi_container;

class planner_multi {
   public:
    planner_multi ();
    planner_multi (int64_t base_time,
                   uint64_t duration,
                   const uint64_t *resource_totals,
                   const char **resource_types,
                   size_t len);
    planner_multi (const planner_multi &o);
    planner_multi &operator= (const planner_multi &o);
    bool operator== (const planner_multi &o) const;
    bool operator!= (const planner_multi &o) const;
    void erase ();
    ~planner_multi ();

    // Public getters and setters
    planner_t *get_planner_at (size_t i) const;
    planner_t *get_planner_at (const char *type) const;
    bool planner_at (const char *type) const;
    size_t get_planners_size () const;
    int64_t get_resource_total_at (size_t i) const;
    int64_t get_resource_total_at (const char *type) const;
    const char *get_resource_type_at (size_t i) const;
    size_t get_resource_type_idx (const char *type) const;
    struct request_multi &get_iter ();
    // Span lookup functions
    std::map<uint64_t, std::vector<int64_t>> &get_span_lookup ();
    std::map<uint64_t, std::vector<int64_t>>::iterator &get_span_lookup_iter ();
    void set_span_lookup_iter (std::map<uint64_t, std::vector<int64_t>>::iterator &it);
    void incr_span_lookup_iter ();
    // Get and set span_counter
    uint64_t get_span_counter ();
    void set_span_counter (uint64_t sc);
    void incr_span_counter ();
    // Update the planner set to match resource_types/resource_totals:
    // add planners for missing types, update the totals and positions of
    // existing ones, and delete planners for types absent from the
    // request. All-or-nothing: on failure returns -1 with errno set and
    // leaves the planner set, iterator requests, and span metadata
    // unchanged.
    int update (const uint64_t *resource_totals, const char **resource_types, size_t len);

   private:
    // The updated planner set, iterator request counts, and per-span
    // metadata vectors built on the side by stage_update (). Newly
    // created planners are owned by `added` until commit_update ()
    // installs them; planners for types absent from the request are
    // collected in `removed` but stay alive inside the planner_multi
    // until then.
    struct staged_update {
        multi_container set;
        std::unordered_map<std::string, int64_t> counts;
        std::vector<std::vector<int64_t>> span_vecs;
        std::vector<std::unique_ptr<planner_t>> added;
        std::vector<planner_t *> removed;
    };
    int stage_update (const uint64_t *resource_totals,
                      const char **resource_types,
                      size_t len,
                      staged_update &staged);
    void commit_update (staged_update &staged);

    multi_container m_types_totals_planners;
    struct request_multi m_iter;
    std::map<uint64_t, std::vector<int64_t>> m_span_lookup;
    std::map<uint64_t, std::vector<int64_t>>::iterator m_span_lookup_iter;
    uint64_t m_span_counter = 0;
};

struct planner_multi_t {
    planner_multi_t ();
    planner_multi_t (const planner_multi &o);
    planner_multi_t (int64_t base_time,
                     uint64_t duration,
                     const uint64_t *resource_totals,
                     const char **resource_types,
                     size_t len);
    ~planner_multi_t ();

    planner_multi *plan_multi = nullptr;
};

#endif /* PLANNER_MULTI_HPP */

/*
 * vi: ts=4 sw=4 expandtab
 */
