/*****************************************************************************\
 * Copyright 2021 Lawrence Livermore National Security, LLC
 * (c.f. AUTHORS, NOTICE.LLNS, LICENSE)
 *
 * This file is part of the Flux resource manager framework.
 * For details, see https://github.com/flux-framework.
 *
 * SPDX-License-Identifier: LGPL-3.0
\*****************************************************************************/

#ifndef DATA_STD_H
#define DATA_STD_H

#include "data_std.hpp"

#include <algorithm>
#include <map>
#include <set>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>
#include <src/common/libintern/interner.hpp>

namespace Flux {
namespace resource_model {

// We create an x_checker planner for each resource vertex for quick exclusivity
// checking. We update x_checker for all of the vertices involved in each
// job allocation/reservation -- subtract 1 from x_checker planner for the
// scheduled span. Any vertex with less than X_CHECKER_NJOBS available in its
// x_checker cannot be exclusively allocated or reserved.
const char *const X_CHECKER_JOBS_STR = "jobs";
const int64_t X_CHECKER_NJOBS = 0x40000000;

constexpr uint64_t subsystem_id{0};
struct subsystem_tag {};
using subsystem_t = intern::interned_string<intern::dense_storage<subsystem_tag, uint8_t>>;
extern subsystem_t containment_sub;

constexpr uint64_t resource_type_id{1};
struct resource_type_tag {};
using resource_type_t = intern::interned_string<intern::dense_storage<resource_type_tag, uint16_t>>;
extern resource_type_t slot_rt;
extern resource_type_t xor_slot_rt;
extern resource_type_t cluster_rt;
extern resource_type_t rack_rt;
extern resource_type_t node_rt;
extern resource_type_t socket_rt;
extern resource_type_t gpu_rt;
extern resource_type_t core_rt;
extern resource_type_t ssd_rt;
extern resource_type_t storage_node_rt;

template<class T, int likely_count = 2>
using subsystem_key_vec = intern::interned_key_vec<subsystem_t, T, likely_count>;
using multi_subsystems_t = intern::interned_key_vec<subsystem_t, std::string>;
using multi_subsystemsS = intern::interned_key_vec<subsystem_t, std::set<std::string>>;

// Interned resource-property strings.
//
// Property names *and* values are drawn from an open, user-influenced
// vocabulary: they arrive through RFC 20 / JGF input and can be mutated at run
// time via the resource module (set-property / remove-property RPCs). Unlike
// subsystem_t and resource_type_t -- small, closed vocabularies that use dense
// storage -- properties therefore use refcounted (sparse) interner storage:
//
//   * memory stays bounded under adversarial or churny input (entries are
//     reclaimed once no vertex references them), and
//   * every name/value comparison collapses to an O(1) identity check on the
//     canonical shared_ptr the interner hands back for equal strings, so the
//     traverser never pays string-length or hashing costs on the hot path.
//
// A single pool backs both keys and values; "gpu" as a name and "gpu" as a
// value simply share one canonical entry, which is harmless. Split the tag if
// separate key/value pools are ever desired.
struct property_tag {};
using property_istr_t = intern::interned_string<intern::rc_storage<property_tag>>;

/*! Per-vertex property map: { interned name -> interned value }.
 *
 *  Thin, allocation-light wrapper over a std::map of interned strings. The
 *  container is deliberately an ordered map rather than an unordered_map:
 *  because the keys are interned, every comparison is already an O(1) id/pointer
 *  compare, so the usual reason to prefer a hash map with string keys (avoiding
 *  O(L) strcmp) does not apply here. What remains is unordered_map's fixed
 *  per-probe overhead -- hashing plus a prime-bucket modulo -- which, for the
 *  small property counts seen per vertex (<~100), measures several times slower
 *  than a red-black tree walk of ~log2(N) pointer comparisons. A std::map is
 *  thus faster on the traversal hot path (lookup) and lighter on insertion,
 *  while still avoiding any string work. flat_map is a reasonable alternative
 *  when aggregate memory across many vertices dominates; the wrapper makes the
 *  container a one-line implementation detail.
 *
 *  The public surface intentionally mirrors the std::map<std::string,
 *  std::string> it replaces (insert / erase / find / at / contains / iterate /
 *  ==), so existing call sites migrate with minimal churn. Iterators expose
 *  interned strings, which provide .get()/.c_str()/operator<<, satisfying the
 *  writers' associative_cstr_key concept unchanged.
 */
class property_map_t {
   public:
    using key_type = property_istr_t;
    using mapped_type = property_istr_t;
    using map_type = std::map<key_type, mapped_type>;
    using value_type = map_type::value_type;
    using iterator = map_type::iterator;
    using const_iterator = map_type::const_iterator;

    property_map_t () = default;
    property_map_t (const property_map_t &) = default;
    property_map_t (property_map_t &&) = default;
    property_map_t &operator= (const property_map_t &) = default;
    property_map_t &operator= (property_map_t &&) = default;

    // ---- capacity ----------------------------------------------------------
    bool empty () const
    {
        return m_map.empty ();
    }
    std::size_t size () const
    {
        return m_map.size ();
    }
    void clear ()
    {
        m_map.clear ();
    }

    // ---- iteration (kv.first / kv.second are interned strings) -------------
    iterator begin ()
    {
        return m_map.begin ();
    }
    iterator end ()
    {
        return m_map.end ();
    }
    const_iterator begin () const
    {
        return m_map.begin ();
    }
    const_iterator end () const
    {
        return m_map.end ();
    }
    const_iterator cbegin () const
    {
        return m_map.cbegin ();
    }
    const_iterator cend () const
    {
        return m_map.cend ();
    }

    // ---- creation (interns on the way in) ----------------------------------
    // Return type mirrors std::unordered_map::insert so callers that inspect
    // .second keep compiling. Like the map it replaces, an existing key is left
    // untouched (no overwrite) and reported as {it, false}.
    std::pair<iterator, bool> insert (std::string_view key, std::string_view value)
    {
        return m_map.insert ({key_type{key}, mapped_type{value}});
    }
    std::pair<iterator, bool> insert (const std::pair<std::string, std::string> &kv)
    {
        return insert (kv.first, kv.second);
    }
    // Last-write-wins upsert.
    void insert_or_assign (std::string_view key, std::string_view value)
    {
        m_map.insert_or_assign (key_type{key}, mapped_type{value});
    }

    // ---- removal -----------------------------------------------------------
    std::size_t erase (std::string_view key)
    {
        return m_map.erase (key_type{key});
    }

    // ---- O(1) queries ------------------------------------------------------
    // Presence of a name.
    bool contains (std::string_view key) const
    {
        return m_map.find (key_type{key}) != m_map.end ();
    }
    // Exact name+value match.
    bool contains (std::string_view key, std::string_view value) const
    {
        auto it = m_map.find (key_type{key});
        return it != m_map.end () && it->second == mapped_type{value};
    }
    // Pre-interned fast paths: zero interning cost at call time. Use these on
    // hot paths after interning the query term once.
    bool contains (key_type key) const
    {
        return m_map.find (key) != m_map.end ();
    }
    bool contains (key_type key, mapped_type value) const
    {
        auto it = m_map.find (key);
        return it != m_map.end () && it->second == value;
    }

    iterator find (std::string_view key)
    {
        return m_map.find (key_type{key});
    }
    const_iterator find (std::string_view key) const
    {
        return m_map.find (key_type{key});
    }

    // Non-throwing value lookup; nullptr when absent.
    const mapped_type *get (std::string_view key) const
    {
        auto it = m_map.find (key_type{key});
        return it == m_map.end () ? nullptr : &it->second;
    }
    // Throwing accessor mirroring std::map::at. Callers needing a std::string
    // (e.g. std::stoi) should use .at(...).get().
    const mapped_type &at (std::string_view key) const
    {
        return m_map.at (key_type{key});
    }

    // ---- structural (order-independent) equality ---------------------------
    // Used by the JGF reader to detect property drift between an incoming and
    // an existing vertex.
    bool operator== (const property_map_t &o) const
    {
        return m_map == o.m_map;
    }
    bool operator!= (const property_map_t &o) const
    {
        return !(*this == o);
    }

    // ---- deterministic ordering for output / serialization -----------------
    // Iteration follows interned-id order (an artifact of allocation/insertion),
    // not lexicographic name order, and is not even stable across runs. Any
    // user-facing or test-compared emission (get-property, the JGF/RV1 and dot
    // writers) must reproduce a stable, name-sorted ordering, so it iterates
    // this view instead of the map directly. Uses the same byte-wise key
    // ordering std::map<std::string,...> did, so existing expected outputs match
    // unchanged. This is a cold path; the query methods above never touch it.
    // The returned pairs hold interned strings, so they still satisfy the
    // writers' associative_cstr_key concept (.c_str()/.get()).
    std::vector<std::pair<key_type, mapped_type>> sorted_by_key () const
    {
        std::vector<std::pair<key_type, mapped_type>> out;
        out.reserve (m_map.size ());
        for (auto &kv : m_map)
            out.emplace_back (kv.first, kv.second);
        std::sort (out.begin (), out.end (), [] (auto const &a, auto const &b) {
            return a.first.get () < b.first.get ();
        });
        return out;
    }

   private:
    map_type m_map;
};

}  // namespace resource_model
}  // namespace Flux

#endif  // DATA_STD_H

/*
 * vi:tabstop=4 shiftwidth=4 expandtab
 */
