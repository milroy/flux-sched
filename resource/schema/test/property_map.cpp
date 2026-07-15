/*****************************************************************************\
 * Copyright 2024 Lawrence Livermore National Security, LLC
 * (c.f. AUTHORS, NOTICE.LLNS, LICENSE)
 *
 * This file is part of the Flux resource manager framework.
 * For details, see https://github.com/flux-framework.
 *
 * SPDX-License-Identifier: LGPL-3.0
\*****************************************************************************/

#if HAVE_CONFIG_H
#include <config.h>
#endif
#include <sstream>
#include <string>
#include <vector>
#include "src/common/libtap/tap.h"
#include "resource/schema/data_std.hpp"

using namespace Flux::resource_model;

//  Creation, upsert, removal, and the std::map-compatible surface the wrapper
//  exposes to existing call sites.
static void test_basic ()
{
    property_map_t p;
    ok (p.empty () && p.size () == 0, "new map is empty");

    p.insert ("gpu", "h100");
    p.insert (std::pair<std::string, std::string>{"class", "batch"});
    ok (p.size () == 2, "insert adds distinct keys");

    //  insert() leaves an existing key untouched and reports {*, false},
    //  matching std::map semantics the resource module relies on.
    auto r = p.insert ("gpu", "a100");
    ok (!r.second, "insert on existing key does not overwrite");
    ok (p.contains ("gpu", "h100"), "original value retained after insert");

    //  insert_or_assign is the explicit upsert (last-write-wins).
    p.insert_or_assign ("gpu", "a100");
    ok (p.contains ("gpu", "a100") && !p.contains ("gpu", "h100"),
        "insert_or_assign overwrites value");

    ok (p.find ("gpu") != p.end (), "find locates present key");
    ok (p.find ("absent") == p.end (), "find returns end() for absent key");
    ok (p.get ("gpu") != nullptr && p.get ("gpu")->get () == "a100",
        "get returns value for present key");
    ok (p.get ("absent") == nullptr, "get returns nullptr for absent key");
    ok (p.at ("class").get () == "batch", "at returns value (as std::string via get)");

    ok (p.erase ("class") == 1, "erase removes present key");
    ok (p.erase ("class") == 0, "erase of absent key is a no-op");
    ok (!p.contains ("class"), "erased key no longer present");

    p.clear ();
    ok (p.empty (), "clear empties the map");
}

//  The property test that matters for pruning: presence and exact key+value
//  queries, and that the interned fast-path overloads agree with the
//  string-view overloads.
static void test_match_queries ()
{
    property_map_t p;
    p.insert ("gpu", "h100");
    p.insert ("zone", "west");
    p.insert ("batch", "");  // presence-only property (empty value)

    ok (p.contains ("gpu"), "contains(key): present");
    ok (!p.contains ("fpga"), "contains(key): absent");
    ok (p.contains ("batch"), "contains(key): present even with empty value");

    ok (p.contains ("gpu", "h100"), "contains(key,value): exact match");
    ok (!p.contains ("gpu", "a100"), "contains(key,value): value mismatch");
    ok (!p.contains ("fpga", "x"), "contains(key,value): absent key");
    ok (p.contains ("batch", ""), "contains(key,value): empty-value match");

    //  Pre-interned overloads (the traverser hot path) must agree exactly.
    property_istr_t k_gpu{"gpu"}, v_h100{"h100"}, v_a100{"a100"};
    ok (p.contains (k_gpu), "interned contains(key) agrees");
    ok (p.contains (k_gpu, v_h100), "interned contains(key,value) agrees");
    ok (!p.contains (k_gpu, v_a100), "interned contains(key,value) rejects mismatch");
}

//  Interned equality is identity, so two independently constructed keys for the
//  same string are the same key (this is what makes the queries above O(1)).
static void test_interned_identity ()
{
    property_istr_t a{"perf_class"}, b{"perf_class"}, c{"other"};
    ok (a == b, "equal strings intern to equal keys");
    ok (!(a == c), "different strings are unequal");
    ok (a.id ().get () == b.id ().get (), "equal strings share one canonical entry");
}

//  Order-independent structural equality, used by the JGF reader to detect
//  property drift regardless of insertion order.
static void test_equality ()
{
    property_map_t a, b;
    a.insert ("x", "1");
    a.insert ("y", "2");
    b.insert ("y", "2");
    b.insert ("x", "1");
    ok (a == b, "maps equal regardless of insertion order");

    b.insert_or_assign ("x", "9");
    ok (a != b, "differing value makes maps unequal");

    b.insert_or_assign ("x", "1");
    b.insert ("z", "3");
    ok (a != b, "differing key set makes maps unequal");
}

//  sorted_by_key() must yield a stable, byte-wise (std::map-compatible) order.
//  Serialization (get-property, JGF/RV1/dot writers) depends on this so that
//  existing expected outputs continue to match; e.g. t3034 expects
//  "amd-mi60@gpu" before "arm-v9@core".
static void test_sorted_by_key ()
{
    property_map_t p;
    //  insert in a non-sorted order on purpose
    p.insert ("arm-v9@core", "");
    p.insert ("amd-mi60@gpu", "");

    std::vector<std::string> keys;
    for (auto &kv : p.sorted_by_key ())
        keys.push_back (kv.first.get ());
    ok (keys.size () == 2 && keys[0] == "amd-mi60@gpu" && keys[1] == "arm-v9@core",
        "sorted_by_key orders keys byte-wise (amd < arm), matching t3034");

    //  Uppercase sorts before lowercase; "beta10" < "beta2" (byte-wise), i.e.
    //  identical to how std::map<std::string,...> ordered the old field.
    property_map_t q;
    for (const char *k : {"zeta", "Alpha", "alpha", "beta10", "beta2"})
        q.insert (k, "v");
    std::ostringstream oss;
    for (auto &kv : q.sorted_by_key ())
        oss << kv.first.get () << " ";
    is (oss.str ().c_str (), "Alpha alpha beta10 beta2 zeta ",
        "sorted_by_key matches std::string byte ordering");

    //  key=value emission round-trips through the sorted view
    property_map_t r;
    r.insert ("gpu", "h100");
    std::ostringstream emit;
    for (auto &kv : r.sorted_by_key ())
        emit << kv.first.get () << "=" << kv.second.get ();
    is (emit.str ().c_str (), "gpu=h100", "sorted_by_key preserves values for emission");
}

int main (int argc, char *argv[])
{
    plan (NO_PLAN);

    test_basic ();
    test_match_queries ();
    test_interned_identity ();
    test_equality ();
    test_sorted_by_key ();

    done_testing ();
    return 0;
}

/*
 * vi: ts=4 sw=4 expandtab
 */
