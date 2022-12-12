/*****************************************************************************\
 * Copyright 2021 Lawrence Livermore National Security, LLC
 * (c.f. AUTHORS, NOTICE.LLNS, LICENSE)
 *
 * This file is part of the Flux resource manager framework.
 * For details, see https://github.com/flux-framework.
 *
 * SPDX-License-Identifier: LGPL-3.0
\*****************************************************************************/

#include <limits>
#include "resource/schema/infra_data.hpp"

namespace Flux {
namespace resource_model {


/****************************************************************************
 *                                                                          *
 *   Public Methods on the Data Belonging to the Scheduler Infrastructure   *
 *                                                                          *
 ****************************************************************************/

infra_base_t::infra_base_t ()
{

}

infra_base_t::infra_base_t (const infra_base_t &o)
{
    member_of = o.member_of;
}

infra_base_t &infra_base_t::operator= (const infra_base_t &o)
{
    member_of = o.member_of;
    return *this;
}

infra_base_t::~infra_base_t ()
{

}


/****************************************************************************
 *                                                                          *
 *        Public Methods on Infrastructure Data for Resource Pool           *
 *                                                                          *
 ****************************************************************************/

pool_infra_t::pool_infra_t ()
{

}

pool_infra_t::pool_infra_t (const pool_infra_t &o): infra_base_t (o)
{
    std::cout << "BEGIN INFRA CTOR\n";
    ephemeral.clear ();
    tags = o.tags;
    x_spans = o.x_spans;
    job2span = o.job2span;
    colors = o.colors;

    std::cout << "INFRA CTOR AFTER MAPS\n";

    for (auto &kv : o.subplans) {
        planner_multi_t *mp = kv.second;
        if (!mp)
            continue;
        subplans[kv.first] = planner_multi_copy (mp);
    }
    std::cout << "INFRA CTOR AFTER PLANS\n";
    if (o.x_checker) {
        x_checker = planner_copy (o.x_checker);
    } else {
        x_checker = planner_new_empty ();
    }
    std::cout << "END INFRA CTOR\n";
}

pool_infra_t &pool_infra_t::operator= (const pool_infra_t &o)
{
    std::cout << "BEGIN INFRA == OL\n";
    ephemeral.clear ();
    tags = o.tags;
    x_spans = o.x_spans;
    job2span = o.job2span;
    colors = o.colors;

    //for (auto &kv : subplans) {
   //     planner_multi_destroy (&(kv.second));
    //}
    //subplans.clear ();
    //if (x_checker)
    //    planner_destroy (&x_checker);

    for (auto &kv : o.subplans) {
        planner_multi_t *omp = kv.second;
        planner_multi_t *mp = subplans.at (kv.first);
        if (!omp)
            continue;
        if (!mp) {
            subplans[kv.first] = planner_multi_copy (omp);
        } else {
            *mp = *(omp);
            subplans[kv.first] = mp;
        }
    }
    if (o.x_checker) {
        if (x_checker) {
            *x_checker = *(o.x_checker);
        }
        else {
            x_checker = planner_copy (o.x_checker);
        }
    } else {
        x_checker = planner_new_empty ();
    }
    std::cout << "END INFRA == OL\n";
    return *this;
}

pool_infra_t::~pool_infra_t ()
{
    for (auto &kv : subplans)
        planner_multi_destroy (&(kv.second));
    if (x_checker)
        planner_destroy (&x_checker);
}

void pool_infra_t::scrub ()
{
    tags.clear ();
    x_spans.clear ();
    job2span.clear ();
    for (auto &kv : subplans)
        planner_multi_destroy (&(kv.second));
    colors.clear ();
    if (x_checker)
        planner_destroy (&x_checker);
    ephemeral.clear ();
}


/****************************************************************************
 *                                                                          *
 *      Public Methods on Infrastructure Data for Resource Relation         *
 *                                                                          *
 ****************************************************************************/

relation_infra_t::relation_infra_t ()
{

}

relation_infra_t::relation_infra_t (const relation_infra_t &o): infra_base_t (o)
{
    m_needs = o.m_needs;
    m_trav_token = o.m_trav_token;
    m_exclusive = o.m_exclusive;
}

relation_infra_t &relation_infra_t::operator= (const relation_infra_t &o)
{
    infra_base_t::operator= (o);
    m_needs = o.m_needs;
    m_trav_token = o.m_trav_token;
    m_exclusive = o.m_exclusive;
    return *this;
}

relation_infra_t::~relation_infra_t ()
{

}

void relation_infra_t::scrub ()
{
    m_needs = 0;
    m_trav_token = 0;
    m_exclusive = 0;
}

void relation_infra_t::set_for_trav_update (uint64_t needs, int exclusive,
                                            uint64_t trav_token)
{
    m_needs = needs;
    m_trav_token = trav_token;
    m_exclusive = exclusive;
}

uint64_t relation_infra_t::get_needs () const
{
    return m_needs;
}

int relation_infra_t::get_exclusive () const
{
    return m_exclusive;
}

uint64_t relation_infra_t::get_trav_token () const
{
    return m_trav_token;
}

uint64_t relation_infra_t::get_weight () const
{
    return m_weight;
}

void relation_infra_t::set_weight (uint64_t weight)
{
    m_weight = weight;
}

} // resource_model
} // Flux

/*
 * vi:tabstop=4 shiftwidth=4 expandtab
 */
