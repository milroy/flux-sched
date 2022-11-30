/*****************************************************************************\
 * Copyright 2021 Lawrence Livermore National Security, LLC
 * (c.f. AUTHORS, NOTICE.LLNS, LICENSE)
 *
 * This file is part of the Flux resource manager framework.
 * For details, see https://github.com/flux-framework.
 *
 * SPDX-License-Identifier: LGPL-3.0
\*****************************************************************************/

#include "resource/schema/sched_data.hpp"

namespace Flux {
namespace resource_model {

schedule_t::schedule_t ()
{

}

schedule_t::schedule_t (const schedule_t &o)
{
    for (auto const &alloc_it : o.allocations) {
        allocations.emplace (alloc_it.first, alloc_it.second);
    }
    for (auto const &reserv_it : o.reservations) {
        reservations.emplace (reserv_it.first, reserv_it.second);
    }

    if (o.plans) {
        plans = o.plans;
    }
}

schedule_t &schedule_t::operator= (const schedule_t &o)
{
    for (auto const &alloc_it : o.allocations) {
        allocations.emplace (alloc_it.first, alloc_it.second);
    }
    for (auto const &reserv_it : o.reservations) {
        reservations.emplace (reserv_it.first, reserv_it.second);
    }
    
    if (o.plans) {
        plans = o.plans;
    }
    return *this;
}

schedule_t::~schedule_t ()
{
    if (plans)
        planner_destroy (&plans);
}

} // resource_model
} // Flux

/*
 * vi:tabstop=4 shiftwidth=4 expandtab
 */
