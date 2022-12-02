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
#include <iostream>

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

    std::cout << "SCHED_T COPY CTOR\n";
    if (o.plans) {
        if (!plans)
            plans = new planner_t ();
        plans = new planner_t (*(o.plans));
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

    std::cout << "SCHED_T ASSIGN CTOR\n";
    
    if (o.plans) {
        if (!plans)
            plans = new planner_t ();
        *plans = *(o.plans);
        std::cout << "SCHED_T ASSIGN PLANS\n";
    }
    return *this;
}

schedule_t::~schedule_t ()
{
    if (plans)
        planner_destroy (&plans);

    allocations.clear ();
    reservations.clear ();
}

} // resource_model
} // Flux

/*
 * vi:tabstop=4 shiftwidth=4 expandtab
 */
