/*****************************************************************************\
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

#include "resource/schema/sched_data.hpp"

namespace Flux {
namespace resource_model {

allotment_t::allotment_t ()
{

}

void allotment_t::insert (const int64_t jobid, const int64_t span,
                         const std::string &jobtype)
{
    id2spantype[jobid].span = span;
    id2spantype[jobid].jobtype = jobtype;

    type2id[jobtype].insert (jobid);

}

void allotment_t::erase (const int64_t jobid)
{
    std::string jtype = id2spantype[jobid].jobtype;

    type2id[jtype].erase (jobid);
    id2spantype.erase (jobid);
}

allotment_t::~allotment_t ()
{

}

schedule_t::schedule_t ()
{

}

schedule_t::schedule_t (const schedule_t &o)
{
    int64_t base_time = 0;
    uint64_t duration = 0;
    size_t len = 0;

    // copy constructor does not copy the contents
    // of the schedule tables and of the planner objects.
    if (o.plans) {
        base_time = planner_multi_base_time (o.plans);
        duration = planner_multi_duration (o.plans);
        plans = planner_multi_new (base_time, duration,
                             planner_multi_resource_totals (o.plans),
                             planner_multi_resource_types (o.plans),
                             planner_multi_job_types (o.plans), len);
    }
}

schedule_t &schedule_t::operator= (const schedule_t &o)
{
    int64_t base_time = 0;
    uint64_t duration = 0;
    size_t len = 0;

    // assign operator does not copy the contents
    // of the schedule tables and of the planner objects.
    if (o.plans) {
        base_time = planner_multi_base_time (o.plans);
        duration = planner_multi_duration (o.plans);
        plans = planner_multi_new (base_time, duration,
                             planner_multi_resource_totals (o.plans),
                             planner_multi_resource_types (o.plans),
                             planner_multi_job_types (o.plans), len);
    }
    return *this;
}

schedule_t::~schedule_t ()
{
    if (plans)
        planner_multi_destroy (&plans);
}

} // resource_model
} // Flux

/*
 * vi:tabstop=4 shiftwidth=4 expandtab
 */
