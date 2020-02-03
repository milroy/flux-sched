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
    int64_t adaptbase_time = 0;
    uint64_t adaptduration = 0;
    int64_t elasticbase_time = 0;
    uint64_t elasticduration = 0;

    // copy constructor does not copy the contents
    // of the schedule tables and of the planner objects.
    if (o.plans) {
        base_time = planner_base_time (o.plans);
        duration = planner_duration (o.plans);
        plans = planner_new (base_time, duration,
                             planner_resource_total (o.plans),
                             planner_resource_type (o.plans));
    }

    if (o.adaptiveplans) {
        adaptbase_time = planner_base_time (o.adaptiveplans);
        adaptduration = planner_duration (o.adaptiveplans);
        adaptiveplans = planner_new (adaptbase_time, adaptduration,
                             planner_resource_total (o.adaptiveplans),
                             planner_resource_type (o.adaptiveplans));
    }

    if (o.elasticplans) {
        elasticbase_time = planner_base_time (o.elasticplans);
        elasticduration = planner_duration (o.elasticplans);
        elasticplans = planner_new (elasticbase_time, elasticduration,
                             planner_resource_total (o.elasticplans),
                             planner_resource_type (o.elasticplans));
    }
}

schedule_t &schedule_t::operator= (const schedule_t &o)
{
    int64_t base_time = 0;
    uint64_t duration = 0;
    int64_t adaptbase_time = 0;
    uint64_t adaptduration = 0;

    // assign operator does not copy the contents
    // of the schedule tables and of the planner objects.
    if (o.plans) {
        base_time = planner_base_time (o.plans);
        duration = planner_duration (o.plans);
        plans = planner_new (base_time, duration,
                             planner_resource_total (o.plans),
                             planner_resource_type (o.plans));
    }

    if (o.adaptiveplans) {
        adaptbase_time = planner_base_time (o.adaptiveplans);
        adaptduration = planner_duration (o.adaptiveplans);
        adaptiveplans = planner_new (adaptbase_time, adaptduration,
                             planner_resource_total (o.adaptiveplans),
                             planner_resource_type (o.adaptiveplans));
    }

    if (o.elasticplans) {
        elasticbase_time = planner_base_time (o.elasticplans);
        elasticduration = planner_duration (o.elasticplans);
        elasticplans = planner_new (elasticbase_time, elasticduration,
                             planner_resource_total (o.elasticplans),
                             planner_resource_type (o.elasticplans));
    }

    return *this;
}

schedule_t::~schedule_t ()
{
    if (plans)
        planner_destroy (&plans);

    if (adaptiveplans)
        planner_destroy (&adaptiveplans);

    if (elasticplans)
        planner_destroy (&elasticplans);
}

} // resource_model
} // Flux

/*
 * vi:tabstop=4 shiftwidth=4 expandtab
 */
