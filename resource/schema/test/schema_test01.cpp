/*****************************************************************************\
 * Copyright 2014 Lawrence Livermore National Security, LLC
 * (c.f. AUTHORS, NOTICE.LLNS, LICENSE)
 *
 * This file is part of the Flux resource manager framework.
 * For details, see https://github.com/flux-framework.
 *
 * SPDX-License-Identifier: LGPL-3.0
\*****************************************************************************/

#if HAVE_CONFIG_H
# include <config.h>
#endif
#include <cstdlib>
#include <sstream>
#include <string>
#include <cstring>
#include <cstdint>
#include <cerrno>
#include <vector>
#include <map>
#include "sched_data.hpp"
#include "src/common/libtap/tap.h"

static void to_stream (int64_t base_time, uint64_t duration, uint64_t cnts,
                      const char *type, std::stringstream &ss)
{
    if (base_time != -1)
        ss << "B(" << base_time << "):";
    ss << "D(" << duration << "):" << "R_";
    ss << type << "(" << cnts << ")";
}

static int test_planner_getters ()
{
    bool bo = false;
    int64_t rc = -1;
    int64_t avail = -1;
    std::stringstream ss;
    planner_t *ctx = NULL;
    const char *type = NULL;
    uint64_t resource_total = 10;
    const char resource_type[] = "1";

    to_stream (0, 9999, resource_total, resource_type, ss);
    ctx = planner_new (0, 9999, resource_total, resource_type);
    ok (ctx != nullptr, "new with (%s)", ss.str ().c_str ());

    rc = planner_base_time (ctx);
    ok ((rc == 0), "base_time works for (%s)", ss.str ().c_str ());

    rc = planner_duration (ctx);
    ok ((rc == 9999), "duration works for (%s)", ss.str ().c_str ());

    avail = planner_resource_total (ctx);
    bo = (bo || (avail != 10));

    type = planner_resource_type (ctx);
    bo = (bo || (type == resource_type));

    ok (!bo, "planner getters work");

    planner_destroy (&ctx);
    return 0;
}



int main (int argc, char *argv[])
{
    plan (52);

    test_planner_getters ();

    done_testing ();

    return EXIT_SUCCESS;
}

/*
 * vi: ts=4 sw=4 expandtab
 */
