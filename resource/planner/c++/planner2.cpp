/*****************************************************************************\
 * Copyright 2024 Lawrence Livermore National Security, LLC
 * (c.f. AUTHORS, NOTICE.LLNS, LICENSE)
 *
 * This file is part of the Flux resource manager framework.
 * For details, see https://github.com/flux-framework.
 *
 * SPDX-License-Identifier: LGPL-3.0
\*****************************************************************************/

extern "C" {
#if HAVE_CONFIG_H
#include "config.h"
#endif
}

#include <limits>
#include <map>
#include <string>

#include "planner2.hpp"


/****************************************************************************
 *                                                                          *
 *                     Public Span_t Methods                                *
 *                                                                          *
 ****************************************************************************/

bool span_t::operator== (const span_t &o) const
{
    if (start != o.start)
        return false;
    if (last != o.last)
        return false;
    if (span_id != o.span_id)
        return false;
    if (resource_count != o.resource_count)
        return false;

    return true;
}

bool span_t::operator!= (const span_t &o) const
{
    return !operator == (o);
}

/****************************************************************************
 *                                                                          *
 *                     Public Planner Methods                               *
 *                                                                          *
 ****************************************************************************/

planner2::planner2 () = default;


/*
 * vi: ts=4 sw=4 expandtab
 */
