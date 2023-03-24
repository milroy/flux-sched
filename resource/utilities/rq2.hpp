/*****************************************************************************\
 * Copyright 2022 Lawrence Livermore National Security, LLC
 * (c.f. AUTHORS, NOTICE.LLNS, LICENSE)
 *
 * This file is part of the Flux resource manager framework.
 * For details, see https://github.com/flux-framework.
 *
 * SPDX-License-Identifier: LGPL-3.0
\*****************************************************************************/

#include <memory>
#include <cerrno>
#include <vector>
#include <map>
#include "resource/reapi/bindings/c++/reapi_cli.hpp"
#include "resource/reapi/bindings/c++/reapi_cli_impl.hpp"

using namespace Flux;
using namespace Flux::resource_model;
using namespace Flux::resource_model::detail;

int match (resource_query_t &ctx, std::vector<std::string> &args);
int match_multi (resource_query_t &ctx, std::vector<std::string> &args);
int info (resource_query_t &ctx, std::vector<std::string> &args);
int help (resource_query_t &ctx, std::vector<std::string> &args);
int quit (resource_query_t &ctx, std::vector<std::string> &args);
int cancel (resource_query_t &ctx, std::vector<std::string> &args);
int find (resource_query_t &ctx, std::vector<std::string> &args);

