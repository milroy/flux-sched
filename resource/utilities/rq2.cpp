/*****************************************************************************\
 * Copyright 2022 Lawrence Livermore National Security, LLC
 * (c.f. AUTHORS, NOTICE.LLNS, LICENSE)
 *
 * This file is part of the Flux resource manager framework.
 * For details, see https://github.com/flux-framework.
 *
 * SPDX-License-Identifier: LGPL-3.0
\*****************************************************************************/

#include "resource/hlapi/bindings/c++/reapi_cli.hpp"
#include "resource/hlapi/bindings/c++/reapi_cli_impl.hpp"

using namespace Flux;
using namespace Flux::resource_model;
using namespace Flux::resource_model::detail;


int main (int argc, char *argv[])
{
    resource_query_t *ctx = nullptr;
    std::ifstream ifs ("../../t/data/resource/jgfs/tiny.json");
    std::string rgraph ( (std::istreambuf_iterator<char> (ifs) ),
                        (std::istreambuf_iterator<char> () ) );

    try {
        ctx = new resource_query_t (rgraph, "{}");
    } catch (std::bad_alloc &e) {
        errno = ENOMEM;
        std::cerr << "Memory error\n";
        return EXIT_FAILURE;
    } catch (std::runtime_error &e) {
        errno = EPROTO;
        std::cerr << ": Runtime error: "
                     + std::string (e.what ()) + "\n";
        return EXIT_FAILURE;
    }

    std::cout << "Initialization succeeded!\n";

    return EXIT_SUCCESS;
}