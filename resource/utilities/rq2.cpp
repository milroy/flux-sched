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
#include <cerrno>

using namespace Flux;
using namespace Flux::resource_model;
using namespace Flux::resource_model::detail;

int match (resource_query_t &ctx) 
{
    int rc = -1;
    int64_t at = 0;
    bool orelse_reserve = false;
    bool reserved = false;
    std::string R = "";
    double ov = 0.0;

//    try {
    uint64_t jobid = ctx.get_job_counter ();
    std::ifstream ifs ("../../t/data/resource/jobspecs/basics/test001.yaml");
    std::string jobspec ( (std::istreambuf_iterator<char> (ifs) ),
                        (std::istreambuf_iterator<char> () ) );

    //std::ifstream jobspec_in (jobspec_fn);
    //if (!jobspec_in) {
    //    std::cerr << "ERROR: can't open " << jobspec_fn << std::endl;
    //    return 0;
    //}
    //Flux::Jobspec::Jobspec jobspec {jobspec_in};
    //jobspec_in.close ();

    rc = reapi_cli_t::match_allocate (&ctx, orelse_reserve, jobspec, jobid, reserved, R, at, ov);

//    } catch (parse_error &e) {
//        std::cerr << "ERROR: Jobspec error for " << ctx->jobid_counter <<": "
//                  << e.what () << std::endl;
//    }

    return rc;


}

int info (resource_query_t &ctx)
{
    int rc = -1;
    std::string mode = "";
    const uint64_t jobid = 1;
    bool reserved = false;
    int64_t at = 0;
    double overhead = 0.0;
    std::string err_msg = "";

    rc = reapi_cli_t::info (&ctx, jobid, mode, reserved, at, overhead);

    std::cout << "Info outputs: " << jobid << " " << mode << " " << reserved << " " << at << " " << overhead << " \n";
    std::cout << reapi_cli_t::get_err_message () << "\n";

    return rc;
}

int main (int argc, char *argv[])
{
    resource_query_t *ctx = nullptr;
    std::ifstream ifs ("../../t/data/resource/jgfs/tiny.json");
    std::string rgraph ( (std::istreambuf_iterator<char> (ifs) ),
                        (std::istreambuf_iterator<char> () ) );
    int match_out, info_out = 0;

    try {
        ctx = new resource_query_t (rgraph, "{\"matcher_policy\": \"low\"}");
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

    match_out = match (*ctx);
    info_out = info (*ctx);

    if (match_out == 0)
        std::cout << "Match succeeded!\n";

    if (info_out == 0)
        std::cout << "Info succeeded!\n";

    return EXIT_SUCCESS;
}
