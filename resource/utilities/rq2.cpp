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
#include <getopt.h>
#include <string.h>
#include <jansson.h>
#include <boost/filesystem.hpp>

namespace fs = boost::filesystem;
using namespace Flux;
using namespace Flux::resource_model;
using namespace Flux::resource_model::detail;


#define OPTIONS "L:f:W:S:P:F:g:o:p:t:r:edh"
static const struct option longopts[] = {
    {"load-file",        required_argument,  0, 'L'},
    {"load-format",      required_argument,  0, 'f'},
    {"load-allowlist",   required_argument,  0, 'W'},
    {"match-subsystems", required_argument,  0, 'S'},
    {"match-policy",     required_argument,  0, 'P'},
    {"match-format",     required_argument,  0, 'F'},
    {"graph-format",     required_argument,  0, 'g'},
    {"graph-output",     required_argument,  0, 'o'},
    {"prune-filters",    required_argument,  0, 'p'},
    {"test-output",      required_argument,  0, 't'},
    {"reserve-vtx-vec",  required_argument,  0, 'r'},
    {"elapse-time",      no_argument,        0, 'e'},
    {"disable-prompt",   no_argument,        0, 'd'},
    {"help",             no_argument,        0, 'h'},
    { 0, 0, 0, 0 },
};

static void usage (int code)
{
    std::cerr <<
"usage: resource-query [OPTIONS...]\n"
"\n"
"Command-line utility that takes in an HPC resource request written in\n"
"Flux's Canonical Job Specification (or simply a jobspec) (RFC 14) and\n"
"selects the best-matching compute and other resources in accordance\n"
"with a selection policy.\n"
"\n"
"Read in a resource-graph generation recipe written in the GRUG format\n"
"and populate the resource-graph data store representing the compute and\n"
"other HPC resources and their relationships (RFC 4).\n"
"\n"
"Provide a simple command-line interface (cli) to allow users to allocate\n"
"or reserve the resource set in this resource-graph data store \n"
"using a jobspec as an input.\n"
"Traverse the resource graph in a predefined order for resource selection.\n"
"Currently only support one traversal type: depth-first traversal on the\n"
"dominant subsystem and up-walk traversal on one or more auxiliary \n"
"subsystems.\n"
"\n"
"OPTIONS allow for using a predefined matcher that is configured\n"
"to use a different set of subsystems as its dominant and/or auxiliary\n"
"ones to perform the matches on.\n"
"\n"
"OPTIONS also allow for instantiating a different resource-matching\n"
"selection policy--e.g., select resources with high or low IDs first.\n"
"\n"
"OPTIONS allow for exporting the filtered graph of the used matcher\n"
"in a selected graph format at the end of the cli session.\n"
"\n"
"To see cli commands, type in \"help\" in the cli: i.e.,\n"
"  % resource-query> help\n"
"\n"
"\n"
"\n"
"OPTIONS:\n"
"    -h, --help\n"
"            Display this usage information\n"
"\n"
"    -L, --load-file=filepath\n"
"            Input file from which to load the resource graph data store\n"
"            (default=conf/default)\n"
"\n"
"    -f, --load-format=<grug|hwloc|jgf|rv1exec>\n"
"            Format of the load file (default=grug)\n"
"\n"
"    -W, --load-allowlist=<resource1[,resource2[,resource3...]]>\n"
"            Allowlist of resource types to be loaded\n"
"            Resources that are not included in this list will be filtered out\n"
"\n"
"    -S, --match-subsystems="
         "<CA|IBA|IBBA|PFS1BA|PA|C+IBA|C+PFS1BA|C+PA|IB+IBBA|"
              "C+P+IBA|VA|V+PFS1BA|ALL>\n"
"            Set the predefined matcher to use. Available matchers are:\n"
"                CA: Containment Aware\n"
"                IBA: InfiniBand connection-Aware\n"
"                IBBA: InfiniBand Bandwidth-Aware\n"
"                PFS1BA: Parallel File System 1 Bandwidth-aware\n"
"                PA: Power-Aware\n"
"                C+IBA: Containment- and InfiniBand connection-Aware\n"
"                C+PFS1BA: Containment- and PFS1 Bandwidth-Aware\n"
"                C+PA: Containment- and Power-Aware\n"
"                IB+IBBA: InfiniBand connection and Bandwidth-Aware\n"
"                C+P+IBA: Containment-, Power- and InfiniBand connection-Aware\n"
"                VA: Virtual Hierarchy-Aware\n"
"                V+PFS1BA: Virtual Hierarchy and PFS1 Bandwidth-Aware \n"
"                ALL: Aware of everything.\n"
"            (default=CA).\n"
"\n"
"    -P, --match-policy=<low|high|lonode|hinode|lonodex|hinodex|first|locality|variation>\n"
"            Set the resource match selection policy. Available policies are:\n"
"                low: Select resources with low ID first\n"
"                high: Select resources with high ID first\n"
"                lonode: Select resources with lowest node ID first, \n"
"                        low ID first otherwise (e.g., node-local resource types) \n"
"                hinode: Select resources with highest node ID first, \n"
"                        high ID first otherwise (e.g., node-local resource types) \n"
"                lonodex: Same as lonode except each node is exclusively allocated \n"
"                hinodex: Same as hinode except each node is exclusively allocated \n"
"                first: Select the first matching resources and stop the search\n"
"                locality: Select contiguous resources first in their ID space\n"
"                variation: Allocate resources based on performance classes.\n"
"                                (perf_class must be set using set-property).\n"
"                (default=first).\n"
"\n"
"    -F, --match-format=<simple|pretty_simple|jgf|rlite|rv1|rv1_nosched>\n"
"            Specify the emit format of the matched resource set.\n"
"            (default=simple).\n"
"\n"
"    -p, --prune-filters=<HL-resource1:LL-resource1[,HL-resource2:LL-resource2...]...]>\n"
"            Install a planner-based filter at each High-Level (HL) resource\n"
"                vertex which tracks the state of the Low-Level (LL) resources\n"
"                in aggregate, residing under its subtree. If a jobspec requests\n"
"                1 node with 4 cores, and the visiting compute-node vertex has\n"
"                only a total of 2 available cores in aggregate at its\n"
"                subtree, this filter allows the traverser to prune a further descent\n"
"                to accelerate the search.\n"
"                Use the ALL keyword for HL-resource if you want LL-resource to be\n"
"                tracked at all of the HL-resource vertices. Examples:\n"
"                    rack:node,node:core\n"
"                    ALL:core,cluster:node,rack:node\n"
"                (default=ALL:core).\n"
"\n"
"    -g, --graph-format=<dot|graphml>\n"
"            Specify the graph format of the output file\n"
"            (default=dot).\n"
"\n"
"    -r, --reserve-vtx-vec=<size>\n"
"            Reserve the graph vertex size to optimize resource graph loading.\n"
"            The size value must be a non-zero integer up to 2000000.\n"
"\n"
"    -e, --elapse-time\n"
"            Print the elapse time per scheduling operation.\n"
"\n"
"    -d, --disable-prompt\n"
"            Don't print the prompt.\n"
"\n"
"    -o, --graph-output=<basename>\n"
"            Set the basename of the graph output file\n"
"            For AT&T Graphviz dot, <basename>.dot\n"
"            For GraphML, <basename>.graphml.\n"
"\n"
"    -t, --test-output=<filename>\n"
"            Set the output filename where allocated or reserved resource\n"
"            information is stored into.\n"
"\n";
    exit (code);
}

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

static void process_args (json_t *options,
                          int argc, char *argv[])
{
    int rc = 0;
    int ch = 0;
    std::string token;

    while ( (ch = getopt_long (argc, argv, OPTIONS, longopts, NULL)) != -1) {
        switch (ch) {
            case 'h': /* --help */
                usage (0);
                break;
            case 'L': /* --load-file */
                json_object_set_new (options, "load_file", json_string (optarg));
                if (!fs::exists (optarg)) {
                    std::cerr << "[ERROR] file does not exist for --load-file: ";
                    std::cerr << optarg << std::endl;
                    usage (1);
                } else if (fs::is_directory (optarg)) {
                    std::cerr << "[ERROR] path passed to --load-file is a directory: ";
                    std::cerr << optarg << std::endl;
                    usage (1);
                }
                break;
            case 'f': /* --load-format */
                json_object_set_new (options, "load_format", json_string (optarg));
                if (!known_resource_reader (optarg)) {
                    std::cerr << "[ERROR] unknown format for --load-format: ";
                    std::cerr << optarg << std::endl;
                    usage (1);
                }
                break;
            case 'W': /* --hwloc-allowlist */
                token = optarg;
                if (token.find_first_not_of (' ') != std::string::npos) {
                    if (!json_object_get (options,"load_allowlist")){
                        json_object_set_new (options, "load_allowlist", json_string (""));
                    }
                    json_object_set_new (options, "load_allowlist",
                                        json_pack ("s++",
                                        json_object_get (options,"load_allowlist"),
                                         "cluster,", token.c_str ()));
                }
                break;
            case 'S': /* --match-subsystems */
                json_object_set_new (options, "matcher_name", json_string (optarg));
                break;
            case 'P': /* --match-policy */
                json_object_set_new (options, "matcher_policy", json_string (optarg));
                break;
            case 'F': /* --match-format */
                json_object_set_new (options, "match_format", json_string (optarg));
                if (!known_match_format (optarg)) {
                    std::cerr << "[ERROR] unknown format for --match-format: ";
                    std::cerr << optarg << std::endl;
                    usage (1);
                }
                break;
            // case 'g': /* --graph-format */
            //     rc = string_to_graph_format (optarg, ctx->params.o_format);
            //     if ( rc != 0) {
            //         std::cerr << "[ERROR] unknown format for --graph-format: ";
            //         std::cerr << optarg << std::endl;
            //         usage (1);
            //     }
            //     graph_format_to_ext (ctx->params.o_format, ctx->params.o_fext);
            //     break;
            // case 'o': /* --graph-output */
            //     ctx->params.o_fname = optarg;
            //     break;
            case 'p': /* --prune-filters */
                token = optarg;
                if (token.find_first_not_of (' ') != std::string::npos) {
                    if (!json_object_get (options,"prune_filters")){
                        json_object_set_new (options, "prune_filters", json_string (token.c_str ()));
                    }else{
                      json_object_set_new (options, "prune_filters",
                                          json_pack ("s++",
                                          json_object_get (options,"prune_filters"),
                                           ",", token.c_str ()));
                    }

                }
                break;
            // case 't': /* --test-output */
            //     ctx->params.r_fname = optarg;
            //     break;
            case 'r': /* --reserve-vtx-vec */
                // If atoi fails, it defaults to 0, which is fine for us
                json_object_set_new (options, "match_format", json_integer (atoi (optarg)));
                if ( (atoi (optarg) < 0)
                    || (atoi (optarg) > 2000000)) {
                    json_object_set_new (options, "match_format", json_integer (0));
                    std::cerr
                        << "WARN: out of range specified for --reserve-vtx-vec: ";
                    std::cerr << optarg << std::endl;
                }
                break;
            // case 'e': /* --elapse-time */
            //     ctx->params.elapse_time = true;
            //     break;
            // case 'd': /* --disable-prompt */
            //     ctx->params.disable_prompt = true;
            //     break;
            default:
                usage (1);
                break;
        }
    }

    if (optind != argc)
        usage (1);
}

void get_rgraph (std::string &rgraph, json_t* options)
{
    const char *load_file = json_string_value (json_object_get (options, "load_file"));
    if (load_file == NULL) {
      load_file = "conf/default";
    }
    std::ifstream ifs (load_file);
    rgraph = std::string ( (std::istreambuf_iterator<char> (ifs) ),
                         (std::istreambuf_iterator<char> () ) );

}
int main (int argc, char *argv[])
{
    json_t *json_options = json_object ();
    std::string rgraph;

    process_args (json_options, argc, argv);
    get_rgraph (rgraph, json_options);


    std::string options = json_dumps (json_options, JSON_COMPACT);

    std::cout << options << std::endl;

    resource_query_t *ctx = nullptr;
    int match_out, info_out = 0;

    try {
        ctx = new resource_query_t (rgraph, options);
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
