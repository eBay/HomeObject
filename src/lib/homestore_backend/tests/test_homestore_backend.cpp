/*********************************************************************************
 * Modifications Copyright 2017-2019 eBay Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 *********************************************************************************/
#include "homeobj_fixture.hpp"

SISL_OPTION_GROUP(
    test_homeobject_repl_common,
    (spdk, "", "spdk", "spdk", ::cxxopts::value< bool >()->default_value("false"), "true or false"),
    (dev_size_mb, "", "dev_size_mb", "size of each device in MB", ::cxxopts::value< uint64_t >()->default_value("2048"),
     "number"),
    (pg_size, "", "pg_size", "default size of pg in MB", ::cxxopts::value< uint64_t >()->default_value("100"),
     "number"),
    (num_threads, "", "num_threads", "number of threads", ::cxxopts::value< uint32_t >()->default_value("2"), "number"),
    (num_devs, "", "num_devs", "number of devices to create", ::cxxopts::value< uint32_t >()->default_value("3"),
     "number"),
    (use_file, "", "use_file", "use file instead of real drive", ::cxxopts::value< bool >()->default_value("false"),
     "true or false"),
    (init_device, "", "init_device", "init real device", ::cxxopts::value< bool >()->default_value("false"),
     "true or false"),
    (replicas, "", "replicas", "Total number of replicas", ::cxxopts::value< uint8_t >()->default_value("3"), "number"),
    (base_port, "", "base_port", "Port number of first replica", ::cxxopts::value< uint16_t >()->default_value("4000"),
     "number"),
    (replica_num, "", "replica_num", "Internal replica num (used to lauch multi process) - don't override",
     ::cxxopts::value< uint8_t >()->default_value("0"), "number"),
    (replica_dev_list, "", "replica_dev_list", "Device list for all replicas",
     ::cxxopts::value< std::vector< std::string > >(), "path [...]"),
    (qdepth, "", "qdepth", "Max outstanding operations", ::cxxopts::value< uint32_t >()->default_value("8"), "number"),
    (num_pgs, "", "num_pgs", "number of pgs", ::cxxopts::value< uint64_t >()->default_value("2"), "number"),
    (num_shards, "", "num_shards", "number of shards", ::cxxopts::value< uint64_t >()->default_value("4"), "number"),
    (num_blobs, "", "num_blobs", "number of blobs", ::cxxopts::value< uint64_t >()->default_value("20"), "number"),
    (is_restart, "", "is_restart", "the process is restart or the first start", ::cxxopts::value< bool >()->
        default_value("false"), "true or false"));

SISL_LOGGING_INIT(homeobject)
#define test_options logging, config, homeobject, test_homeobject_repl_common
SISL_OPTIONS_ENABLE(test_options)

std::unique_ptr< test_common::HSReplTestHelper > g_helper;

int main(int argc, char* argv[]) {
    int parsed_argc = argc;
    char** orig_argv = argv;
    std::vector< std::string > args;
    for (int i = 0; i < argc; ++i) {
        args.emplace_back(argv[i]);
    }
    ::testing::InitGoogleTest(&parsed_argc, argv);
    SISL_OPTIONS_LOAD(parsed_argc, argv, test_options);

    g_helper = std::make_unique< test_common::HSReplTestHelper >("test_homeobject", args, orig_argv);
    g_helper->setup(SISL_OPTIONS["replicas"].as< uint8_t >());
    auto ret = RUN_ALL_TESTS();
    g_helper->teardown();
    return ret;
}