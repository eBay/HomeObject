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
/*
 * Homeobject Replication testing binaries shared common definitions, apis and data structures
 */

#pragma once
#include <mutex>
#include <condition_variable>
#include <map>
#include <set>
#include <boost/process.hpp>
#include <boost/asio.hpp>
#include <boost/interprocess/shared_memory_object.hpp>
#include <boost/interprocess/mapped_region.hpp>
#include <boost/interprocess/sync/interprocess_mutex.hpp>
#include <boost/interprocess/sync/interprocess_condition.hpp>
#include <boost/uuid/string_generator.hpp>
#include <boost/uuid/nil_generator.hpp>

#include <sisl/logging/logging.h>
#include <sisl/options/options.h>
#include <sisl/settings/settings.hpp>
#include <sisl/grpc/rpc_client.hpp>
#include <iomgr/io_environment.hpp>
#include <iomgr/http_server.hpp>

#include <folly/init/Init.h>

#include "homeobject/common.hpp"

namespace bip = boost::interprocess;
using namespace homeobject;

#define INVALID_UINT64_ID UINT64_MAX

namespace test_common {

class HSReplTestHelper {
protected:
    struct IPCData {
        void sync(uint64_t sync_point, uint32_t max_count) {
            std::unique_lock lg(mtx_);
            LOGINFO("=== Syncing: replica={}(total {}), sync_point_num={} ===", homeobject_replica_count_, max_count,
                    sync_point);
            ++homeobject_replica_count_;
            if (homeobject_replica_count_ == max_count) {
                sync_point_num_ = sync_point;
                homeobject_replica_count_ = 0;
                uint64_id_ = INVALID_UINT64_ID;
                auxiliary_uint64_id_ = UINT64_MAX;
                cv_.notify_all();
            } else {
                cv_.wait(lg, [this, sync_point]() { return sync_point_num_ == sync_point; });
            }
        }

        void set_uint64_id(uint64_t input_uint64_id) {
            std::unique_lock lg(mtx_);
            uint64_id_ = input_uint64_id;
        }

        uint64_t get_uint64_id() {
            std::unique_lock lg(mtx_);
            return uint64_id_;
        }

        void set_auxiliary_uint64_id(uint64_t input_auxiliary_uint64_id) {
            std::unique_lock lg(mtx_);
            auxiliary_uint64_id_ = input_auxiliary_uint64_id;
        }
        uint64_t get_auxiliary_uint64_id() {
            std::unique_lock lg(mtx_);
            return auxiliary_uint64_id_;
        }

    private:
        bip::interprocess_mutex mtx_;
        bip::interprocess_condition cv_;
        uint8_t homeobject_replica_count_{0};

        // the following variables are used to share shard_id, blob_id and others among different replicas
        uint64_t uint64_id_{0};
        uint64_t auxiliary_uint64_id_{0};

        // the nth synchronization point, that is how many times different replicas have synced
        uint64_t sync_point_num_{UINT64_MAX};
    };

public:
    class TestReplApplication : public homeobject::HomeObjectApplication {
    private:
        HSReplTestHelper& helper_;

    public:
        TestReplApplication(HSReplTestHelper& h) : helper_{h} {}
        virtual ~TestReplApplication() = default;

        // implement all the virtual functions in HomeObjectApplication
        bool spdk_mode() const override { return SISL_OPTIONS["spdk"].as< bool >(); }
        uint32_t threads() const override { return SISL_OPTIONS["num_threads"].as< uint32_t >(); }

        std::list< device_info_t > devices() const override {
            auto const use_file = SISL_OPTIONS["use_file"].as< bool >();
            std::list< device_info_t > devs;
            if (SISL_OPTIONS.count("device_list") && !use_file) {
                for (const auto& dev : helper_.dev_list_)
                    devs.emplace_back(dev, DevType::HDD);
            } else {
                for (const auto& dev : helper_.generated_devs)
                    devs.emplace_back(dev, DevType::HDD);
            }
            return devs;
        }

        peer_id_t discover_svcid(std::optional< peer_id_t > const& p) const override {
            if (p.has_value()) RELEASE_ASSERT_EQ(p.value(), helper_.my_replica_id_, "input svcid not matching");
            return helper_.my_replica_id_;
        }

        std::string lookup_peer(peer_id_t const& pid) const override {
            uint16_t port;
            if (auto it = helper_.members_.find(pid); it != helper_.members_.end()) {
                port = SISL_OPTIONS["base_port"].as< uint16_t >() + it->second;
            } else {
                RELEASE_ASSERT(false, "Gotten lookup_peer call for a non member");
            }

            return std::string("127.0.0.1:") + std::to_string(port);
        }

        void get_prometheus_metrics(const Pistache::Rest::Request&, Pistache::Http::ResponseWriter response) {
            response.send(Pistache::Http::Code::Ok, sisl::MetricsFarm::getInstance().report(sisl::ReportFormat::kTextFormat));
        }

        void start_http_server() {
            std::vector< iomgr::http_route > routes = {
                {Pistache::Http::Method::Get, "/metrics",
                    Pistache::Rest::Routes::bind(&TestReplApplication::get_prometheus_metrics, this)},
            };

            auto http_server = ioenvironment.get_http_server();
            if (!http_server) {
                LOGERROR("http server not available");
                return;
            }
            try {
                http_server->setup_routes(routes);
            } catch (std::runtime_error const& e) { LOGERROR("setup routes failed, {}", e.what()); }
            http_server->start();
            LOGD("http server is running");
        }

        void stop_http_server() {
            auto http_server = ioenvironment.get_http_server();
            if (!http_server) {
                LOGWARN("http server not available");
                return;
            }
            http_server->stop();
        }
    };


public:
    friend class TestReplApplication;

    HSReplTestHelper(std::string const& name, std::vector< std::string > const& args, char** argv) :
            test_name_{name}, args_{args}, argv_{argv} {}

    void setup(uint8_t num_replicas) {
        total_replicas_nums_ = num_replicas;
        replica_num_ = SISL_OPTIONS["replica_num"].as< uint8_t >();

        sisl::logging::SetLogger(test_name_ + std::string("_replica_") + std::to_string(replica_num_));
        sisl::logging::SetLogPattern("[%D %T%z] [%^%L%$] [%n] [%t] %v");

        boost::uuids::string_generator gen;
        for (uint32_t i{0}; i < num_replicas; ++i) {
            auto replica_id = gen(fmt::format("{:04}", i) + std::string("0123456789abcdef0123456789ab"));
            if (i == replica_num_) { my_replica_id_ = replica_id; }
            members_.insert(std::pair(replica_id, i));
        }

        // example:
        // --num_replicas 3 --replica_dev_list replica_0_dev_1, replica_0_dev_2, replica_0_dev_3, replica_1_dev_1,
        // replica_1_dev_2, replica_1_dev_3, replica_2_dev_1, replica_2_dev_2, replica_2_dev_3    // every replica 3
        // devs;
        // --num_replicas 3 --replica_dev_list replica_0_dev_1, replica_1_dev_1, replica_2_dev_1  // <<< every
        // replica has 1 dev;
        std::vector< std::string > dev_list_all;
        std::vector< std::vector< std::string > > rdev_list(num_replicas);
        if (SISL_OPTIONS.count("replica_dev_list")) {
            dev_list_all = SISL_OPTIONS["replica_dev_list"].as< std::vector< std::string > >();
            RELEASE_ASSERT(dev_list_all.size() % num_replicas == 0,
                           "Number of replica devices should be times of number replicas");
            LOGINFO("Device list from input={}", fmt::join(dev_list_all, ","));
            uint32_t num_devs_per_replica = dev_list_all.size() / num_replicas;
            for (uint32_t i{0}; i < num_replicas; ++i) {
                for (uint32_t j{0}; j < num_devs_per_replica; ++j) {
                    rdev_list[i].push_back(dev_list_all[i * num_devs_per_replica + j]);
                }
            }
            for (auto const& dev : rdev_list[replica_num_]) {
                dev_list_.emplace_back(dev);
            }
        }
        name_ = test_name_ + std::to_string(replica_num_);

        if (replica_num_ == 0) {
            // Erase previous shmem and create a new shmem with IPCData structure
            bip::shared_memory_object::remove("HO_repl_test_shmem");

            // kill the previous processes using the port
            for (uint32_t i = 0; i < num_replicas; ++i)
                check_and_kill(SISL_OPTIONS["base_port"].as< uint16_t >() + i);

            shm_ =
                std::make_unique< bip::shared_memory_object >(bip::create_only, "HO_repl_test_shmem", bip::read_write);
            shm_->truncate(sizeof(IPCData));
            region_ = std::make_unique< bip::mapped_region >(*shm_, bip::read_write);
            ipc_data_ = new (region_->get_address()) IPCData;

            for (uint32_t i{1}; i < num_replicas; ++i) {
                spawn_homeobject_process(i, false);
                sleep(1); // Don't spawn processes at the same time to avoid conflict on updating config files
            }
        } else {
            shm_ = std::make_unique< bip::shared_memory_object >(bip::open_only, "HO_repl_test_shmem", bip::read_write);
            region_ = std::make_unique< bip::mapped_region >(*shm_, bip::read_write);
            ipc_data_ = static_cast< IPCData* >(region_->get_address());
            if (SISL_OPTIONS["is_restart"].as<bool>()) {
                // reset sync point to the next sync point before restart
                sync_point_num = ipc_data_->sync_point_num_ + 1;
            }
        }

        int tmp_argc = 1;
        folly_ = std::make_unique< folly::Init >(&tmp_argc, &argv_, true);

        LOGINFO("Starting HomeObject replica={}", replica_num_);
        app = std::make_shared< TestReplApplication >(*this);
    }

    void spawn_homeobject_process(uint8_t replica_num, bool is_restart) {
        std::string cmd_line;
        fmt::format_to(std::back_inserter(cmd_line), "{} --replica_num {} --is_restart={}", args_[0], replica_num,
                       is_restart? "true" : "false");
        if (is_restart) {
            auto ut = testing::UnitTest::GetInstance();
            std::string pattern = "";
            bool is_following = false;
            for (int i = 0; i < ut->total_test_suite_count(); i++) {
                auto ts = ut->GetTestSuite(i);
                for (int j = 0; j < ts->total_test_count(); j++) {
                    auto ti = ts->GetTestInfo(j);
                    if (!is_following && ti == ut->current_test_info()) { is_following = true; }
                    if (is_following && ti->should_run()) {
                        if (!pattern.empty()) { fmt::format_to(std::back_inserter(pattern), ":"); }
                        fmt::format_to(std::back_inserter(pattern), "{}", ti->test_case_name());
                        fmt::format_to(std::back_inserter(pattern), ".{}", ti->name());
                    }
                }
            }
            LOGINFO("Restart, gtest filter pattern: {}", pattern);
            if ("" != pattern) { fmt::format_to(std::back_inserter(cmd_line), " --gtest_filter={}", pattern); }
        }
        for (int j{1}; j < (int)args_.size(); ++j) {
            if (is_restart && args_[j].find("--gtest_filter") != std::string::npos) {
                continue; // Ignore existing gtest_filter
            }
            fmt::format_to(std::back_inserter(cmd_line), " {}", args_[j]);
        }
        LOGINFO("Spawning Homeobject cmd: {}", cmd_line);
        boost::process::child c(boost::process::cmd = cmd_line, proc_grp_);
        c.detach();
    }

    std::shared_ptr< HomeObject > build_new_homeobject() {
        auto is_restart = is_current_testcase_restarted();
        prepare_devices(!is_restart);
        homeobj_ = init_homeobject(std::weak_ptr< TestReplApplication >(app));
        return homeobj_;
    }

    void delete_homeobject() {
        LOGINFO("Clearing Homeobject replica={}", replica_num_);
        homeobj_.reset();
        remove_test_files();
    }

    std::shared_ptr< homeobject::HomeObject > restart(uint32_t shutdown_delay_secs = 0u, uint32_t restart_delay_secs = 0u) {
        if (shutdown_delay_secs > 0) {
            std::this_thread::sleep_for(std::chrono::seconds(shutdown_delay_secs));
        }
        LOGINFO("Stoping homeobject after {} secs, replica={}", shutdown_delay_secs, replica_num_);
        homeobj_.reset();
        if (restart_delay_secs > 0) {
            std::this_thread::sleep_for(std::chrono::seconds(restart_delay_secs));
        }
        LOGINFO("Starting homeobject after {} secs, replica={}", restart_delay_secs, replica_num_);
        homeobj_ = init_homeobject(std::weak_ptr< TestReplApplication >(app));
        return homeobj_;
    }

    uint8_t replica_num() const { return replica_num_; }

    peer_id_t my_replica_id() const { return my_replica_id_; }

    peer_id_t replica_id(uint16_t member_id) const {
        auto it = std::find_if(members_.begin(), members_.end(),
                               [member_id](auto const& p) { return p.second == member_id; });
        if (it != members_.end()) { return it->first; }
        return boost::uuids::nil_uuid();
    }

    uint16_t member_id(peer_id_t replica_id) const {
        auto it = members_.find(replica_id);
        if (it != members_.end()) { return it->second; }
        return members_.size();
    }

    std::map< peer_id_t, uint32_t > const& members() const { return members_; }

    std::string name() const { return name_; }
    std::string test_name() const { return test_name_; }

    void teardown() { sisl::GrpcAsyncClientWorker::shutdown_all(); }

    void sync() {
        ipc_data_->sync(sync_point_num++, total_replicas_nums_);
    }

    // Bump sync point to avoid interference across different test cases
    void bump_sync_point_and_sync() {
        static constexpr uint64_t sync_point_gap_per_test = 1000;
        sync_point_num = (sync_point_num / sync_point_gap_per_test + 1) * sync_point_gap_per_test;
        sync();
    }

    void set_uint64_id(uint64_t uint64_id) { ipc_data_->set_uint64_id(uint64_id); }
    uint64_t get_uint64_id() { return ipc_data_->get_uint64_id(); }

    void set_auxiliary_uint64_id(uint64_t input_auxiliary_uint64_id) {
        ipc_data_->set_auxiliary_uint64_id(input_auxiliary_uint64_id);
    }

    uint64_t get_auxiliary_uint64_id() { return ipc_data_->get_auxiliary_uint64_id(); }

    void check_and_kill(int port) {
        std::string command = "lsof -t -i:" + std::to_string(port);
        if (::system(command.c_str())) {
            std::cout << "Port " << port << " is not in use." << std::endl;
        } else {
            std::cout << "Port " << port << " is in use. Trying to kill the process..." << std::endl;
            command += " | xargs kill -9";
            int result = ::system(command.c_str());
            if (result == 0) {
                std::cout << "Killed the process using port " << port << std::endl;
            } else {
                std::cout << "Failed to kill the process." << std::endl;
            }
        }
    }

    bool is_current_testcase_restarted() const {
        if (!SISL_OPTIONS["is_restart"].as< bool >()) { return false; }

        // check if the current testcase is the first one
        auto ut = testing::UnitTest::GetInstance();
        for (int i = 0; i < ut->total_test_suite_count(); i++) {
            auto ts = ut->GetTestSuite(i);
            if (!ts->should_run()) { continue; }
            for (int j = 0; j < ts->total_test_count(); j++) {
                auto ti = ts->GetTestInfo(j);
                if (!ti->should_run()) { continue; }
                if (ti != ut->current_test_info()) { return false; }
                return true;
            }
        }
        RELEASE_ASSERT(true, "Cannot find any available testcase");
        return false; // should not reach here
    }

private:
    void prepare_devices(bool init_device = true) {
        auto const use_file = SISL_OPTIONS["use_file"].as< bool >();
        auto const ndevices = SISL_OPTIONS["num_devs"].as< uint32_t >();
        auto const dev_size = SISL_OPTIONS["dev_size_mb"].as< uint64_t >() * 1024 * 1024;
        if (use_file && !dev_list_.empty()) LOGWARN("Ignoring device_list as use_file is set to true");
        if (!use_file && !dev_list_.empty()) {
            init_raw_devices(dev_list_);
        } else {
            for (uint32_t i{0}; i < ndevices; ++i) {
                generated_devs.emplace_back(std::string{"/tmp/" + name_ + "_" + std::to_string(i + 1)});
            }
            if (init_device) {
                LOGINFO("creating {} device files with each of size {} ", ndevices, homestore::in_bytes(dev_size));
                init_files(generated_devs, dev_size);
            }
        }
    }

    void remove_test_files() {
        for (auto const& dev : generated_devs) {
            if (std::filesystem::exists(dev)) std::filesystem::remove(dev);
        }
        generated_devs.clear();
    }

    void init_raw_devices(const std::vector< std::string >& devs) {
        // TODO: do not use 4096 and 0 directly, instead use the constants from homestore::hs_super_blk
        auto const zero_size = 4096 /*homestore::hs_super_blk::first_block_size()*/ * 1024;
        std::vector< int > zeros(zero_size, 0);
        for (auto const& path : devs) {
            if (!std::filesystem::exists(path)) { RELEASE_ASSERT(false, "Device {} does not exist", path); }

            auto fd = ::open(path.c_str(), O_RDWR, 0640);
            RELEASE_ASSERT(fd != -1, "Failed to open device");

            auto const write_sz =
                pwrite(fd, zeros.data(), zero_size /* size */, 0 /*homestore::hs_super_blk::first_block_offset())*/);
            RELEASE_ASSERT(write_sz == zero_size, "Failed to write to device");
            LOGINFO("Successfully zeroed the 1st {} bytes of device {}", zero_size, path);
            ::close(fd);
        }
    }

    void init_files(const std::vector< std::string >& file_paths, uint64_t dev_size) {
        for (const auto& fpath : file_paths) {
            if (std::filesystem::exists(fpath)) std::filesystem::remove(fpath);
            std::ofstream ofs{fpath, std::ios::binary | std::ios::out | std::ios::trunc};
            std::filesystem::resize_file(fpath, dev_size);
        }
    }

private:
    uint8_t replica_num_;
    uint8_t total_replicas_nums_;
    uint64_t sync_point_num{0};
    std::string name_;
    std::string test_name_;
    std::vector< std::string > args_;
    char** argv_;
    std::vector< std::string > generated_devs;
    std::vector< std::string > dev_list_;
    std::shared_ptr< homeobject::HomeObject > homeobj_;

    boost::process::group proc_grp_;
    std::unique_ptr< bip::shared_memory_object > shm_;
    std::unique_ptr< bip::mapped_region > region_;
    std::unique_ptr< folly::Init > folly_;
    std::map< peer_id_t, uint32_t > members_;
    peer_id_t my_replica_id_;
    IPCData* ipc_data_;

    std::shared_ptr< TestReplApplication > app;
};
} // namespace test_common