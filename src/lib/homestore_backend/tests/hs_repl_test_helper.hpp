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

#include <folly/init/Init.h>

#include "homeobject/common.hpp"

namespace bip = boost::interprocess;
using namespace homeobject;

#define INVALID_UINT64_ID UINT64_MAX

namespace test_common {

class HSReplTestHelper {
protected:
    struct IPCData {
        void sync(uint64_t sync_point, uint32_t max_count = 0) {
            if (max_count == 0) { max_count = SISL_OPTIONS["replicas"].as< uint8_t >(); }
            std::unique_lock< bip::interprocess_mutex > lg(mtx_);
            ++homeobject_replica_count_;
            if (homeobject_replica_count_ == max_count) {
                sync_point_num_ = sync_point;
                homeobject_replica_count_ = 0;
                uint64_id_ = INVALID_UINT64_ID;
                cv_.notify_all();
            } else {
                cv_.wait(lg, [this, sync_point]() { return sync_point_num_ == sync_point; });
            }
        }

        void set_uint64_id(uint64_t input_uint64_id) {
            std::unique_lock< bip::interprocess_mutex > lg(mtx_);
            uint64_id_ = input_uint64_id;
        }

        uint64_t get_uint64_id() {
            std::unique_lock< bip::interprocess_mutex > lg(mtx_);
            return uint64_id_;
        }

    private:
        bip::interprocess_mutex mtx_;
        bip::interprocess_condition cv_;
        uint8_t homeobject_replica_count_{0};

        // the following variables are used to share shard_id and blob_id among different replicas
        uint64_t uint64_id_{0};

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
    };

public:
    friend class TestReplApplication;

    HSReplTestHelper(std::string const& name, std::vector< std::string > const& args, char** argv) :
            name_{name}, args_{args}, argv_{argv} {}

    void setup(uint32_t num_replicas) {
        num_replicas_ = num_replicas;
        replica_num_ = SISL_OPTIONS["replica_num"].as< uint16_t >();

        sisl::logging::SetLogger(name_ + std::string("_replica_") + std::to_string(replica_num_));
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
        name_ += std::to_string(replica_num_);

        // prepare_devices();

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
                LOGINFO("Spawning Homeobject replica={} instance", i);

                std::string cmd_line;
                fmt::format_to(std::back_inserter(cmd_line), "{} --replica_num {}", args_[0], i);
                for (int j{1}; j < (int)args_.size(); ++j) {
                    fmt::format_to(std::back_inserter(cmd_line), " {}", args_[j]);
                }
                boost::process::child c(boost::process::cmd = cmd_line, proc_grp_);
                c.detach();
            }
        } else {
            shm_ = std::make_unique< bip::shared_memory_object >(bip::open_only, "HO_repl_test_shmem", bip::read_write);
            region_ = std::make_unique< bip::mapped_region >(*shm_, bip::read_write);
            ipc_data_ = static_cast< IPCData* >(region_->get_address());
        }

        int tmp_argc = 1;
        folly_ = std::make_unique< folly::Init >(&tmp_argc, &argv_, true);

        LOGINFO("Starting HomeObject replica={}", replica_num_);
        app = std::make_shared< TestReplApplication >(*this);
    }

    std::shared_ptr< homeobject::HomeObject > build_new_homeobject() {
        prepare_devices();
        homeobj_ = init_homeobject(std::weak_ptr< TestReplApplication >(app));
        return homeobj_;
    }

    void delete_homeobject() {
        LOGINFO("Clearing Homeobject replica={}", replica_num_);
        homeobj_.reset();
        remove_test_files();
    }

    std::shared_ptr< homeobject::HomeObject > restart(uint32_t shutdown_delay_secs = 5u) {
        LOGINFO("Restarting homeobject replica={}", replica_num_);
        homeobj_.reset();
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

    void teardown() { sisl::GrpcAsyncClientWorker::shutdown_all(); }

    void sync(uint32_t num_members = 0) { ipc_data_->sync(sync_point_num++, num_members); }
    void set_uint64_id(uint64_t uint64_id) { ipc_data_->set_uint64_id(uint64_id); }
    uint64_t get_uint64_id() { return ipc_data_->get_uint64_id(); }

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
    uint64_t sync_point_num{0};
    std::string name_;
    std::vector< std::string > args_;
    char** argv_;
    uint32_t num_replicas_;
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