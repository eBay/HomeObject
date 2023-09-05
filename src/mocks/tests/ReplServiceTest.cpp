#include <chrono>
#include <string>

#include <gtest/gtest.h>
#include <gmock/gmock-matchers.h>
#include <folly/executors/QueuedImmediateExecutor.h>

#include <sisl/logging/logging.h>
#include <sisl/options/options.h>
#include <variant>

#include "mocks/repl_service.h"

using namespace std::chrono_literals;
using home_replication::ReplServiceError;

SISL_LOGGING_INIT(logging, HOMEREPL_LOG_MODS)
SISL_OPTIONS_ENABLE(logging)

class ReplServiceFixture : public ::testing::Test {
public:
    void SetUp() override {
        m_mock_svc = home_replication::create_repl_service([](auto&) { return nullptr; });

        auto members = std::set< std::string, std::less<> >();
        members.insert("ourself");
        auto v = m_mock_svc->create_replica_set("test_fixture", std::move(members))
                     .via(&folly::QueuedImmediateExecutor::instance())
                     .get();
        ASSERT_TRUE(std::holds_alternative< home_replication::rs_ptr_t >(v));
        m_repl_set = std::get< home_replication::rs_ptr_t >(v);
        ASSERT_TRUE(!!m_repl_set);
    }

protected:
    std::shared_ptr< home_replication::ReplicationService > m_mock_svc;
    home_replication::rs_ptr_t m_repl_set;
};

TEST_F(ReplServiceFixture, CreateEmptyReplSet) {
    auto v = m_mock_svc->create_replica_set("0", std::set< std::string, std::less<> >())
                 .via(&folly::QueuedImmediateExecutor::instance())
                 .get();
    ASSERT_TRUE(std::holds_alternative< ReplServiceError >(v));
    EXPECT_EQ(std::get< ReplServiceError >(v), ReplServiceError::BAD_REQUEST);
}

TEST_F(ReplServiceFixture, CreateDuplicateReplSet) {
    auto members = std::set< std::string, std::less<> >();
    members.insert("them");
    auto v = m_mock_svc->create_replica_set("test_fixture", std::move(members))
                 .via(&folly::QueuedImmediateExecutor::instance())
                 .get();
    ASSERT_TRUE(std::holds_alternative< ReplServiceError >(v));
    EXPECT_EQ(std::get< ReplServiceError >(v), ReplServiceError::SERVER_ALREADY_EXISTS);
}

int main(int argc, char* argv[]) {
    int parsed_argc = argc;
    ::testing::InitGoogleTest(&parsed_argc, argv);
    SISL_OPTIONS_LOAD(parsed_argc, argv, logging);
    sisl::logging::SetLogger(std::string(argv[0]));
    return RUN_ALL_TESTS();
}
