#include <gtest/gtest.h>
#include <sisl/logging/logging.h>
#include <sisl/options/options.h>

#include "homeobject/homeobject.hpp"

SISL_LOGGING_INIT(logging, homeobject)
SISL_OPTIONS_ENABLE(logging)

TEST(HomeObject, BasicAssert) {
    homeobject::foo();
    ASSERT_EQ(1,1);
}

int main(int argc, char* argv[]) {
    int parsed_argc = argc;
    ::testing::InitGoogleTest(&parsed_argc, argv);
    SISL_OPTIONS_LOAD(parsed_argc, argv, logging);
    sisl::logging::SetLogger("test_repl_state_machine");

    return RUN_ALL_TESTS();
}
