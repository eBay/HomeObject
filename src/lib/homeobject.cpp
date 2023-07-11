#include "homeobject/homeobject.hpp"

#include <sisl/logging/logging.h>

SISL_LOGGING_DECL(homeobject);

namespace homeobject {

void foo() {
    LOGINFOMOD(homeobject, "Test");
}

}
