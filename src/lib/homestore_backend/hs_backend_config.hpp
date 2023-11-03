#pragma once

#include "common/generated/hs_backend_config_generated.h"

SETTINGS_INIT(homeobjectcfg::HSBackendSettings, hs_backend_config);

// DM info size depends on these three parameters. If below parameter changes then we have to add
// the code for upgrade/revert.

namespace homeobject {
#define HS_BACKEND_DYNAMIC_CONFIG_WITH(...) SETTINGS(hs_backend_config, __VA_ARGS__)
#define HS_BACKEND_DYNAMIC_CONFIG_THIS(...) SETTINGS_THIS(hs_backend_config, __VA_ARGS__)
#define HS_BACKEND_DYNAMIC_CONFIG(...) SETTINGS_VALUE(hs_backend_config, __VA_ARGS__)

#define HS_BACKEND_SETTINGS_FACTORY() SETTINGS_FACTORY(hs_backend_config)

} // namespace homeobject
