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
#pragma once

// sisl v14 COMPATIBILITY SHIM (force-included for all homeobject TUs; see top-level CMakeLists.txt).
//
// sisl removed its 15 cast-shorthand macros from <sisl/fds/utils.hpp> ("expand all call sites to the
// real C++ cast keyword"). homeobject still uses them across ~600 sites, so this restores them verbatim
// to keep homeobject compiling against sisl v14 while it is being modernized. Delete this header (and the
// force-include) once homeobject's call sites are expanded to the real cast keywords like sisl did.

#include <cstdint>
#include <cstddef>
#include <memory>

#ifndef r_cast
#define r_cast reinterpret_cast
#define s_cast static_cast
#define d_cast dynamic_cast
#define dp_cast std::dynamic_pointer_cast
#define sp_cast std::static_pointer_cast
#define uintptr_cast reinterpret_cast< uint8_t* >
#define voidptr_cast reinterpret_cast< void* >
#define c_voidptr_cast reinterpret_cast< const void* >
#define charptr_cast reinterpret_cast< char* >
#define c_charptr_cast reinterpret_cast< const char* >
#define int_cast static_cast< int >
#define uint32_cast static_cast< uint32_t >
#define int64_cast static_cast< int64_t >
#define uint64_cast static_cast< uint64_t >
#define size_cast static_cast< size_t >
#endif

// sisl v14 keeps Clock and the get_elapsed_time_* helpers in namespace sisl, but homeobject uses them
// bare across ~100 sites (they used to arrive into scope transitively). Pull them to global scope here
// (this header is force-included after the std/cast section, so the symbols resolve). Same interim
// nature as the cast macros -- drop once homeobject qualifies these as sisl::.
#include <sisl/fds/utils.hpp>
using sisl::Clock; // Clock is in namespace sisl; get_elapsed_time_* are global (no using needed).
