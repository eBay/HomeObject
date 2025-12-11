from conan import ConanFile
from conan.errors import ConanInvalidConfiguration
from conan.tools.build import check_min_cppstd
from conan.tools.cmake import CMakeToolchain, CMakeDeps, CMake
from conan.tools.files import copy
from os.path import join

required_conan_version = ">=1.60.0"


class HomeObjectConan(ConanFile):
    name = "homeobject"
    version = "4.0.9"

    homepage = "https://github.com/eBay/HomeObject"
    description = "Blob Store built on HomeStore"
    topics = ("ebay")
    url = "https://github.com/eBay/HomeObject"
    license = "Apache-2.0"

    settings = "arch", "os", "compiler", "build_type"

    options = {
        "shared": ['True', 'False'],
        "fPIC": ['True', 'False'],
        "coverage": ['True', 'False'],
        "sanitize": ['True', 'False'],
    }
    default_options = {
        'shared': False,
        'fPIC': True,
        'coverage': False,
        'sanitize': False,
    }

    exports_sources = ("CMakeLists.txt", "cmake/*", "src/*", "LICENSE")

    def configure(self):
        if self.options.shared:
            self.options.rm_safe("fPIC")
        if self.settings.build_type == "Debug":
            if self.options.coverage and self.options.sanitize:
                raise ConanInvalidConfiguration("Sanitizer does not work with Code Coverage!")
            if self.conf.get("tools.build:skip_test", default=False):
                if self.options.coverage or self.options.sanitize:
                    raise ConanInvalidConfiguration("Coverage/Sanitizer requires Testing!")

    def build_requirements(self):
        self.test_requires("gtest/1.17.0")

    def requirements(self):
        self.requires("sisl/[^13.0]@oss/master", transitive_headers=True)
        self.requires("homestore/[^7.1]@oss/master")
        self.requires("iomgr/[^12.0]@oss/master")

    def validate(self):
        if self.info.settings.compiler.cppstd:
            check_min_cppstd(self, 20)

    def layout(self):
        self.folders.source = "."
        if self.options.get_safe("sanitize"):
            self.folders.build = join("build", "Sanitized")
        elif self.options.get_safe("coverage"):
            self.folders.build = join("build", "Coverage")
        else:
            self.folders.build = join("build", str(self.settings.build_type))
        self.folders.generators = join(self.folders.build, "generators")

        self.cpp.source.components["homestore"].includedirs = ["src/include"]
        self.cpp.source.components["memory"].includedirs = ["src/include"]

        self.cpp.build.components["homestore"].libdirs = ["src/lib/homestore_backend"]
        self.cpp.build.components["memory"].libdirs = ["src/lib/memory_backend"]

        self.cpp.package.components["homestore"].libs = ["homeobject_homestore"]
        self.cpp.package.components["memory"].libs = ["homeobject_memory"]
        self.cpp.package.includedirs = ["include"]  # includedirs is already set to 'include' by
        self.cpp.package.libdirs = ["lib"]

    def generate(self):
        # This generates "conan_toolchain.cmake" in self.generators_folder
        tc = CMakeToolchain(self)
        tc.variables["CONAN_CMAKE_SILENT_OUTPUT"] = "ON"
        tc.variables['CMAKE_EXPORT_COMPILE_COMMANDS'] = 'ON'
        tc.variables["CTEST_OUTPUT_ON_FAILURE"] = "ON"
        tc.variables["MEMORY_SANITIZER_ON"] = "OFF"
        tc.variables["CODE_COVERAGE"] = "OFF"
        tc.variables["CONAN_PACKAGE_NAME"] = self.name
        tc.variables["CONAN_PACKAGE_VERSION"] = self.version
        if self.settings.build_type == "Debug":
            if self.options.get_safe("coverage"):
                tc.variables['CODE_COVERAGE'] = 'ON'
            elif self.options.get_safe("sanitize"):
                tc.variables['MEMORY_SANITIZER_ON'] = 'ON'
        tc.generate()

        # This generates "boost-config.cmake" and "grpc-config.cmake" etc in self.generators_folder
        deps = CMakeDeps(self)
        deps.generate()

    def build(self):
        cmake = CMake(self)
        cmake.configure()
        cmake.build()
        if not self.conf.get("tools.build:skip_test", default=False):
            cmake.test()

    def package(self):
        lib_dir = join(self.package_folder, "lib")
        copy(self, "LICENSE", self.source_folder, join(self.package_folder, "licenses"), keep_path=False)
        copy(self, "*.lib", self.build_folder, lib_dir, keep_path=False)
        copy(self, "*.a", self.build_folder, lib_dir, keep_path=False)
        copy(self, "*.dylib*", self.build_folder, lib_dir, keep_path=False)
        copy(self, "*.dll*", self.build_folder, join(self.package_folder, "bin"), keep_path=False)
        copy(self, "*.so*", self.build_folder, lib_dir, keep_path=False)
        copy(self, "*", join(self.source_folder, "src", "flip", "client", "python"),
             join(self.package_folder, "bindings", "flip", "python"), keep_path=False)

        copy(self, "*.h*", join(self.source_folder, "src", "include"), join(self.package_folder, "include"),
             keep_path=True)

    def package_info(self):
        self.cpp_info.components["homestore"].requires = ["homestore::homestore", "iomgr::iomgr", "sisl::sisl"]
        self.cpp_info.components["memory"].requires = ["sisl::sisl"]
        self.cpp_info.components["homeobject"].requires = ["homestore"]

        if self.settings.os == "Linux":
            self.cpp_info.components["homestore"].system_libs.append("pthread")
            self.cpp_info.components["memory"].system_libs.append("pthread")
        if self.options.sanitize:
            self.cpp_info.components["memory"].sharedlinkflags.append("-fsanitize=address")
            self.cpp_info.components["memory"].exelinkflags.append("-fsanitize=address")
            self.cpp_info.components["memory"].sharedlinkflags.append("-fsanitize=undefined")
            self.cpp_info.components["memory"].exelinkflags.append("-fsanitize=undefined")

        self.cpp_info.names["cmake_find_package"] = "HomeObject"
        self.cpp_info.names["cmake_find_package_multi"] = "HomeObject"
