.PHONY: clean clean_all

PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

EXTENSION_NAME=polyglot

# Set to 1 to enable Unstable API (binaries will only work on TARGET_DUCKDB_VERSION, forwards compatibility will be broken)
# Note: currently extension-template-rs requires this, as duckdb-rs relies on unstable C API functionality
USE_UNSTABLE_C_API=1

# Target DuckDB version
TARGET_DUCKDB_VERSION=v1.4.4

all: configure debug

# Include makefiles from DuckDB
include extension-ci-tools/makefiles/c_api_extensions/base.Makefile
include extension-ci-tools/makefiles/c_api_extensions/rust.Makefile

# Enable C++ exceptions for WASM builds (required by DuckDB's bundled C++ code).
# cc-rs sets -fno-exceptions by default for emscripten; -fexceptions overrides that.
# wasm_eh and wasm_threads additionally use the native WASM exception handling mechanism.
ifneq ($(DUCKDB_WASM_PLATFORM),)
    WASM_EXCEPTION_FLAGS := -fexceptions
    ifneq ($(filter wasm_eh wasm_threads,$(DUCKDB_WASM_PLATFORM)),)
        WASM_EXCEPTION_FLAGS += -fwasm-exceptions
    endif
    export CXXFLAGS_wasm32_unknown_emscripten := $(WASM_EXCEPTION_FLAGS)

endif

# Override emcc link targets to pass exception flags (base.Makefile omits them for Rust extensions)
ifneq ($(DUCKDB_WASM_PLATFORM),)
link_wasm_debug:
	emcc $(EXTENSION_BUILD_PATH)/debug/$(EXTENSION_LIB_FILENAME) -o $(EXTENSION_BUILD_PATH)/debug/$(EXTENSION_FILENAME_NO_METADATA) -O3 -g -sSIDE_MODULE=2 $(WASM_EXCEPTION_FLAGS) -sEXPORTED_FUNCTIONS="_$(EXTENSION_NAME)_init_c_api"

link_wasm_release:
	emcc $(EXTENSION_BUILD_PATH)/release/$(EXTENSION_LIB_FILENAME) -o $(EXTENSION_BUILD_PATH)/release/$(EXTENSION_FILENAME_NO_METADATA) -O3 -sSIDE_MODULE=2 $(WASM_EXCEPTION_FLAGS) -sEXPORTED_FUNCTIONS="_$(EXTENSION_NAME)_init_c_api"
endif

configure: venv platform extension_version

debug: build_extension_library_debug build_extension_with_metadata_debug
release: build_extension_library_release build_extension_with_metadata_release

test: test_debug
test_debug: test_extension_debug
test_release: test_extension_release

clean: clean_build clean_rust
clean_all: clean_configure clean
