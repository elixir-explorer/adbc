ifndef MIX_APP_PATH
	MIX_APP_PATH=$(shell pwd)
endif

PRIV_DIR = $(MIX_APP_PATH)/priv
NIF_SO = $(PRIV_DIR)/adbc_nif.so
NIF_SO_REL = $(NIF_SO:$(shell pwd)/%=%)
ADBC_SRC = $(shell pwd)/3rd_party/apache-arrow-adbc
ADBC_C_SRC = $(shell pwd)/3rd_party/apache-arrow-adbc/c
UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Linux)
	ADBC_DRIVER_COMMON_LIB = $(PRIV_DIR)/lib/libadbc_driver_manager.so
endif
ifeq ($(UNAME_S),Darwin)
	ADBC_DRIVER_COMMON_LIB = $(PRIV_DIR)/lib/libadbc_driver_manager.dylib
endif

C_SRC = $(shell pwd)/c_src
C_SRC_REL = c_src
ifdef CMAKE_TOOLCHAIN_FILE
	CMAKE_CONFIGURE_FLAGS=-D CMAKE_TOOLCHAIN_FILE="$(CMAKE_TOOLCHAIN_FILE)"
endif

CMAKE_BUILD_TYPE ?= Release
DEFAULT_JOBS ?= 1
CMAKE_ADBC_BUILD_DIR = $(MIX_APP_PATH)/cmake_adbc
CMAKE_ADBC_OPTIONS ?= ""
CMAKE_ADBC_NIF_BUILD_DIR = $(MIX_APP_PATH)/cmake_adbc_nif
CMAKE_ADBC_NIF_OPTIONS ?= ""
MAKE_BUILD_FLAGS ?= -j$(DEFAULT_JOBS)

.DEFAULT_GLOBAL := build

build: $(NIF_SO_REL)
	@echo > /dev/null

priv_dir:
	@ if [ ! -e "$(PRIV_DIR)" ]; then \
 		mkdir -p "$(PRIV_DIR)" ; \
 	fi

adbc: priv_dir
	@ if [ ! -f "$(ADBC_DRIVER_COMMON_LIB)" ]; then \
		mkdir -p "$(CMAKE_ADBC_BUILD_DIR)" && \
		cd "$(CMAKE_ADBC_BUILD_DIR)" && \
		cmake --no-warn-unused-cli \
			-DADBC_BUILD_SHARED="ON" \
			-DADBC_DRIVER_MANAGER="ON" \
			-DADBC_DRIVER_POSTGRESQL="OFF" \
			-DADBC_DRIVER_SQLITE="ON" \
			-DADBC_DRIVER_FLIGHTSQL="OFF" \
			-DADBC_DRIVER_SNOWFLAKE="OFF" \
			-DADBC_BUILD_STATIC="OFF" \
			-DADBC_BUILD_TESTS="OFF" \
			-DADBC_USE_ASAN="OFF" \
			-DADBC_USE_UBSAN="OFF" \
			-DCMAKE_BUILD_TYPE="$(CMAKE_BUILD_TYPE)" \
			-DCMAKE_INSTALL_PREFIX="$(PRIV_DIR)" \
			-DADBC_DEPENDENCY_SOURCE=BUNDLED \
			$(CMAKE_CONFIGURE_FLAGS) $(CMAKE_ADBC_OPTIONS) "$(ADBC_C_SRC)" && \
    	cmake --build . --target install -j ; \
	fi

$(NIF_SO_REL): priv_dir adbc $(C_SRC_REL)/adbc_nif_resource.hpp $(C_SRC_REL)/adbc_nif.cpp $(C_SRC_REL)/nif_utils.hpp $(C_SRC_REL)/nif_utils.cpp
	@ mkdir -p "$(CMAKE_ADBC_NIF_BUILD_DIR)" && \
	cd "$(CMAKE_ADBC_NIF_BUILD_DIR)" && \
	cmake --no-warn-unused-cli \
		-D CMAKE_BUILD_TYPE="$(CMAKE_BUILD_TYPE)" \
		-D C_SRC="$(C_SRC)" \
		-D ADBC_SRC="$(ADBC_SRC)" \
		-D MIX_APP_PATH="$(MIX_APP_PATH)" \
		-D PRIV_DIR="$(PRIV_DIR)" \
		-D ERTS_INCLUDE_DIR="$(ERTS_INCLUDE_DIR)" \
		$(CMAKE_CONFIGURE_FLAGS) $(CMAKE_ADBC_NIF_OPTIONS) "$(shell pwd)" && \
	make "$(MAKE_BUILD_FLAGS)" && \
	cp "$(CMAKE_ADBC_NIF_BUILD_DIR)/adbc_nif.so" "$(NIF_SO)"
