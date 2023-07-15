#
# CMake Toolchain file for cross-compiling for i686-linux-gnu.
#
set(CMAKE_SYSTEM_NAME Linux)
set(CMAKE_SYSTEM_PROCESSOR x86_64)

set(CMAKE_C_COMPILER "/usr/bin/x86_64-linux-gnu-gcc")
set(CMAKE_CXX_COMPILER "/usr/bin/x86_64-linux-gnu-g++")

set(CMAKE_FIND_ROOT_PATH /usr/x86_64-linux-gnu)

set(CMAKE_FIND_ROOT_PATH_MODE_INCLUDE ONLY)
set(CMAKE_FIND_ROOT_PATH_MODE_LIBRARY ONLY)

set(CMAKE_FIND_ROOT_PATH_MODE_PROGRAM NEVER)
