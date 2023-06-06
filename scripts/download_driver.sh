#!/bin/bash

DRIVER_NAME=$1
SAVE_TO=$2
PRIV_LIB_DIR=$3

ADBC_VERSION="0.4.0"
ADBC_DRIVER_BASE_URL="https://github.com/apache/arrow-adbc/releases/download/apache-arrow-adbc-${ADBC_VERSION}"
ADBC_DRIVER_MACOS_X86_64_URL="${ADBC_DRIVER_BASE_URL}/adbc_driver_${DRIVER_NAME}-${ADBC_VERSION}-py3-none-macosx_10_9_x86_64.whl"
ADBC_DRIVER_MACOS_ARM64_URL="${ADBC_DRIVER_BASE_URL}/adbc_driver_${DRIVER_NAME}-${ADBC_VERSION}-py3-none-macosx_11_0_arm64.whl"
ADBC_DRIVER_LINUX_X86_64_URL="${ADBC_DRIVER_BASE_URL}/adbc_driver_${DRIVER_NAME}-${ADBC_VERSION}-py3-none-manylinux_2_17_x86_64.manylinux2014_x86_64.whl"
ADBC_DRIVER_LINUX_AARCH64_URL="${ADBC_DRIVER_BASE_URL}/adbc_driver_${DRIVER_NAME}-${ADBC_VERSION}-py3-none-manylinux_2_17_aarch64.manylinux2014_aarch64.whl"

case "${DRIVER_NAME}" in
  sqlite)
    ;;
  postgresql)
    ;;
  flightsql)
    ;;
  snowflake)
    ;;
  *)
    echo "Driver '${DRIVER_NAME}' is not in the official precompiled driver list. Official drivers are sqlite, postgresql, flightsql and snowflake"
    exit 1
    ;;
esac

get_triplet() {
  if [[ -n "${TARGET_ARCH}" && -n "${TARGET_OS}" && -n "${TARGET_ABI}" ]]; then
    if [ "${TARGET_ARCH}" = "arm" ]; then
      case "${TARGET_CPU}" in
      arm1176jzf_s*)
        echo "armv6-${TARGET_OS}-${TARGET_ABI}"
      ;;
      cortex*)
        echo "armv7l-${TARGET_OS}-${TARGET_ABI}"
      ;;
      *)
        echo "Unknown TARGET_CPU: ${TARGET_CPU}"
        exit 1
      ;;
      esac
    else
      echo "${TARGET_ARCH}-${TARGET_OS}-${TARGET_ABI}"
    fi
  fi

  UNAME_M="$(uname -m)"
  if [ -n "${TARGET_ARCH}" ]; then
    UNAME_M="${TARGET_ARCH}"
    if [ "${TARGET_ARCH}" = "arm" ]; then
      case "${TARGET_CPU}" in
        arm1176jzf_s*)
          UNAME_M="armv6"
        ;;
        cortex*)
          UNAME_M="armv7l"
        ;;
        *)
          UNAME_M="${TARGET_ARCH}"
        ;;
      esac
    fi
  fi
  UNAME_S="$(uname -s)"
  if [ -n "${TARGET_OS}" ]; then
    UNAME_S="${TARGET_OS}"
  fi

  case "${UNAME_M}-${UNAME_S}" in
    arm64-Darwin*)
      echo "aarch64-apple-darwin"
    ;;
    x86_64-Darwin*)
      echo "x86_64-apple-darwin"
    ;;
    *-Linux*)
      # check libc type
      ABI="gnu"

      if [ -n "${TARGET_ABI}" ]; then
        ABI="${TARGET_ABI}"
      else
        if [ -x "$(which ldd)" ]; then
          ldd --version | grep musl >/dev/null ;
          if [ $? -eq 0 ]; then
            ABI="musl"
          fi
        fi

        case "${UNAME_M}" in
          armv6*|armv7*)
            ABI="${ABI}eabihf"
          ;;
        esac
      fi

      echo "${UNAME_M}-linux-${ABI}"
    ;;
  esac
}

DRIVER_TRIPLET="$(get_triplet)"
echo "DRIVER_TRIPLET: ${DRIVER_TRIPLET}"

get_download_url() {
  triplet=$1
  if [ "${triplet}" == "aarch64-apple-darwin" ]; then
    echo "${ADBC_DRIVER_MACOS_ARM64_URL}"
  elif [ "${triplet}" == "x86_64-apple-darwin" ]; then
    echo "${ADBC_DRIVER_MACOS_X86_64_URL}"
  elif [ "${triplet}" == "aarch64-linux-gnu" ]; then
    echo "${ADBC_DRIVER_LINUX_AARCH64_URL}"
  elif [ "${triplet}" == "x86_64-linux-gnu" ]; then
    echo "${ADBC_DRIVER_LINUX_X86_64_URL}"
  else
    echo "No official precompiled ${DRIVER_NAME} driver for ${triplet}"
    exit 0
  fi
}

DRIVER_URL="$(get_download_url ${DRIVER_TRIPLET})"
echo "DRIVER_URL: ${DRIVER_URL}"

SAVE_AS="${SAVE_TO}/${DRIVER_NAME}-${ADBC_VERSION}-${DRIVER_TRIPLET}.zip"
UNARCHIVE_DIR="${SAVE_TO}/${DRIVER_NAME}-${ADBC_VERSION}-${DRIVER_TRIPLET}"
DST_FILENAME="${PRIV_LIB_DIR}/libadbc_driver_${DRIVER_NAME}"
case "${DRIVER_TRIPLET}" in
  *"-apple-darwin")
    DST_FILENAME="${DST_FILENAME}.dylib"
    ;;
  *)
    DST_FILENAME="${DST_FILENAME}.so"
    ;;
esac

download_driver() {
  url=$1
  save_as=$2
  if [ ! -e "${save_as}" ]; then
    echo "Downloading driver from ${url}..."
    if [ -e "$(which curl)" ]; then
      curl -fSL "${url}" -o "${save_as}"
    elif [ -e "$(which wget)" ]; then
      wget "${url}" -O "${save_as}"
    else
      echo "cannot find curl or wget, cannot download driver"
      exit 1
    fi
  fi
}

unarchive_driver() {
  unarchive_dir=$1
  archive_file=$2
  if [ ! -d "${unarchive_dir}" ]; then
    echo "Unarchiving driver..."
    rm -rf "${unarchive_dir}" &&
    mkdir -p "${unarchive_dir}" &&
    cd "${unarchive_dir}" &&
    unzip "${archive_file}"
  fi
}

copy_if_not_exists() {
  src="$1"
  dst="$2"
  if [ ! -e "$dst" ]; then
    cp -a "${src}" "${dst}"
  fi
}

if [ ! -e "${DST_FILENAME}" ]; then
  if [ ! -d "${UNARCHIVE_DIR}" ]; then
    if [ ! -e "${SAVE_AS}" ]; then
      mkdir -p "${SAVE_TO}" && \
      download_driver "${DRIVER_URL}" "${SAVE_AS}"
    fi
    unarchive_driver "${UNARCHIVE_DIR}" "${SAVE_AS}"
  fi
  mkdir -p "${PRIV_LIB_DIR}" && \
  copy_if_not_exists "${UNARCHIVE_DIR}/adbc_driver_${DRIVER_NAME}/libadbc_driver_${DRIVER_NAME}.so" "${DST_FILENAME}"
fi
