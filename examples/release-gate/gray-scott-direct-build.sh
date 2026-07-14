#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 7 ]; then
  echo "usage: $0 SPACK SOURCE_ROOT BUILD_ROOT INSTALL_ROOT CORE_COMMIT GRAY_TREE ADIOS_HASH" >&2
  exit 64
fi

readonly spack="$1"
readonly source_root="$2"
readonly build_root="$3"
readonly install_root="$4"
readonly core_commit="$5"
readonly gray_tree="$6"
readonly adios_hash="$7"

require_absolute_path() {
  local label="$1"
  local value="$2"
  if [[ ! "$value" =~ ^/[A-Za-z0-9._+/-]+$ ]] || [[ "$value" == *//* ]] ||
    [[ "$value" == */ ]] || [[ "$value" == *"/./"* ]] ||
    [[ "$value" == */. ]] || [[ "/$value/" == *"/../"* ]]; then
    echo "$label is not a canonical absolute path" >&2
    exit 64
  fi
}

require_sha1() {
  local label="$1"
  local value="$2"
  if [[ ! "$value" =~ ^[0-9a-f]{40}$ ]]; then
    echo "$label is not a full lowercase Git object id" >&2
    exit 64
  fi
}

require_absolute_path "Spack executable" "$spack"
require_absolute_path "source root" "$source_root"
require_absolute_path "build root" "$build_root"
require_absolute_path "install root" "$install_root"
require_sha1 "clio-core commit" "$core_commit"
require_sha1 "Gray-Scott tree" "$gray_tree"
if [[ ! "$adios_hash" =~ ^[a-z0-9]{32}$ ]]; then
  echo "ADIOS2 DAG hash is not canonical" >&2
  exit 64
fi
if [ ! -x "$spack" ]; then
  echo "Spack executable is absent: $spack" >&2
  exit 66
fi

adios_prefix=""
if ! adios_prefix="$("$spack" location -i "/$adios_hash")"; then
  echo "selected ADIOS2 prefix lookup failed" >&2
  exit 66
fi
readonly adios_prefix
require_absolute_path "ADIOS2 prefix" "$adios_prefix"
if [ ! -d "$adios_prefix" ]; then
  echo "ADIOS2 prefix is absent: $adios_prefix" >&2
  exit 66
fi

declare -a adios_configs=()
mapfile -d '' -t adios_configs < <(
  find "$adios_prefix" -type f \
    \( -name ADIOS2Config.cmake -o -name adios2-config.cmake \) -print0
)
readonly find_pid="$!"
if ! wait "$find_pid"; then
  echo "ADIOS2 CMake package discovery failed" >&2
  exit 74
fi
if [ "${#adios_configs[@]}" -ne 1 ]; then
  echo "selected ADIOS2 prefix must contain exactly one CMake package config" >&2
  exit 65
fi
readonly adios2_dir="$(dirname -- "${adios_configs[0]}")"
require_absolute_path "ADIOS2 CMake package directory" "$adios2_dir"
if [[ "$adios2_dir/" != "$adios_prefix/"* ]]; then
  echo "ADIOS2 CMake package directory escaped the selected prefix" >&2
  exit 65
fi

for path in "$source_root" "$build_root" "$install_root"; do
  if [ -e "$path" ]; then
    echo "refusing to reuse acceptance path: $path" >&2
    exit 73
  fi
done

umask 077
git clone --filter=blob:none --no-checkout https://github.com/iowarp/clio-core.git \
  "$source_root"
git -C "$source_root" fetch --depth=1 origin "$core_commit"
git -C "$source_root" -c advice.detachedHead=false checkout --detach "$core_commit"
test "$(git -C "$source_root" rev-parse HEAD)" = "$core_commit"
test "$(git -C "$source_root" rev-parse HEAD:external/iowarp-gray-scott)" = \
  "$gray_tree"

"$spack" build-env "/$adios_hash" -- \
  cmake -S "$source_root/external/iowarp-gray-scott" -B "$build_root" \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX="$install_root" \
    -DADIOS2_DIR="$adios2_dir" \
    -DENABLE_RPATH=ON \
    -DENABLE_VTK=OFF
"$spack" build-env "/$adios_hash" -- cmake --build "$build_root" --parallel 4
"$spack" build-env "/$adios_hash" -- cmake --install "$build_root"
test -x "$install_root/bin/gray-scott"
