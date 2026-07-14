#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 2 ]; then
  echo "usage: $0 FRESH_ROOT REAL_SPACK" >&2
  exit 64
fi

readonly fresh_root="$1"
readonly real_spack="$2"

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

require_absolute_path "fresh Spack root" "$fresh_root"
require_absolute_path "real Spack executable" "$real_spack"
if [ ! -x "$real_spack" ]; then
  echo "Spack executable is absent: $real_spack" >&2
  exit 66
fi
if [ -e "$fresh_root" ]; then
  echo "refusing to reuse disposable Spack root: $fresh_root" >&2
  exit 73
fi

umask 077
install -d -m 700 \
  "$fresh_root/bin" \
  "$fresh_root/config" \
  "$fresh_root/overrides" \
  "$fresh_root/cache" \
  "$fresh_root/source-cache" \
  "$fresh_root/misc-cache" \
  "$fresh_root/stage" \
  "$fresh_root/store"

cat >"$fresh_root/overrides/config.yaml" <<EOF
config:
  install_tree:
    root: $fresh_root/store
  build_stage:
  - $fresh_root/stage
  source_cache: $fresh_root/source-cache
  misc_cache: $fresh_root/misc-cache
EOF
cat >"$fresh_root/overrides/concretizer.yaml" <<'EOF'
concretizer:
  reuse: false
EOF
cat >"$fresh_root/overrides/mirrors.yaml" <<'EOF'
mirrors:: {}
EOF
cat >"$fresh_root/overrides/upstreams.yaml" <<'EOF'
upstreams:: {}
EOF
printf '%s\n' "$real_spack" >"$fresh_root/real-spack"

cat >"$fresh_root/bin/spack" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
readonly root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
readonly real_spack="$(cat -- "$root/real-spack")"
export SPACK_USER_CONFIG_PATH="$root/config"
export SPACK_USER_CACHE_PATH="$root/cache"
exec "$real_spack" -C "$root/overrides" "$@"
EOF
chmod 700 "$fresh_root/bin/spack"

"$fresh_root/bin/spack" compiler find --scope=user
"$fresh_root/bin/spack" config get config >/dev/null
"$fresh_root/bin/spack" config get concretizer >/dev/null
test -x "$fresh_root/bin/spack"

if find "$fresh_root/bin" "$fresh_root/config" "$fresh_root/overrides" \
  -type l -print -quit | grep -q .; then
  echo "fresh Spack configuration contains a symbolic link" >&2
  exit 73
fi
(
  cd -- "$fresh_root"
  mapfile -d '' configuration_files < <(
    find bin config overrides real-spack -type f -print0 | LC_ALL=C sort -z
  )
  if [ "${#configuration_files[@]}" -lt 7 ]; then
    echo "fresh Spack configuration manifest is incomplete" >&2
    exit 73
  fi
  sha256sum -- "${configuration_files[@]}" >acceptance-manifest.sha256
  sha256sum --check --strict acceptance-manifest.sha256 >/dev/null
)
chmod 600 "$fresh_root/acceptance-manifest.sha256"
sha256sum "$fresh_root/acceptance-manifest.sha256"
