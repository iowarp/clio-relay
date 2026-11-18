#!/bin/bash
# clio-relay's launcher for Tapis (RL47).
#
# Tapis's ZIP runtime unpacks this archive in the job's directory and runs
# this script with the job's app arguments. Relay passes one argument: a
# base64 record of NUL-terminated fields, so no shell quoting is involved.
#
#   version, mode, directory, stdin, stdout, stderr, content dir, package dir,
#   input count, then (name, sha256) per input, then
#   for mode exec:    executable, arguments...
#   for mode package: name, version, sha256, key=value parameters...
#
# The launcher writes the job's exit code to relay.exitcode in the job's
# directory and exits with it.

set -u

job_dir="${_tapisExecSystemOutputDir:-$PWD}"
mkdir -p "$job_dir"

finish() {
    printf '%s\n' "$1" > "$job_dir/relay.exitcode"
    exit "$1"
}

fail() {
    printf 'clio-relay launcher: %s\n' "$2" >&2
    printf 'clio-relay launcher: %s\n' "$2" >> "$job_dir/relay.launcher.log"
    finish "$1"
}

[ "$#" -ge 1 ] || fail 64 "no job record was passed"
mapfile -d '' -t f < <(printf '%s' "$1" | base64 -d) || fail 64 "the job record is not base64"
[ "${f[0]:-}" = "1" ] || fail 64 "unknown job record version ${f[0]:-none}"

mode="${f[1]}"
work="${f[2]:-$job_dir}"
stdin_path="${f[3]:-/dev/null}"
stdout_path="${f[4]:-$job_dir/stdout}"
stderr_path="${f[5]:-$job_dir/stderr}"
content_dir="${f[6]}"
package_root="${f[7]}"
count="${f[8]}"
i=9

mkdir -p "$work" || fail 66 "cannot make the directory $work"
for ((n = 0; n < count; n++)); do
    name="${f[i]}"
    digest="${f[i + 1]}"
    i=$((i + 2))
    source="$content_dir/$digest"
    [ -f "$source" ] || fail 66 "input $name ($digest) is not in the content store"
    mkdir -p "$(dirname -- "$work/$name")"
    ln -sfn "$source" "$work/$name" || fail 66 "cannot link input $name"
done

cd "$work" || fail 66 "cannot enter $work"

case "$mode" in
exec)
    "${f[@]:i}" < "$stdin_path" > "$stdout_path" 2> "$stderr_path"
    finish "$?"
    ;;
package)
    name="${f[i]}"
    version="${f[i + 1]}"
    digest="${f[i + 2]}"
    params=("${f[@]:i+3}")
    archive="$content_dir/$digest"
    [ -f "$archive" ] || fail 66 "package $name $version is not in the content store"
    actual="$(sha256sum "$archive" | cut -d' ' -f1)"
    [ "$actual" = "$digest" ] || fail 65 "package $name hash $actual does not match the reviewed $digest"
    command -v jarvis > /dev/null || fail 69 "jarvis is not on PATH on this system"
    unpacked="$package_root/$digest"
    if [ ! -d "$unpacked" ]; then
        mkdir -p "$unpacked.part" && tar -xzf "$archive" -C "$unpacked.part" \
            && mv "$unpacked.part" "$unpacked" || fail 66 "cannot unpack package $name"
    fi
    pipeline="relay_${_tapisJobUUID:-job}"
    pipeline="${pipeline//-/_}"
    {
        jarvis repo add "$unpacked" \
            && jarvis ppl create "$pipeline" \
            && jarvis ppl append "$name" "${params[@]}" \
            && jarvis ppl run
    } < "$stdin_path" > "$stdout_path" 2> "$stderr_path"
    finish "$?"
    ;;
*)
    fail 64 "unknown mode $mode"
    ;;
esac
