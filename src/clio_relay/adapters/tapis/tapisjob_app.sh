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
#   for mode package or package-service: name, version, sha256, key=value parameters...
#
# A package is `<repo>.<package>`, and its archive holds
# `<repo>/<repo>/<package>/...`. The launcher runs it with JARVIS_ROOT in the
# job's directory: jarvis init, repo add <unpacked>/<repo>, ppl create,
# ppl append <name> pkg key=value..., then ppl run. A package-service job runs
# ppl start instead, holds until the job is cancelled, then runs ppl stop.
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
package | package-service)
    name="${f[i]}"
    version="${f[i + 1]}"
    digest="${f[i + 2]}"
    params=("${f[@]:i+3}")
    [[ "$name" =~ ^[A-Za-z0-9_]+\.[A-Za-z0-9_]+$ ]] \
        || fail 64 "package name $name is not <repo>.<package>"
    repo="${name%%.*}"
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
    [ -d "$unpacked/$repo/$repo" ] || fail 66 "package archive has no $repo/$repo directory"
    pipeline="relay_${_tapisJobUUID:-job}"
    pipeline="${pipeline//-/_}"
    # A JARVIS of the job's own, so the site's ~/.ppi-jarvis is never read or changed.
    export JARVIS_ROOT="$job_dir/.relay-jarvis"
    {
        jarvis init "$JARVIS_ROOT/config" "$JARVIS_ROOT/private" "$JARVIS_ROOT/shared" \
            && jarvis repo add "$unpacked/$repo" \
            && jarvis ppl create "$pipeline" \
            && jarvis ppl append "$name" pkg ${params[@]+"${params[@]}"} \
            || finish "$?"
        if [ "$mode" = package ]; then
            jarvis ppl run
            finish "$?"
        fi
        # A service runs until the job is cancelled, then stops cleanly.
        # A Slurm cancel sends TERM. A Tapis FORK cancel is `pkill -9 -P`, which
        # kills only the sleep, so a sleep that dies early also stops the service.
        sleeper=
        stop_service() {
            trap - TERM INT HUP
            [ -z "$sleeper" ] || kill "$sleeper" 2> /dev/null
            jarvis ppl stop
            finish "$?"
        }
        trap stop_service TERM INT HUP
        jarvis ppl start || { started=$?; jarvis ppl stop; finish "$started"; }
        while :; do
            sleep 60 &
            sleeper=$!
            wait "$sleeper" || break
        done
        stop_service
    } < "$stdin_path" > "$stdout_path" 2> "$stderr_path"
    ;;
*)
    fail 64 "unknown mode $mode"
    ;;
esac
