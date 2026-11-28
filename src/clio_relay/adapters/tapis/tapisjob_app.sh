#!/bin/bash
# clio-relay's launcher for Tapis (RL47).
#
# Tapis's ZIP runtime unpacks this archive in the job's directory and runs
# this script with the job's app arguments. Relay passes one argument: the
# path, after an @, of a file holding a base64 record of NUL-terminated
# fields, so no shell quoting is involved and no argument grows long.
#
#   version, directory, stdin, stdout, stderr, content dir, input count,
#   then (name, sha256) per input, then executable, arguments...
#
# A package job is JARVIS's: relay's exec runs jarvis ppl submit through this
# launcher like any command (clio_relay.jarvis_remote), so it only runs a command.
#
# The launcher writes the job's exit code to tapisjob.exitcode (which Tapis
# reads for a FORK job) and relay.exitcode in the job's directory, and exits
# with it.

set -u

job_dir="${_tapisExecSystemOutputDir:-$PWD}"
mkdir -p "$job_dir"

finish() {
    # Tapis reads tapisjob.exitcode for a FORK job and takes a missing one as 0.
    printf '%s\n' "$1" > "$job_dir/tapisjob.exitcode"
    printf '%s\n' "$1" > "$job_dir/relay.exitcode"
    exit "$1"
}

fail() {
    printf 'clio-relay launcher: %s\n' "$2" >&2
    printf 'clio-relay launcher: %s\n' "$2" >> "$job_dir/relay.launcher.log"
    finish "$1"
}

[ "$#" -ge 1 ] || fail 64 "no job record was passed"
[ "${1#@}" != "$1" ] && [ -f "${1#@}" ] || fail 64 "no job record file was passed"
mapfile -d '' -t f < <(base64 -d < "${1#@}") || fail 64 "the job record is not base64"
[ "${f[0]:-}" = "2" ] || fail 64 "unknown job record version ${f[0]:-none}"

work="${f[1]:-$job_dir}"
stdin_path="${f[2]:-/dev/null}"
stdout_path="${f[3]:-$job_dir/stdout}"
stderr_path="${f[4]:-$job_dir/stderr}"
content_dir="${f[5]}"
count="${f[6]}"
i=7

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

"${f[@]:i}" < "$stdin_path" > "$stdout_path" 2> "$stderr_path"
finish "$?"
