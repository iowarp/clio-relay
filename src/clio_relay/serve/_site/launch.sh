#!/bin/sh
# Relay's wrapper around a served program (RL19 to RL23).
#
# Usage, from the job directory: sh serve/launch.sh -- program [arguments...]
#
# It reads serve/ports ("name port protocol health_path" per line, "-" for no
# path) and serve/secrets.env, which it deletes once read. It starts the
# program, checks each port as a listening socket in /proc/net/tcp (never by
# connecting), runs frpc beside the program when CLIO_RELAY_FRPC is set and
# restarts it if it dies, and writes serve/ready.json once every port is up.
# It stops on serve/stop, on a signal, on idle release, or when the program
# exits. A stop gives the program SIGTERM and CLIO_RELAY_STOP_GRACE seconds,
# then lists what the program wrote in serve/artifacts.txt and the end in
# serve/stopped.json.

set -u
[ "${1:-}" = "--" ] && shift

JOB=$PWD
S=$JOB/serve
export CLIO_RELAY_JOB_DIR="$JOB"
GRACE=${CLIO_RELAY_STOP_GRACE:-30}
IDLE=${CLIO_RELAY_IDLE_S:-0}
FRPC=${CLIO_RELAY_FRPC:-}
SVC_DIR=${CLIO_RELAY_SERVICE_DIR:-$JOB}

note() {
    printf '%s %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*" >> "$S/launch.log"
}

if [ -f "$S/secrets.env" ]; then
    chmod 600 "$S/secrets.env" 2>/dev/null
    . "$S/secrets.env"
    rm -f "$S/secrets.env"
fi

alive() {
    # kill -0 also succeeds on a zombie, which dash leaves until it waits.
    kill -0 "$1" 2>/dev/null || return 1
    [ -r "/proc/$1/stat" ] || return 0
    [ "$(sed 's/.*) //' "/proc/$1/stat" 2>/dev/null | cut -d' ' -f1)" != Z ]
}

hexport() {
    printf '%04X' "$1"
}

listening() {
    cat /proc/net/tcp /proc/net/tcp6 2>/dev/null |
        awk -v h=":$(hexport "$1")" '$4 == "0A" && substr($2, length($2) - 4) == h { f = 1 } END { exit !f }'
}

established() {
    cat /proc/net/tcp /proc/net/tcp6 2>/dev/null |
        awk -v h=":$(hexport "$1")" '$4 == "01" && substr($2, length($2) - 4) == h { n++ } END { print n + 0 }'
}

answers() {
    if command -v python3 >/dev/null 2>&1; then
        python3 - "$1" "$2" <<'PY'
import sys
import urllib.error
import urllib.request

try:
    urllib.request.urlopen(f"http://127.0.0.1:{sys.argv[1]}{sys.argv[2]}", timeout=5)
except urllib.error.HTTPError as exc:
    sys.exit(0 if exc.code < 500 else 1)
except Exception:
    sys.exit(1)
PY
    elif command -v curl >/dev/null 2>&1; then
        code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 "http://127.0.0.1:$1$2")
        [ "$code" != 000 ] && [ "$code" -lt 500 ]
    else
        return 0
    fi
}

healthy() {
    while read -r name port proto path; do
        [ -n "$name" ] || continue
        listening "$port" || return 1
        if [ "$path" != "-" ] && [ "$proto" != tcp ]; then
            answers "$port" "$path" || return 1
        fi
    done < "$S/ports"
    return 0
}

registered() {
    [ -n "$FRPC" ] || return 0
    while read -r name port proto path; do
        [ -n "$name" ] || continue
        grep -F "[relay-${CLIO_RELAY_OPERATION:-}-$name]" "$S/frpc.log" 2>/dev/null |
            grep -q "start proxy success" || return 1
    done < "$S/ports"
    return 0
}

connections() {
    total=0
    while read -r name port proto path; do
        [ -n "$name" ] || continue
        total=$((total + $(established "$port")))
    done < "$S/ports"
    echo "$total"
}

write_json() {
    printf '%s\n' "$2" > "$S/$1.tmp" && mv -f "$S/$1.tmp" "$S/$1"
}

# Relay writes serve/stop from another machine. On NFS a plain test can keep a
# cached "absent" for the mount's acdirmax (120 s on some clusters); reading
# the directory revalidates it.
stop_asked() {
    [ -f "$S/stop" ] && return 0
    ls -a "$S" 2>/dev/null | grep -qx stop
}

finish() {
    reason=$1
    code=$2
    extra=${3:-}
    : > "$S/artifacts.txt.tmp"
    for dir in "$JOB" "$SVC_DIR"; do
        [ -d "$dir" ] || continue
        if [ "$dir" = "$JOB" ]; then
            # The package runner's archive and JARVIS tree are relay's own; package
            # outputs land in .relay-jarvis/shared.
            (cd "$JOB" && find . -type f -newer "$S/started" ! -path './serve/*' \
                ! -path './.relay-package/*' ! -path '*/__pycache__/*' \
                ! \( -path './.relay-jarvis/*' ! -path './.relay-jarvis/shared/*' \) \
                ! -name stdout ! -name stderr ! -name state.json ! -name job.json \
                ! -name spec.json ! -name native_id ! -name submit.sh ! -name supervisor.log \
                ! -name cancel ! -name states 2>/dev/null) |
                while IFS= read -r file; do
                    printf '%s\t%s\n' "$(wc -c < "$JOB/$file" | tr -d ' ')" "${file#./}"
                done >> "$S/artifacts.txt.tmp"
        elif [ "$SVC_DIR" != "$JOB" ]; then
            find "$SVC_DIR" -maxdepth 2 -type f -newer "$S/started" 2>/dev/null | head -n 200 |
                while IFS= read -r file; do
                    printf '%s\t%s\n' "$(wc -c < "$file" | tr -d ' ')" "$file"
                done >> "$S/artifacts.txt.tmp"
        fi
    done
    mv -f "$S/artifacts.txt.tmp" "$S/artifacts.txt"
    # A staged frpc is a copy of a content-addressed input; the site keeps the blob.
    [ -f "$S/frpc" ] && rm -f "$S/frpc"
    write_json stopped.json "{\"reason\": \"$reason\", \"exit_code\": $code, \"frpc_restarts\": $restarts$extra}"
    note "ended: $reason, program exit code $code"
}

mkdir -p "$S"
rm -f "$S/ready.json" "$S/stopped.json" "$S/stop-begin" "$S/frpc-restarts"
: > "$S/started"
restarts=0

while read -r name port proto path; do
    [ -n "$name" ] || continue
    if listening "$port"; then
        note "port $port ($name) is already taken on $(hostname)"
        finish port_taken 98 ", \"port\": $port, \"port_name\": \"$name\", \"host\": \"$(hostname)\""
        exit 98
    fi
done < "$S/ports"

case $FRPC in
    "~/"*) FRPC=$HOME/${FRPC#"~/"} ;;
    /*|"") ;;
    *) FRPC=$JOB/$FRPC ;;
esac
[ -n "$FRPC" ] && [ -f "$FRPC" ] && [ ! -x "$FRPC" ] && chmod u+x "$FRPC"

(
    unset CLIO_RELAY_FRP_TOKEN CLIO_RELAY_FRP_SK CLIO_RELAY_HOP_TOKEN
    cd "$SVC_DIR" && exec "$@"
) > "$S/service.log" 2>&1 < /dev/null &
svc=$!
note "started program pid $svc on $(hostname)"

frpc_pid=
start_frpc() {
    [ -n "$FRPC" ] || return 0
    "$FRPC" -c "$S/frpc.toml" >> "$S/frpc.log" 2>&1 < /dev/null &
    frpc_pid=$!
}
start_frpc

reason=
trap 'reason=signal' TERM INT HUP
ready=0
idle_for=0
while :; do
    stop_asked && reason=${reason:-stopped}
    [ -n "$reason" ] && break
    if ! alive "$svc"; then
        reason=exited
        break
    fi
    if [ -n "$frpc_pid" ] && ! alive "$frpc_pid"; then
        restarts=$((restarts + 1))
        echo "$restarts" > "$S/frpc-restarts"
        note "frpc exited, starting it again (restart $restarts)"
        start_frpc
    fi
    if [ "$ready" = 0 ] && healthy && registered; then
        ready=1
        ports=$(awk 'NF { printf "%s\"%s\": %s", (n++ ? ", " : ""), $1, $2 }' "$S/ports")
        write_json ready.json "{\"host\": \"$(hostname)\", \"pid\": $svc, \"ports\": {$ports}}"
        note "ready"
    fi
    if [ "$ready" = 1 ] && [ "$IDLE" -gt 0 ]; then
        if [ "$(connections)" -gt 0 ]; then
            idle_for=0
        else
            idle_for=$((idle_for + 1))
            if [ "$idle_for" -ge "$IDLE" ]; then
                reason=idle
                break
            fi
        fi
    fi
    sleep 1
done

: > "$S/stop-begin"
note "stopping: $reason"
if alive "$svc"; then
    kill -TERM "$svc" 2>/dev/null
    waited=0
    while alive "$svc" && [ "$waited" -lt "$GRACE" ]; do
        sleep 1
        waited=$((waited + 1))
    done
    if alive "$svc"; then
        note "grace of ${GRACE}s is over, killing the program"
        kill -KILL "$svc" 2>/dev/null
    fi
fi
wait "$svc"
code=$?
[ -n "$frpc_pid" ] && kill -TERM "$frpc_pid" 2>/dev/null
finish "$reason" "$code"

case $reason in
    stopped|idle) exit 0 ;;
    signal) exit 143 ;;
    *) exit "$code" ;;
esac
