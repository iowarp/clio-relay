#!/bin/sh
# Relay's connector beside a package service that JARVIS starts (RL19 to RL23).
#
# Usage, from the job directory, started by the batch script's pre_cmds:
#   sh serve/connect.sh operation idle_seconds frpc
# frpc is "-" for none (the ssh path). JARVIS owns the service: it starts it,
# and jarvis execution stop ends it. The connector only carries its ports.
#
# It reads serve/ports ("name port protocol health_path" per line) and
# serve/secrets.env, which it deletes once read. It runs frpc and restarts it
# if it dies, and writes serve/ready.json once every port listens on this
# host (checked in /proc/net/tcp, never by connecting) and frpc registered
# them. With an idle release it writes serve/idle once no connection used the
# ports for that long; relay then runs jarvis execution stop. It ends with the
# batch shell that started it, or on a signal, and writes serve/stopped.json.

set -u
OPERATION=${1:-}
IDLE=${2:-0}
FRPC=${3:--}
[ "$FRPC" = "-" ] && FRPC=

JOB=$PWD
S=$JOB/serve
BATCH=$PPID
export CLIO_RELAY_JOB_DIR="$JOB"

note() {
    printf '%s %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*" >> "$S/launch.log"
}

if [ -f "$S/secrets.env" ]; then
    chmod 600 "$S/secrets.env" 2>/dev/null
    . "$S/secrets.env"
    rm -f "$S/secrets.env"
fi

alive() {
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

healthy() {
    while read -r name port proto path; do
        [ -n "$name" ] || continue
        listening "$port" || return 1
    done < "$S/ports"
    return 0
}

registered() {
    [ -n "$FRPC" ] || return 0
    while read -r name port proto path; do
        [ -n "$name" ] || continue
        grep -F "[relay-$OPERATION-$name]" "$S/frpc.log" 2>/dev/null |
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

mkdir -p "$S"
rm -f "$S/ready.json" "$S/stopped.json" "$S/idle" "$S/frpc-restarts"
: > "$S/started"
restarts=0

case $FRPC in
    "~/"*) FRPC=$HOME/${FRPC#"~/"} ;;
    /*|"") ;;
    *) FRPC=$JOB/$FRPC ;;
esac
[ -n "$FRPC" ] && [ -f "$FRPC" ] && [ ! -x "$FRPC" ] && chmod u+x "$FRPC"

frpc_pid=
start_frpc() {
    [ -n "$FRPC" ] || return 0
    "$FRPC" -c "$S/frpc.toml" >> "$S/frpc.log" 2>&1 < /dev/null &
    frpc_pid=$!
}
start_frpc
unset CLIO_RELAY_FRP_TOKEN CLIO_RELAY_FRP_SK CLIO_RELAY_HOP_TOKEN
note "connector started on $(hostname) for the batch shell $BATCH"

reason=
trap 'reason=signal' TERM INT HUP
ready=0
idle_for=0
while :; do
    [ -n "$reason" ] && break
    if ! alive "$BATCH"; then
        reason=ended
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
        write_json ready.json "{\"host\": \"$(hostname)\", \"pid\": $BATCH, \"ports\": {$ports}}"
        note "ready"
    fi
    if [ "$ready" = 1 ] && [ "$IDLE" -gt 0 ] && [ ! -f "$S/idle" ]; then
        if [ "$(connections)" -gt 0 ]; then
            idle_for=0
        else
            idle_for=$((idle_for + 1))
            if [ "$idle_for" -ge "$IDLE" ]; then
                write_json idle "{\"idle_s\": $IDLE}"
                note "idle for ${IDLE}s; relay stops the pipeline"
            fi
        fi
    fi
    sleep 1
done

[ -n "$frpc_pid" ] && kill -TERM "$frpc_pid" 2>/dev/null
[ -f "$S/frpc" ] && rm -f "$S/frpc"
[ -f "$S/idle" ] && reason=idle
write_json stopped.json "{\"reason\": \"$reason\", \"exit_code\": 0, \"frpc_restarts\": $restarts}"
note "connector ended: $reason"
exit 0
