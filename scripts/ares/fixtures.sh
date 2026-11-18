#!/usr/bin/env bash
# Build, start, inspect and remove the clio-relay rework fixtures on a Docker host.
#
#   scripts/ares/fixtures.sh build            build the images
#   scripts/ares/fixtures.sh up               build if needed, start, wait, write the env file
#   scripts/ares/fixtures.sh status           list the fixture containers and the port map
#   scripts/ares/fixtures.sh down [--images]  remove the fixture containers and network
#
# Every container, network and image carries the labels clio-relay-rework=1,
# com.docker.compose.project=clio-relay-rework and clio-relay-rework.owner=w2-fixtures.
# Tear-down selects by those labels only, so nothing else on the host is touched.
#
# Settings (environment):
#   FX_STATE_DIR   where keys, secrets, ports and the env file live
#                  (default ~/clio-relay-rework/state/w2-fixtures)
#   FX_BIND_ADDR   address the published ports bind to (default 127.0.0.1)
#   FX_PORT_BASE   first port tried when choosing free ports (default 47100)
#   FX_SITES       space-separated subset of: ssh slurm pbs otp frps secret counter
set -euo pipefail

REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
SITES_DIR=$REPO_ROOT/tests/fixtures/sites
STATE_DIR=${FX_STATE_DIR:-$HOME/clio-relay-rework/state/w2-fixtures}
BIND_ADDR=${FX_BIND_ADDR:-127.0.0.1}
PORT_BASE=${FX_PORT_BASE:-47100}
PROJECT=clio-relay-rework
OWNER=w2-fixtures
PREFIX=clio-relay-rework-fx
NETWORK=$PREFIX
ALL_SITES="ssh slurm pbs otp frps secret counter"
SITES=${FX_SITES:-$ALL_SITES}
LABELS=(
    --label "clio-relay-rework=1"
    --label "com.docker.compose.project=$PROJECT"
    --label "clio-relay-rework.owner=$OWNER"
)
FILTERS=(--filter "label=clio-relay-rework=1" --filter "label=clio-relay-rework.owner=$OWNER")
PORT_NAMES="SSH SLURM PBS OTP FRPS FRPS_WSS SECRET COUNTER"

log() { printf '[fixtures] %s\n' "$*" >&2; }
die() {
    log "error: $*"
    exit 1
}

image_for() {
    case $1 in
    ssh | secret | counter) echo "$PREFIX/ssh:latest" ;;
    slurm | pbs | otp | frps) echo "$PREFIX/$1:latest" ;;
    *) die "unknown site $1" ;;
    esac
}

has_site() { [[ " $SITES " == *" $1 "* ]]; }

# ---------------------------------------------------------------- build

build_one() {
    local name=$1
    shift
    log "building $PREFIX/$name"
    docker build --quiet "${LABELS[@]}" -t "$PREFIX/$name:latest" \
        -f "$SITES_DIR/$name/Dockerfile" "$@" "$SITES_DIR" >/dev/null
}

cmd_build() {
    local base=(--build-arg "BASE_IMAGE=$PREFIX/ssh:latest")
    build_one ssh
    has_site slurm && build_one slurm "${base[@]}"
    has_site otp && build_one otp "${base[@]}"
    has_site frps && build_one frps
    has_site pbs && build_one pbs "${base[@]}"
    docker image prune --force "${FILTERS[@]}" >/dev/null
    log "images built"
}

images_present() {
    local site
    for site in $SITES; do
        docker image inspect "$(image_for "$site")" >/dev/null 2>&1 || return 1
    done
}

# ---------------------------------------------------------------- state

port_in_use() {
    [ -n "$(ss -Htln "sport = :$1" 2>/dev/null)" ]
}

choose_ports() {
    local ports_file=$STATE_DIR/ports.env
    if [ -f "$ports_file" ]; then
        # Reuse the earlier map so known_hosts and clients stay valid.
        # shellcheck disable=SC1090
        source "$ports_file"
        local name var
        for name in $PORT_NAMES; do
            var=FX_${name}_PORT
            if port_in_use "${!var}"; then
                die "port ${!var} ($var) is taken; run down first, or delete $ports_file"
            fi
        done
        return
    fi
    local port=$PORT_BASE limit=$((PORT_BASE + 500)) name
    : >"$ports_file.tmp"
    for name in $PORT_NAMES; do
        while port_in_use "$port"; do
            port=$((port + 1))
            [ "$port" -lt "$limit" ] || die "no free port between $PORT_BASE and $limit"
        done
        echo "FX_${name}_PORT=$port" >>"$ports_file.tmp"
        port=$((port + 1))
    done
    mv "$ports_file.tmp" "$ports_file"
    # shellcheck disable=SC1090
    source "$ports_file"
}

make_secrets() {
    local s=$STATE_DIR/secrets
    install -d -m 0700 "$s"
    [ -f "$s/client_ed25519" ] ||
        ssh-keygen -q -t ed25519 -N '' -C clio-relay-rework-fixture-client -f "$s/client_ed25519"
    [ -f "$s/ssh_host_ed25519_key" ] ||
        ssh-keygen -q -t ed25519 -N '' -C clio-relay-rework-fixture-host -f "$s/ssh_host_ed25519_key"
    [ -f "$s/frp_token" ] || (umask 077 && openssl rand -hex 24 >"$s/frp_token")
    [ -f "$s/secret_service_secret" ] || (umask 077 && openssl rand -hex 24 >"$s/secret_service_secret")
    [ -f "$s/otp_totp_secret" ] ||
        (umask 077 && head -c 20 /dev/urandom | base32 | tr -d '=\n' >"$s/otp_totp_secret")
    if [ ! -f "$s/wss_cert.pem" ]; then
        (umask 077 && openssl req -x509 -newkey rsa:2048 -nodes -days 365 \
            -subj /CN=fx-frps \
            -addext "subjectAltName=DNS:fx-frps,DNS:localhost,IP:127.0.0.1" \
            -keyout "$s/wss_key.pem" -out "$s/wss_cert.pem" 2>/dev/null)
    fi

    # Per-role bundles: each container gets only what it needs. The client's
    # private key never enters a container.
    local m=$STATE_DIR/bundles
    rm -rf "$m"
    install -d -m 0700 "$m/site" "$m/otp" "$m/frps" "$m/secret"
    cp "$s/ssh_host_ed25519_key" "$s/ssh_host_ed25519_key.pub" "$s/client_ed25519.pub" "$m/site/"
    cp "$m/site/"* "$m/otp/"
    cp "$s/otp_totp_secret" "$m/otp/"
    cp "$s/frp_token" "$s/wss_key.pem" "$s/wss_cert.pem" "$m/frps/"
    cp "$s/secret_service_secret" "$m/secret/"
}

write_client_files() {
    local s=$STATE_DIR/secrets host_key connect_addr=$BIND_ADDR
    [ "$connect_addr" != 0.0.0.0 ] || connect_addr=127.0.0.1
    host_key=$(cut -d' ' -f1,2 "$s/ssh_host_ed25519_key.pub")
    : >"$STATE_DIR/known_hosts"
    : >"$STATE_DIR/ssh_config"
    local site var
    for site in ssh slurm pbs otp; do
        has_site "$site" || continue
        var=FX_${site^^}_PORT
        echo "[$connect_addr]:${!var} $host_key" >>"$STATE_DIR/known_hosts"
        cat >>"$STATE_DIR/ssh_config" <<EOF
Host fx-$site
    HostName $connect_addr
    Port ${!var}
    User relay
    IdentityFile $s/client_ed25519
    IdentitiesOnly yes
    UserKnownHostsFile $STATE_DIR/known_hosts
    StrictHostKeyChecking yes
    ConnectTimeout 10

EOF
    done
    cat >"$STATE_DIR/fixtures.env" <<EOF
# Written by scripts/ares/fixtures.sh up. Secrets are referenced by file, never inlined.
FX_STATE_DIR=$STATE_DIR
FX_SITES="$SITES"
FX_BIND_ADDR=$BIND_ADDR
FX_CONNECT_ADDR=$connect_addr
FX_USER=relay
FX_SSH_CONFIG=$STATE_DIR/ssh_config
FX_SSH_KEY=$s/client_ed25519
FX_KNOWN_HOSTS=$STATE_DIR/known_hosts
$(cat "$STATE_DIR/ports.env")
FX_SECRET_URL=http://$connect_addr:$FX_SECRET_PORT
FX_COUNTER_URL=http://$connect_addr:$FX_COUNTER_PORT/mcp
FX_SECRET_FILE=$s/secret_service_secret
FX_FRP_TOKEN_FILE=$s/frp_token
FX_OTP_SECRET_FILE=$s/otp_totp_secret
FX_WSS_CA_FILE=$s/wss_cert.pem
FX_FRPC=$STATE_DIR/bin/frpc
FX_NETWORK=$NETWORK
FX_INNER_FRPS_ADDR=fx-frps
FX_INNER_FRPS_PORT=7000
FX_INNER_FRPS_WSS_PORT=7443
FX_INNER_SECRET_URL=http://fx-secret:8080
FX_INNER_COUNTER_URL=http://fx-counter:8000/mcp
EOF
    chmod 0600 "$STATE_DIR/fixtures.env"
}

# ---------------------------------------------------------------- run

start_container() {
    local site=$1 bundle=$2
    shift 2
    local name=$PREFIX-$site
    docker create --name "$name" --hostname "fx-$site" \
        --network "$NETWORK" --network-alias "fx-$site" \
        --init --stop-timeout 15 \
        "${LABELS[@]}" --label "com.docker.compose.service=fx-$site" \
        "$@" >/dev/null
    # Copied, not bind-mounted: the state directory may sit on NFS with root squash.
    docker cp "$STATE_DIR/bundles/$bundle" "$name:/fixture-secrets" >/dev/null
    docker start "$name" >/dev/null
    log "started $name"
}

wait_until() {
    local what=$1 tries=$2 i
    shift 2
    for ((i = 0; i < tries; i++)); do
        if "$@" >/dev/null 2>&1; then
            log "$what ready"
            return 0
        fi
        sleep 2
    done
    die "$what not ready after $((tries * 2))s"
}

tcp_open() { timeout 3 bash -c "exec 3<>/dev/tcp/$1/$2"; }

cmd_up() {
    command -v docker >/dev/null || die "docker not found"
    install -d -m 0700 "$STATE_DIR"
    if [ -n "$(docker ps -aq "${FILTERS[@]}")" ]; then
        die "fixtures already exist; run status, or down first"
    fi
    images_present || cmd_build
    choose_ports
    make_secrets
    write_client_files

    docker network create "${LABELS[@]}" "$NETWORK" >/dev/null
    local b=$BIND_ADDR
    if has_site frps; then
        # The tests drive frpc from the host, at the same pinned version as frps.
        install -d "$STATE_DIR/bin"
        local tmp
        tmp=$(docker create "${LABELS[@]}" "$(image_for frps)")
        docker cp "$tmp:/usr/local/bin/frpc" "$STATE_DIR/bin/frpc" >/dev/null
        docker rm "$tmp" >/dev/null
    fi
    has_site frps && start_container frps frps -p "$b:$FX_FRPS_PORT:7000" \
        -p "$b:$FX_FRPS_WSS_PORT:7443" "$(image_for frps)"
    has_site secret && start_container secret secret -p "$b:$FX_SECRET_PORT:8080" \
        "$(image_for secret)" sh -c 'FIXTURE_SECRET=$(cat /fixture-secrets/secret_service_secret) \
            exec clio-fixture-secret-service --host 0.0.0.0 --port 8080 \
            --final-artifact /tmp/secret-service-final.json'
    has_site counter && start_container counter site -p "$b:$FX_COUNTER_PORT:8000" \
        "$(image_for counter)" clio-fixture-counter-mcp --transport http --host 0.0.0.0 --port 8000
    has_site ssh && start_container ssh site -p "$b:$FX_SSH_PORT:22" "$(image_for ssh)"
    has_site otp && start_container otp otp -p "$b:$FX_OTP_PORT:22" "$(image_for otp)"
    has_site slurm && start_container slurm site -p "$b:$FX_SLURM_PORT:22" "$(image_for slurm)"
    has_site pbs && start_container pbs site -p "$b:$FX_PBS_PORT:22" "$(image_for pbs)"

    local a
    a=$(sed -n 's/^FX_CONNECT_ADDR=//p' "$STATE_DIR/fixtures.env")
    has_site frps && wait_until frps 30 tcp_open "$a" "$FX_FRPS_PORT"
    has_site secret && wait_until secret 30 curl -fsS "http://$a:$FX_SECRET_PORT/health"
    has_site counter && wait_until counter 60 tcp_open "$a" "$FX_COUNTER_PORT"
    has_site otp && wait_until otp 30 ssh-keyscan -p "$FX_OTP_PORT" "$a"
    local site
    for site in ssh slurm pbs; do
        has_site "$site" || continue
        wait_until "$site" 120 ssh -F "$STATE_DIR/ssh_config" -o BatchMode=yes "fx-$site" true
    done
    cmd_status
    log "env file: $STATE_DIR/fixtures.env"
}

cmd_status() {
    docker ps -a "${FILTERS[@]}" --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}'
    if [ -f "$STATE_DIR/ports.env" ]; then
        echo
        sed 's/^FX_/  /' "$STATE_DIR/ports.env"
    fi
}

cmd_down() {
    local ids
    ids=$(docker ps -aq "${FILTERS[@]}")
    if [ -n "$ids" ]; then
        # shellcheck disable=SC2086
        docker rm -f $ids >/dev/null
        log "removed $(echo "$ids" | wc -l) containers"
    fi
    ids=$(docker network ls -q "${FILTERS[@]}")
    # shellcheck disable=SC2086
    [ -z "$ids" ] || docker network rm $ids >/dev/null
    ids=$(docker volume ls -q "${FILTERS[@]}")
    # shellcheck disable=SC2086
    [ -z "$ids" ] || docker volume rm $ids >/dev/null
    if [ "${1:-}" = "--images" ]; then
        ids=$(docker image ls -q "${FILTERS[@]}" | sort -u)
        # shellcheck disable=SC2086
        [ -z "$ids" ] || docker image rm -f $ids >/dev/null
        log "removed fixture images"
    fi
    rm -rf "$STATE_DIR/bundles"
    log "down"
}

case ${1:-} in
build) cmd_build ;;
up) cmd_up ;;
status) cmd_status ;;
down) cmd_down "${2:-}" ;;
*)
    sed -n '2,8p' "$0"
    exit 2
    ;;
esac
