#!/bin/bash
# Shared start-up steps for every SSH fixture site. Sourced by each entrypoint.
#
# The bring-up script copies /fixture-secrets in with the host key, the
# client public key and any per-site secret. Nothing secret is baked into an image.
set -euo pipefail

FX_SECRETS=${FX_SECRETS:-/fixture-secrets}

fx_log() {
    printf '[fixture %s] %s\n' "$(hostname -s)" "$*" >&2
}

fx_install_host_key() {
    install -d -m 0755 /etc/ssh/fixture
    install -m 0600 "$FX_SECRETS/ssh_host_ed25519_key" /etc/ssh/fixture/ssh_host_ed25519_key
    install -m 0644 "$FX_SECRETS/ssh_host_ed25519_key.pub" \
        /etc/ssh/fixture/ssh_host_ed25519_key.pub
}

# authorized_keys is copied once and stays writable by the user, so a test can
# add and remove its own key (the Tapis credential row does exactly that).
fx_install_client_key() {
    local ssh_dir=/home/relay/.ssh
    install -d -m 0700 -o relay -g relay "$ssh_dir"
    if [ ! -f "$ssh_dir/authorized_keys" ]; then
        install -m 0600 -o relay -g relay "$FX_SECRETS/client_ed25519.pub" \
            "$ssh_dir/authorized_keys"
    fi
}

fx_wait_for() {
    local what=$1 tries=$2
    shift 2
    local i
    for ((i = 0; i < tries; i++)); do
        if "$@" >/dev/null 2>&1; then
            return 0
        fi
        sleep 1
    done
    fx_log "timed out waiting for $what"
    return 1
}

fx_prepare_ssh() {
    fx_install_host_key
    fx_install_client_key
    mkdir -p /run/sshd
    /usr/sbin/sshd -t
}

fx_exec_sshd() {
    fx_log "sshd ready"
    exec /usr/sbin/sshd -D -e
}
