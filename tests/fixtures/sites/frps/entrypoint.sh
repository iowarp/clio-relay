#!/bin/sh
# frp rendezvous host: frps takes tcp and websocket on its bind port, and a TLS
# front on a second port carries wss, as Cloudflare does in front of a public frps.
set -eu

token=$(tr -d '\r\n' </fixture-secrets/frp_token)
cat >/tmp/frps.toml <<EOF
bindAddr = "0.0.0.0"
bindPort = 7000
auth.method = "token"
auth.token = "${token}"
transport.tcpMux = true
EOF
chmod 0600 /tmp/frps.toml
cat /fixture-secrets/wss_key.pem /fixture-secrets/wss_cert.pem >/tmp/wss.pem
chmod 0600 /tmp/wss.pem

/usr/local/bin/frps -c /tmp/frps.toml &
frps_pid=$!
socat OPENSSL-LISTEN:7443,reuseaddr,fork,cert=/tmp/wss.pem,verify=0 TCP:127.0.0.1:7000 &
socat_pid=$!

trap 'kill "$frps_pid" "$socat_pid" 2>/dev/null; exit 0' TERM INT
while kill -0 "$frps_pid" 2>/dev/null && kill -0 "$socat_pid" 2>/dev/null; do
    sleep 2
done
echo "frps or the wss front exited" >&2
exit 1
