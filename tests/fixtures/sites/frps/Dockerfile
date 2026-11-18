# frp rendezvous host, frps at the version relay pins (bootstrap_constants.py).
FROM alpine:3.20
LABEL clio-relay-rework="1" clio-relay-rework.owner="w2-fixtures"
ARG FRP_VERSION=0.69.1
ARG FRP_LINUX_AMD64_SHA256=7be257b72dbbc60bcb3e0e25a5afd1dfac7b63f897084864d3c956dd3d5674e1

RUN apk add --no-cache socat curl ca-certificates \
    && archive="frp_${FRP_VERSION}_linux_amd64.tar.gz" \
    && curl -fsSL -o "/tmp/${archive}" \
        "https://github.com/fatedier/frp/releases/download/v${FRP_VERSION}/${archive}" \
    && echo "${FRP_LINUX_AMD64_SHA256}  /tmp/${archive}" | sha256sum -c - \
    && tar -xzf "/tmp/${archive}" -C /tmp \
    && install -m 0755 "/tmp/frp_${FRP_VERSION}_linux_amd64/frps" /usr/local/bin/frps \
    && install -m 0755 "/tmp/frp_${FRP_VERSION}_linux_amd64/frpc" /usr/local/bin/frpc \
    && rm -rf /tmp/frp_* \
    && /usr/local/bin/frps --version

COPY frps/entrypoint.sh /entrypoint.sh
RUN chmod 0755 /entrypoint.sh
EXPOSE 7000 7443
ENTRYPOINT ["/entrypoint.sh"]
