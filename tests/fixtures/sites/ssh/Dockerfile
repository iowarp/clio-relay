# Base SSH site for the clio-relay rework fixtures. Build context: tests/fixtures/sites.
ARG UV_IMAGE=ghcr.io/astral-sh/uv:0.11.28
FROM ${UV_IMAGE} AS uv

FROM ubuntu:22.04
LABEL clio-relay-rework="1" clio-relay-rework.owner="w2-fixtures"
ENV DEBIAN_FRONTEND=noninteractive LANG=C.UTF-8

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        openssh-server openssh-client python3 python3-venv ca-certificates curl git \
        procps iproute2 rsync less tar gzip xz-utils bzip2 unzip sudo \
    && rm -rf /var/lib/apt/lists/* \
    && rm -f /etc/ssh/ssh_host_*

# The fixture services run from their own Python, kept off the user's PATH so the
# site looks like a plain host to relay's bootstrap.
COPY --from=uv /uv /opt/clio-fixtures/uv
ENV UV_PYTHON_INSTALL_DIR=/opt/clio-fixtures/python
RUN /opt/clio-fixtures/uv venv --python 3.12 /opt/clio-fixtures/venv \
    && /opt/clio-fixtures/uv pip install --no-cache \
        --python /opt/clio-fixtures/venv/bin/python \
        "fastmcp==4.0.0b1" "fastmcp-slim[client,server]==4.0.0b1" "fastmcp-tasks==4.0.0b1" \
    && rm -rf /root/.cache

RUN useradd --create-home --shell /bin/bash --uid 2000 relay

COPY common/sshd_config /etc/ssh/sshd_config
COPY common/site-lib.sh /opt/clio-fixtures/site-lib.sh
COPY services/secret_service.py services/counter_mcp.py /opt/clio-fixtures/services/
COPY ssh/entrypoint.sh /opt/clio-fixtures/entrypoint.sh
RUN mkdir -p /etc/ssh/sshd_config.d \
    && printf '#!/bin/sh\nexec /opt/clio-fixtures/venv/bin/python /opt/clio-fixtures/services/secret_service.py "$@"\n' \
        > /usr/local/bin/clio-fixture-secret-service \
    && printf '#!/bin/sh\nexec /opt/clio-fixtures/venv/bin/python /opt/clio-fixtures/services/counter_mcp.py "$@"\n' \
        > /usr/local/bin/clio-fixture-counter-mcp \
    && chmod 0755 /usr/local/bin/clio-fixture-* /opt/clio-fixtures/entrypoint.sh \
    && chmod 0644 /opt/clio-fixtures/site-lib.sh /opt/clio-fixtures/services/*.py

EXPOSE 22
ENTRYPOINT ["/opt/clio-fixtures/entrypoint.sh"]
