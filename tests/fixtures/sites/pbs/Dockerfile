# SSH plus a one-node OpenPBS server, built from source. psij's PBS executor reads
# `qstat -f -F json -x`, which only PBS Professional and OpenPBS answer.
ARG BASE_IMAGE=clio-relay-rework-fx/ssh:latest

FROM ubuntu:22.04 AS build
ARG OPENPBS_VERSION=23.06.06
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        gcc g++ make libtool autoconf automake pkg-config curl ca-certificates \
        libhwloc-dev libx11-dev libxt-dev libedit-dev libical-dev ncurses-dev perl \
        postgresql-server-dev-all postgresql-contrib python3-dev tcl-dev tk-dev swig \
        libexpat-dev libssl-dev libxext-dev libxft-dev libcjson-dev
RUN mkdir -p /src \
    && curl -fsSL "https://github.com/openpbs/openpbs/archive/refs/tags/v${OPENPBS_VERSION}.tar.gz" \
        | tar -xz -C /src --strip-components=1
WORKDIR /src
RUN ./autogen.sh \
    && ./configure --prefix=/opt/pbs \
    && make -j"$(nproc)" \
    && make install DESTDIR=/out \
    && find /out -mindepth 1 -maxdepth 3 -not -path '/out/opt*'

FROM ${BASE_IMAGE}
LABEL clio-relay-rework="1" clio-relay-rework.owner="w2-fixtures"
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        expat libedit2 postgresql postgresql-contrib postgresql-server-dev-all \
        sendmail-bin tcl tk libical3 libcjson1 libhwloc15 libxt6 libxft2 \
    && rm -rf /var/lib/apt/lists/*
# Only /opt/pbs: copying all of /out would replace Ubuntu's /lib symlink.
COPY --from=build /out/opt/pbs /opt/pbs
RUN /opt/pbs/libexec/pbs_postinstall \
    && chmod 4755 /opt/pbs/sbin/pbs_iff /opt/pbs/sbin/pbs_rcp \
    && for tool in /opt/pbs/bin/*; do ln -sf "$tool" /usr/local/bin/; done
COPY pbs/entrypoint.sh /opt/clio-fixtures/entrypoint.sh
RUN chmod 0755 /opt/clio-fixtures/entrypoint.sh

EXPOSE 22
