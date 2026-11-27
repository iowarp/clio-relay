# SSH plus a one-node Slurm 21.08 cluster with slurmdbd, as on Ares.
ARG BASE_IMAGE=clio-relay-rework-fx/ssh:latest
FROM ${BASE_IMAGE}
LABEL clio-relay-rework="1" clio-relay-rework.owner="w2-fixtures"

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        slurmctld slurmd slurmdbd slurm-client munge mariadb-server \
    && rm -rf /var/lib/apt/lists/* \
    && rm -f /etc/munge/munge.key

COPY slurm/slurm.conf /etc/slurm/slurm.conf
COPY slurm/slurmdbd.conf /etc/slurm/slurmdbd.conf
COPY slurm/entrypoint.sh /opt/clio-fixtures/entrypoint.sh
RUN chown slurm:slurm /etc/slurm/slurmdbd.conf \
    && chmod 0600 /etc/slurm/slurmdbd.conf \
    && chmod 0644 /etc/slurm/slurm.conf \
    && chmod 0755 /opt/clio-fixtures/entrypoint.sh

EXPOSE 22
