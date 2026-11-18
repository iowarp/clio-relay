#!/bin/bash
# SSH host that is also a one-node Slurm cluster with accounting.
# shellcheck source=SCRIPTDIR/../common/site-lib.sh
source /opt/clio-fixtures/site-lib.sh

if [ "$#" -gt 0 ]; then
    exec "$@"
fi

fx_prepare_ssh

if [ ! -s /etc/munge/munge.key ]; then
    dd if=/dev/urandom of=/etc/munge/munge.key bs=1024 count=1 status=none
fi
chown munge:munge /etc/munge/munge.key
chmod 0400 /etc/munge/munge.key
install -d -o munge -g munge -m 0755 /run/munge
su -s /bin/sh munge -c /usr/sbin/munged
fx_wait_for munge 30 munge -n

install -d -o mysql -g mysql /run/mysqld
if [ ! -d /var/lib/mysql/mysql ]; then
    mysql_install_db --user=mysql --datadir=/var/lib/mysql >/dev/null
fi
mysqld_safe --user=mysql >/var/log/mysqld-safe.log 2>&1 &
fx_wait_for mariadb 60 mysqladmin ping
mysql -u root <<'SQL'
CREATE DATABASE IF NOT EXISTS slurm_acct_db;
CREATE USER IF NOT EXISTS 'slurm'@'localhost' IDENTIFIED BY 'fixture-local-only';
GRANT ALL ON slurm_acct_db.* TO 'slurm'@'localhost';
FLUSH PRIVILEGES;
SQL

install -d -o slurm -g slurm /var/spool/slurmctld /var/log/slurm
install -d /var/spool/slurmd
/usr/sbin/slurmdbd
fx_wait_for slurmdbd 60 sacctmgr -n list cluster
sacctmgr -i add cluster fixture >/dev/null 2>&1 || true
sacctmgr -i add account fixture Cluster=fixture Description=fixture >/dev/null 2>&1 || true
sacctmgr -i add user relay Account=fixture >/dev/null 2>&1 || true

/usr/sbin/slurmctld
fx_wait_for slurmctld 60 scontrol ping
/usr/sbin/slurmd
fx_wait_for "node idle" 60 sh -c 'sinfo -h -N -o %T | grep -q idle'
fx_log "slurm ready"

fx_exec_sshd
