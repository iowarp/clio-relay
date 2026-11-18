#!/bin/bash
# SSH host that is also a one-node OpenPBS server, mom and scheduler.
# shellcheck source=SCRIPTDIR/../common/site-lib.sh
source /opt/clio-fixtures/site-lib.sh

if [ "$#" -gt 0 ]; then
    exec "$@"
fi

fx_prepare_ssh

host=$(hostname -s)
sed -i \
    -e "s/^PBS_SERVER=.*/PBS_SERVER=${host}/" \
    -e "s/^PBS_START_MOM=.*/PBS_START_MOM=1/" \
    /etc/pbs.conf
install -d /var/spool/pbs/mom_priv
cat >/var/spool/pbs/mom_priv/config <<EOF
\$clienthost ${host}
\$usecp *:/home /home
EOF

/opt/pbs/libexec/pbs_init.d start
fx_wait_for "pbs server" 120 qstat -B

if ! pbsnodes "$host" >/dev/null 2>&1; then
    qmgr -c "create node ${host}"
fi
qmgr -c "set server flatuid = true"
qmgr -c "set server job_history_enable = true"
qmgr -c "set server job_history_duration = 24:00:00"
qmgr -c "set server scheduling = true"
if ! qstat -Q debug >/dev/null 2>&1; then
    qmgr -c "create queue debug queue_type = execution"
    qmgr -c "set queue debug enabled = true"
    qmgr -c "set queue debug started = true"
    qmgr -c "set queue debug resources_max.walltime = 00:30:00"
fi
fx_wait_for "node free" 120 sh -c "pbsnodes $host | grep -q 'state = free'"
fx_log "pbs ready"

fx_exec_sshd
