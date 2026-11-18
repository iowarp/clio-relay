#!/bin/bash
# Plain SSH host with no scheduler. With arguments, runs them instead, so the
# same image also hosts the standalone secret service and counter MCP server.
# shellcheck source=SCRIPTDIR/../common/site-lib.sh
source /opt/clio-fixtures/site-lib.sh

if [ "$#" -gt 0 ]; then
    exec "$@"
fi

fx_prepare_ssh
fx_exec_sshd
