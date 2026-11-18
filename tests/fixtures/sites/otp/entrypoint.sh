#!/bin/bash
# SSH host whose sign-in asks for a key and then a one-time code (TOTP), the way
# multi-factor HPC sites do. The TOTP secret comes from /fixture-secrets.
source /opt/clio-fixtures/site-lib.sh

if [ "$#" -gt 0 ]; then
    exec "$@"
fi

fx_prepare_ssh

otp_file=/home/relay/.google_authenticator
{
    tr -d '[:space:]' <"$FX_SECRETS/otp_totp_secret"
    printf '\n" WINDOW_SIZE 3\n" TOTP_AUTH\n'
} >"$otp_file"
chown relay:relay "$otp_file"
chmod 0400 "$otp_file"

fx_exec_sshd
