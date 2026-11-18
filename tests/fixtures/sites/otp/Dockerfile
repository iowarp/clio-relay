# SSH host whose sign-in asks for a one-time code after the key.
ARG BASE_IMAGE=clio-relay-rework-fx/ssh:latest
FROM ${BASE_IMAGE}
LABEL clio-relay-rework="1" clio-relay-rework.owner="w2-fixtures"

RUN apt-get update \
    && apt-get install -y --no-install-recommends libpam-google-authenticator \
    && rm -rf /var/lib/apt/lists/* \
    && sed -i 's|^@include common-auth|auth required pam_google_authenticator.so|' /etc/pam.d/sshd \
    && grep -q '^auth required pam_google_authenticator.so' /etc/pam.d/sshd

COPY otp/otp.conf /etc/ssh/sshd_config.d/otp.conf
COPY otp/entrypoint.sh /opt/clio-fixtures/entrypoint.sh
RUN chmod 0755 /opt/clio-fixtures/entrypoint.sh

EXPOSE 22
