#!/usr/bin/env bash

# Authenticate one git fetch through the job token without persisting credentials.
relay_authenticated_git_fetch() {
  local encoded_credential
  test -n "${GH_TOKEN:-}"
  encoded_credential="$(printf 'x-access-token:%s' "$GH_TOKEN" | base64 | tr -d '\r\n')"
  GIT_CONFIG_COUNT=1 \
    GIT_CONFIG_KEY_0='http.https://github.com/.extraheader' \
    GIT_CONFIG_VALUE_0="AUTHORIZATION: basic $encoded_credential" \
    GIT_TERMINAL_PROMPT=0 \
    git fetch "$@"
  unset encoded_credential
}
