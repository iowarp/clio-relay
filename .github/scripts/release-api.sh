#!/usr/bin/env bash

# Numeric-ID GitHub release helpers for draft-safe protected workflows.
# The caller supplies GH_TOKEN and REPOSITORY through the environment.

relay_release_resolve() {
  local tag_name="$1" draft_state="$2" output_path="$3"
  shift 3
  python src/clio_relay/ci_validation.py resolve-live-release \
    --repository "$REPOSITORY" \
    --tag "$tag_name" \
    --draft "$draft_state" \
    --output "$output_path" \
    "$@"
}

relay_release_resolve_eventually() {
  local tag_name="$1" draft_state="$2" output_path="$3" attempt
  for attempt in $(seq 1 10); do
    relay_release_resolve "$tag_name" "$draft_state" "$output_path" --allow-absent
    if jq -e '. != null' "$output_path" >/dev/null; then
      return 0
    fi
    if [ "$attempt" -eq 10 ]; then
      echo "draft release did not become visible after bounded retries" >&2
      return 1
    fi
    sleep 3
  done
}

relay_release_complete_assets() {
  local release_json="$1" output_path="$2"
  local release_id page_one page_two refreshed
  release_id="$(jq -r .id "$release_json")"
  [[ "$release_id" =~ ^[1-9][0-9]*$ ]]
  page_one="$(mktemp)"
  page_two="$(mktemp)"
  refreshed="$(mktemp)"
  trap 'rm -f "$page_one" "$page_two" "$refreshed"' RETURN
  gh api -H 'X-GitHub-Api-Version: 2026-03-10' \
    "repos/$REPOSITORY/releases/$release_id" >"$refreshed"
  jq -e --slurpfile expected "$release_json" '
    .id == $expected[0].id and
    .tag_name == $expected[0].tag_name and
    .target_commitish == $expected[0].target_commitish and
    .draft == $expected[0].draft and
    .prerelease == $expected[0].prerelease and
    .immutable == $expected[0].immutable
  ' "$refreshed" >/dev/null
  gh api -H 'X-GitHub-Api-Version: 2026-03-10' \
    "repos/$REPOSITORY/releases/$release_id/assets?per_page=100&page=1" >"$page_one"
  gh api -H 'X-GitHub-Api-Version: 2026-03-10' \
    "repos/$REPOSITORY/releases/$release_id/assets?per_page=100&page=2" >"$page_two"
  jq -e 'type == "array" and length <= 100' "$page_one" >/dev/null
  jq -e 'type == "array" and length == 0' "$page_two" >/dev/null
  jq --slurpfile assets "$page_one" '.assets = $assets[0]' "$refreshed" >"$output_path"
  trap - RETURN
  rm -f "$page_one" "$page_two" "$refreshed"
}

relay_release_asset_id() {
  local release_json="$1" asset_name="$2"
  local -a asset_ids=()
  [[ "$asset_name" =~ ^[A-Za-z0-9][A-Za-z0-9._+-]*$ ]]
  mapfile -t asset_ids < <(
    jq -r --arg name "$asset_name" '.assets[] | select(.name == $name) | .id' \
      "$release_json"
  )
  if [ "${#asset_ids[@]}" -eq 0 ]; then
    return 1
  fi
  if [ "${#asset_ids[@]}" -ne 1 ] || [[ ! "${asset_ids[0]}" =~ ^[1-9][0-9]*$ ]]; then
    echo "release asset identity is ambiguous or invalid: $asset_name" >&2
    return 2
  fi
  printf '%s\n' "${asset_ids[0]}"
}

relay_release_download_exact() {
  local release_json="$1" asset_name="$2" destination="$3"
  local asset_id temporary
  asset_id="$(relay_release_asset_id "$release_json" "$asset_name")"
  mkdir -p "$(dirname "$destination")"
  temporary="$(mktemp "${destination}.XXXXXX")"
  trap 'rm -f "$temporary"' RETURN
  gh api \
    -H 'Accept: application/octet-stream' \
    -H 'X-GitHub-Api-Version: 2026-03-10' \
    "repos/$REPOSITORY/releases/assets/$asset_id" >"$temporary"
  test -s "$temporary"
  mv -f "$temporary" "$destination"
  trap - RETURN
}

relay_release_download_pattern() {
  local release_json="$1" pattern="$2" destination_dir="$3"
  local asset_id asset_name count=0
  mkdir -p "$destination_dir"
  while IFS=$'\t' read -r asset_id asset_name; do
    [[ "$asset_name" =~ ^[A-Za-z0-9][A-Za-z0-9._+-]*$ ]]
    # shellcheck disable=SC2053  # The caller-supplied value is an intentional glob.
    if [[ "$asset_name" != $pattern ]]; then
      continue
    fi
    relay_release_download_exact "$release_json" "$asset_name" \
      "$destination_dir/$asset_name"
    count=$((count + 1))
  done < <(jq -r '.assets[] | [.id, .name] | @tsv' "$release_json")
  test "$count" -gt 0
}

relay_release_upload_exact() {
  local release_json="$1" subject="$2"
  local release_id asset_name encoded_name response http_status
  release_id="$(jq -r .id "$release_json")"
  [[ "$release_id" =~ ^[1-9][0-9]*$ ]]
  jq -e '.draft == true and (.immutable // false) == false' "$release_json" >/dev/null
  test -f "$subject"
  test ! -L "$subject"
  asset_name="$(basename "$subject")"
  [[ "$asset_name" =~ ^[A-Za-z0-9][A-Za-z0-9._+-]*$ ]]
  encoded_name="$(jq -nr --arg name "$asset_name" '$name | @uri')"
  response="$(mktemp)"
  trap 'rm -f "$response"' RETURN
  http_status="$(
    curl --silent --show-error \
      --location \
      --proto '=https' \
      --tlsv1.2 \
      --connect-timeout 30 \
      --max-time 600 \
      --output "$response" \
      --write-out '%{http_code}' \
      --request POST \
      --header 'Accept: application/vnd.github+json' \
      --header "Authorization: Bearer $GH_TOKEN" \
      --header 'X-GitHub-Api-Version: 2026-03-10' \
      --header 'Content-Type: application/octet-stream' \
      --data-binary "@$subject" \
      "https://uploads.github.com/repos/$REPOSITORY/releases/$release_id/assets?name=$encoded_name"
  )"
  if [ "$http_status" != 201 ]; then
    cat "$response" >&2
    return 1
  fi
  test "$(jq -r .name "$response")" = "$asset_name"
  test "$(jq -r .state "$response")" = uploaded
  trap - RETURN
  rm -f "$response"
}

relay_release_patch() {
  local release_json="$1" payload="$2" output_path="$3"
  local release_id
  release_id="$(jq -r .id "$release_json")"
  [[ "$release_id" =~ ^[1-9][0-9]*$ ]]
  jq -e '.draft == true and (.immutable // false) == false' "$release_json" >/dev/null
  gh api --method PATCH \
    -H 'X-GitHub-Api-Version: 2026-03-10' \
    "repos/$REPOSITORY/releases/$release_id" \
    --input "$payload" >"$output_path"
  test "$(jq -r .id "$output_path")" = "$release_id"
}
