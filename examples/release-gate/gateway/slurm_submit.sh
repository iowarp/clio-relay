#!/usr/bin/env bash
set -euo pipefail

service_script="$1"
service_port="$2"
log_dir="$3"
job_name="$4"
health_nonce="$5"
if [[ ! "$service_port" =~ ^[0-9]+$ ]] || ((service_port < 1 || service_port > 65535)); then
  echo "invalid service port" >&2
  exit 2
fi
if [[ ! "$job_name" =~ ^[A-Za-z0-9_.-]+$ ]]; then
  echo "invalid job name" >&2
  exit 2
fi
if [[ ! "$health_nonce" =~ ^[0-9a-f]{64}$ ]]; then
  echo "invalid health nonce" >&2
  exit 2
fi
mkdir -p "$log_dir"
printf -v wrapped 'python3 %q --port %q --health-nonce %q --lifetime-seconds 900' \
  "$service_script" "$service_port" "$health_nonce"
job_id="$(sbatch --parsable \
  --job-name "$job_name" \
  --time 00:20:00 \
  --output "$log_dir/gateway-%j.out" \
  --error "$log_dir/gateway-%j.err" \
  --wrap "$wrapped")"
job_id="${job_id%%;*}"
if [[ ! "$job_id" =~ ^[0-9]+$ ]]; then
  echo "sbatch returned a non-numeric job id" >&2
  exit 1
fi
printf '{"scheduler_job_id":"%s"}\n' "$job_id"
