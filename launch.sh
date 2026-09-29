#!/usr/bin/env bash
set -euo pipefail

usage() {
    echo "Usage: $(basename -- "$0") CONF_FILE" >&2
}

if [[ $# -ne 1 ]]; then
    usage
    exit 2
fi

cd -- "$(dirname -- "${BASH_SOURCE[0]}")"

conf_file=$1
if [[ ! -f "$conf_file" ]]; then
    echo "Configuration file does not exist: ${conf_file}" >&2
    exit 1
fi

mkdir -p data/logs

conf_name=$(basename -- "$conf_file")
run_name=${conf_name%.conf}
pid_file="data/logs/${run_name}.producer.pid"
stdout_log="data/logs/${run_name}.producer.stdout.log"
timing_log="data/logs/${run_name}.producer.jsonl"

if [[ -s "$pid_file" ]]; then
    old_pid=$(<"$pid_file")
    if [[ "$old_pid" =~ ^[0-9]+$ ]] && kill -0 "$old_pid" 2>/dev/null; then
        echo "Producer process $old_pid is already running; refusing to start another." >&2
        exit 1
    fi
fi

if [[ -e "$timing_log" || -e "$stdout_log" ]]; then
    echo "Run logs for ${run_name} already exist; refusing to append to them." >&2
    exit 1
fi

# Step 0 is immediate; steps 1 through 28799 follow at a three-second cadence.
# The final snapshot is therefore scheduled at 23:59:57.
nohup python3 mockProducer.py \
    "$conf_file" 512 512 28800 \
    --private-key keys/mockkey \
    --timing-log "$timing_log" \
    --buffer-seconds 600 \
    >"$stdout_log" 2>&1 < /dev/null &
producer_pid=$!
printf '%s\n' "$producer_pid" > "$pid_file"

echo "Started ${run_name} as PID ${producer_pid}."
echo "Follow progress with: tail -f ${stdout_log}"
echo "Inspect timing events with: tail -f ${timing_log}"
