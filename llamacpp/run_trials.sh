#!/usr/bin/env bash
# Run llama.cpp RPC failure trials unattended.
#
#   llamacpp/run_trials.sh MODE N [PROMPT_TOKENS]   one group: N complete trials of MODE
#                                                   (killproc | kill | pause | cut) at that
#                                                   prompt length (default 33)
#   llamacpp/run_trials.sh all                      the whole plan from the README, in order:
#                                                   killproc ×KILLPROC_N (10), kill / pause / cut
#                                                   ×OTHER_N (5), then killproc at each length in
#                                                   SWEEP ("512 2048 6144") ×SWEEP_N (2)
#
# A trial counts when its record has a first token after resubmission (C is defined).
# Before an attempt the cluster is recreated if the main host is not healthy or the previous
# attempt failed. The loop gives up on a group after N + FAIL_ALLOWANCE (4) attempts.
# kill / pause / cut are predicted to go undetected: each of those trials runs the harness
# stall timeout (600 s) plus restart, so ~11 min each; killproc trials take ~1-2 min.
# Example, from the repo root:
#   nohup llamacpp/run_trials.sh all > llamacpp/logs/run_trials.out 2>&1 &
#   tail -f llamacpp/logs/run_trials.out
set -u
HERE=$(cd "$(dirname "$0")" && pwd)
OUT=${TRIALS_OUT:-$HERE/trials.jsonl}
NODES=${NODES:-2}
FAIL_ALLOWANCE=${FAIL_ALLOWANCE:-4}
KILLPROC_N=${KILLPROC_N:-10}; OTHER_N=${OTHER_N:-5}
SWEEP=${SWEEP:-"512 2048 6144"}; SWEEP_N=${SWEEP_N:-2}   # 8192 does not fit the main host's -c 8192

stamp() { LC_ALL=C date +%Y-%m-%dT%H:%M:%S; }
log() { echo "[$(stamp)] $*"; }

count() {  # complete trials of MODE at PROMPT_TOKENS in OUT
  python3 - "$OUT" "$1" "$2" <<'PY'
import json, sys
path, mode, ptok = sys.argv[1], sys.argv[2], int(sys.argv[3])
n = 0
try:
    for line in open(path):
        if not line.strip(): continue
        r = json.loads(line)
        if r.get("mode") != mode or r.get("prompt_tokens_requested", 33) != ptok \
           or r.get("t_resubmit_first_token") is None or not r.get("t_fail"):
            continue
        if mode in ("kill", "killproc") and r.get("rpc_state_after") != "exited":
            continue  # injection did not take (records before cluster.sh killed from the host)
        n += 1
except FileNotFoundError:
    pass
print(n)
PY
}

healthy() { curl -sf -m 2 "http://localhost:${LLAMA_API_PORT:-8080}/health" >/dev/null 2>&1; }

wait_for_running_trial() {
  while pgrep -f "python3 .*llamacpp/trial\.py" >/dev/null 2>&1; do
    log "a llamacpp trial.py is still running; waiting"; sleep 30
  done
}

recreate() {
  log "recreating the cluster ($NODES rpc hosts)"
  "$HERE/cluster.sh" down >/dev/null 2>&1
  "$HERE/cluster.sh" up "$NODES" || return 1
  for _ in $(seq 180); do healthy && return 0; sleep 5; done
  log "main host not healthy 15 min after up"; return 1
}

run_group() {  # MODE N PROMPT_TOKENS
  local mode=$1 want=$2 ptok=$3 attempts=0 fails=0 have before
  case "$mode" in killproc|kill|pause|cut) ;; *) log "bad mode: $mode"; return 2;; esac
  while :; do
    have=$(count "$mode" "$ptok")
    log "$have/$want complete $mode trials at $ptok prompt tokens in $OUT"
    [ "$have" -ge "$want" ] && { log "group $mode@$ptok done"; return 0; }
    if [ "$attempts" -ge $((want + FAIL_ALLOWANCE)) ]; then
      log "giving up on $mode@$ptok after $attempts attempts; check the cluster"; return 1
    fi
    wait_for_running_trial
    attempts=$((attempts + 1))
    if [ "$fails" -gt 0 ] || ! healthy; then
      recreate || { log "cluster.sh failed; retrying in 60 s"; sleep 60; continue; }
    fi
    before=$have
    log "attempt $attempts: trial.py --mode $mode --prompt-tokens $ptok --out $OUT"
    python3 "$HERE/trial.py" --mode "$mode" --prompt-tokens "$ptok" --out "$OUT" \
      || log "trial exited with status $?"
    have=$(count "$mode" "$ptok")
    if [ "$have" -gt "$before" ]; then fails=0; else fails=1; log "no complete record from this attempt"; fi
  done
}

trap 'log "interrupted"; exit 130' INT TERM
case "${1:-}" in
  all)
    healthy || recreate || exit 1
    log "plumbing check (--mode none)"
    python3 "$HERE/trial.py" --mode none --out "$HERE/smoke.jsonl" || { log "plumbing check failed; not starting the plan"; exit 1; }
    run_group killproc "$KILLPROC_N" 33 || exit 1
    for m in kill pause cut; do run_group "$m" "$OTHER_N" 33 || exit 1; done
    for p in $SWEEP; do run_group killproc "$SWEEP_N" "$p" || exit 1; done
    log "plan complete"; "$HERE/cluster.sh" down >/dev/null 2>&1; log "cluster removed" ;;
  killproc|kill|pause|cut)
    healthy || recreate || exit 1
    run_group "$1" "${2:?how many complete trials}" "${3:-33}" ;;
  *) sed -n 2,19p "$0"; exit 1 ;;
esac
