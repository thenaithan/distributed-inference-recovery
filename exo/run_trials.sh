#!/usr/bin/env bash
# Run exo failure trials unattended until a group has enough complete results.
#
#   exo/run_trials.sh MODE WHO N [NODES]
#     MODE   kill | pause | cut
#     WHO    worker | master          (--fail-worker / --fail-master)
#     N      complete trials wanted in exo/trials.jsonl for that mode and role
#     NODES  cluster size (default 3)
#
# Each round: wait for any trial.py already running to finish, recreate the cluster
# (cluster.sh down && up, which also clears a wedged placement), run one trial, recount.
# A trial whose re-placement wedged leaves a record without C and is simply retried, so
# the loop stops after N + WEDGE_ALLOWANCE attempts if something keeps failing.
# Example, from the repo root, leaving it running in the background:
#   nohup exo/run_trials.sh cut worker 10 > exo/logs/run_trials.out 2>&1 &
set -u
HERE=$(cd "$(dirname "$0")" && pwd)
MODE=${1:?mode: kill|pause|cut}; WHO=${2:?who: worker|master}; WANT=${3:?how many complete trials}; NODES=${4:-3}
OUT=${TRIALS_OUT:-$HERE/trials.jsonl}
WEDGE_ALLOWANCE=${WEDGE_ALLOWANCE:-6}
case "$MODE" in kill|pause|cut) ;; *) echo "bad mode: $MODE" >&2; exit 2;; esac
case "$WHO" in worker) FLAG=--fail-worker;; master) FLAG=--fail-master;; *) echo "bad who: $WHO" >&2; exit 2;; esac

stamp() { LC_ALL=C date +%Y-%m-%dT%H:%M:%S; }
log() { echo "[$(stamp)] $*"; }

# complete trials of this mode and role: the record has a first token after resubmission (C is defined)
count() {
  python3 - "$OUT" "$MODE" "$WHO" <<'PY'
import json, sys
path, mode, who = sys.argv[1:]
n = 0
try:
    for line in open(path):
        if not line.strip(): continue
        r = json.loads(line)
        if r.get("mode") != mode or r.get("t_resubmit_first_token") is None: continue
        is_master = r.get("master_node") is not None and r.get("master_node") == r.get("fail_node")
        if (who == "master") == is_master: n += 1
except FileNotFoundError:
    pass
print(n)
PY
}

wait_for_running_trial() {
  while pgrep -f "python3 .*trial\.py" >/dev/null 2>&1; do
    log "a trial.py is still running; waiting"; sleep 30
  done
}

trap 'log "interrupted"; exit 130' INT TERM
attempts=0
while :; do
  have=$(count)
  log "$have/$WANT complete $MODE-$WHO trials in $OUT"
  [ "$have" -ge "$WANT" ] && { log "done"; exit 0; }
  if [ "$attempts" -ge $((WANT + WEDGE_ALLOWANCE)) ]; then
    log "giving up after $attempts attempts this run; check the cluster"; exit 1
  fi
  wait_for_running_trial
  have=$(count); [ "$have" -ge "$WANT" ] && { log "done"; exit 0; }
  attempts=$((attempts + 1))
  log "attempt $attempts: recreating the cluster"
  "$HERE/cluster.sh" down && "$HERE/cluster.sh" up "$NODES" || { log "cluster.sh failed; retrying in 60 s"; sleep 60; continue; }
  log "running: trial.py --nodes $NODES $FLAG --mode $MODE --out $OUT"
  python3 "$HERE/trial.py" --nodes "$NODES" "$FLAG" --mode "$MODE" --out "$OUT" || log "trial exited with status $? (a wedged re-placement is retried)"
done
