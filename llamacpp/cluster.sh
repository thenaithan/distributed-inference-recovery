#!/usr/bin/env bash
# N ggml-rpc-server containers plus one llama-server container on a podman network.
#
#   ./cluster.sh up [N]        start N rpc hosts (default 2) and the main host
#   ./cluster.sh main           (re)start only the main host (llama-server); used after it aborts
#   ./cluster.sh down          remove everything (the model volume is kept)
#   ./cluster.sh logs <name>   follow logs: rpc-0, rpc-1, ..., main
#   ./cluster.sh kill  <i>     SIGKILL rpc-i's container       (crash)
#   ./cluster.sh pause <i>     SIGSTOP rpc-i                   (hang)
#   ./cluster.sh resume <i>    SIGCONT rpc-i
#   ./cluster.sh cut   <i>     disconnect rpc-i from the network (partition)
#   ./cluster.sh heal  <i>     reconnect rpc-i
#   ./cluster.sh killproc <i>  SIGKILL the rpc-server PROCESS (container PID 1, signalled from the
#                              host): the kernel then resets the TCP connection (the crash case in
#                              which the main host is told, as opposed to a vanished namespace)
#
# Layers are split evenly across the rpc hosts (-ts 1,1,... with the main host's own CPU
# weighted 0), so every rpc host holds a pipeline slice and the main host runs none.
set -euo pipefail
NET=llama-net; IMG=llamacpp-rpc; VOL=llama-models
MODEL=${LLAMA_MODEL:-ggml-org/Qwen3-0.6B-GGUF}           # -hf repo; Q8_0 by default
N_DEFAULT=2; RPC_PORT=50052; API_PORT=${LLAMA_API_PORT:-8080}
THREADS=${LLAMA_THREADS:-4}
cmd=${1:-}; arg=${2:-}

ensure() { podman network exists "$NET" 2>/dev/null || podman network create "$NET" >/dev/null
           podman volume exists "$VOL" 2>/dev/null || podman volume create "$VOL" >/dev/null; }
n_rpc() { podman ps -a --format '{{.Names}}' | grep -c '^rpc-' || true; }

start_main() {
  n=$(n_rpc)
  rpc=""; ts=""
  for i in $(seq 0 $((n-1))); do rpc="${rpc:+$rpc,}rpc-$i:$RPC_PORT"; ts="${ts:+$ts,}1"; done
  podman rm -f main >/dev/null 2>&1 || true
  date +%s.%N
  podman run -d --name main --network "$NET" -v "$VOL:/models" -p "$API_PORT:8080" "$IMG" \
    llama-server -hf "$MODEL" --host 0.0.0.0 --port 8080 -ngl 99 --rpc "$rpc" -ts "$ts,0" \
    -t "$THREADS" -c 8192 --no-warmup >/dev/null
}

case "$cmd" in
  up)
    n=${arg:-$N_DEFAULT}; ensure
    for i in $(seq 0 $((n-1))); do
      podman ps -a --format '{{.Names}}' | grep -qx "rpc-$i" && podman rm -f "rpc-$i" >/dev/null
      podman run -d --name "rpc-$i" --network "$NET" -v "$VOL:/models" "$IMG" \
        ggml-rpc-server --host 0.0.0.0 -p $RPC_PORT -t "$THREADS" >/dev/null
      echo "started rpc-$i"
    done
    start_main >/dev/null; echo "started main (llama-server) on http://localhost:$API_PORT" ;;
  main)     start_main ;;
  down)     for c in main $(podman ps -a --format '{{.Names}}' | grep '^rpc-' || true); do podman rm -f "$c" >/dev/null 2>&1 || true; done; echo removed ;;
  logs)     podman logs -f "$arg" ;;
  kill)     date +%s.%N; podman kill -s KILL "rpc-$arg" >/dev/null ;;
  # from the host: the kernel ignores SIGKILL sent to a PID namespace's init from inside it
  killproc) date +%s.%N; kill -9 "$(podman inspect -f '{{.State.Pid}}' "rpc-$arg")" ;;
  pause)    date +%s.%N; podman pause "rpc-$arg" >/dev/null ;;
  resume)   date +%s.%N; podman unpause "rpc-$arg" >/dev/null ;;
  cut)      date +%s.%N; podman network disconnect "$NET" "rpc-$arg" ;;
  heal)     date +%s.%N; podman network connect "$NET" "rpc-$arg" ;;
  *) sed -n 2,19p "$0"; exit 1 ;;
esac
