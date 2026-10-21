#!/usr/bin/env bash
# Start / stop / inspect an N-node exo cluster in rootless Podman on one Linux host.
#
#   ./cluster.sh up [N]        start N nodes (default 3) on an IPv6-enabled podman network;
#                              nodes already running are left alone, stopped ones restarted
#   ./cluster.sh down          stop and remove them (volumes with downloaded models are kept)
#   ./cluster.sh ip <i>        print node i's IPv4 on the cluster network
#   ./cluster.sh logs <i>      follow node i's log
#   ./cluster.sh kill  <i>     SIGKILL node i's container   (clean crash: peers see a reset)
#   ./cluster.sh pause <i>     SIGSTOP node i's container   (silent hang: peers see nothing)
#   ./cluster.sh cut   <i>     disconnect node i from the network (partition)
#   ./cluster.sh heal  <i>     reconnect node i
#   ./cluster.sh resume <i>    SIGCONT node i
#
# Why containers: exo discovers peers by IPv6 link-local multicast (ff12::e0a1:de89,
# UDP 52413) and probes peers at <peer ip>:<own api port>, so every node needs its own IP
# on a shared L2 and the same ports. Host networking with different ports does not form a
# topology and placement refuses. One podman network with --ipv6 gives exactly that.
set -euo pipefail

NET=exo-net
IMG=exo-node
NS=${EXO_NAMESPACE:-exp4}
N_DEFAULT=3
# Optional: one CPU set per node, so nodes behave like separate, equal machines, e.g.
# EXO_CPUSETS="0-3 4-7 8-11" on an i5-14600K (P-core threads 0-11, E-cores 12-19). Off by
# default: rootless podman needs the cpuset cgroup controller delegated by systemd, which
# takes a re-login. Pin vLLM to match (VLLM_CPU_OMP_THREADS_BIND="0-3|4-7") or neither.
read -r -a CPUSETS <<< "${EXO_CPUSETS-}"

cmd=${1:-}; arg=${2:-}

ensure_net() {
  podman network exists "$NET" 2>/dev/null || podman network create --ipv6 "$NET" >/dev/null
}

# Rootless podman can pin CPUs only if systemd delegates the cpuset controller to the user.
ensure_cpuset() {
  local f="/sys/fs/cgroup/user.slice/user-$(id -u).slice/user@$(id -u).service/cgroup.controllers"
  if [ -f "$f" ] && ! grep -qw cpuset "$f"; then
    cat >&2 <<'MSG'
cpuset controller not delegated to your user, so podman cannot pin CPUs. Once, as root:
  mkdir -p /etc/systemd/system/user@.service.d
  printf '[Service]\nDelegate=cpu cpuset io memory pids\n' > /etc/systemd/system/user@.service.d/delegate.conf
  systemctl daemon-reload
then log out and back in. (Or run with EXO_CPUSETS="" to not pin.)
MSG
    exit 1
  fi
}

case "$cmd" in
  up)
    n=${arg:-$N_DEFAULT}
    if [ "${#CPUSETS[@]}" -gt 0 ]; then
      [ "$n" -le "${#CPUSETS[@]}" ] || { echo "EXO_CPUSETS has ${#CPUSETS[@]} sets for $n nodes" >&2; exit 1; }
      ensure_cpuset
    fi
    ensure_net
    for i in $(seq 0 $((n-1))); do
      state=$(podman inspect -f '{{.State.Status}}' "exo-$i" 2>/dev/null || true)
      if [ "$state" = running ] || [ "$state" = paused ]; then
        echo "exo-$i already $state"; continue
      elif [ -n "$state" ]; then  # killed or exited: restart the same container (fresh NodeId)
        podman start "exo-$i" >/dev/null; echo "restarted exo-$i"; continue
      fi
      podman volume exists "exo-data-$i" 2>/dev/null || podman volume create "exo-data-$i" >/dev/null
      # /tmp on tmpfs: MLX's CPU backend JIT-compiles kernels to /tmp/mlx/<ver>/cpu with
      # names up to 252 bytes, which the overlay /tmp can refuse ("File name too long")
      # depending on the host filesystem. exec, since MLX dlopens what it compiles there.
      podman run -d --name "exo-$i" --network "$NET" \
        --tmpfs /tmp:rw,exec,mode=1777 \
        $( [ "${#CPUSETS[@]}" -gt 0 ] && echo "--cpuset-cpus ${CPUSETS[$i]}" ) \
        -v "exo-data-$i:/data" \
        -e EXO_ZENOH_NAMESPACE="$NS" \
        $( [ "$i" = 0 ] && echo "-p 52415:52415" ) \
        "$IMG" >/dev/null
      echo "started exo-$i${CPUSETS[$i]:+ on CPUs ${CPUSETS[$i]}}"
    done
    echo "API of node 0 on http://localhost:52415 ; others via ./cluster.sh ip <i>"
    ;;
  down)
    for c in $(podman ps -a --format '{{.Names}}' | grep '^exo-' || true); do podman rm -f "$c" >/dev/null; done
    echo "cluster removed (volumes kept; podman volume rm exo-data-* to drop models)"
    ;;
  ip)     podman inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "exo-$arg" ;;
  logs)   podman logs -f "exo-$arg" ;;
  kill)   date +%s.%N; podman kill -s KILL "exo-$arg" >/dev/null ;;
  pause)  date +%s.%N; podman pause "exo-$arg" >/dev/null ;;
  resume) date +%s.%N; podman unpause "exo-$arg" >/dev/null ;;
  cut)    date +%s.%N; podman network disconnect "$NET" "exo-$arg" ;;
  heal)   date +%s.%N; podman network connect "$NET" "exo-$arg" ;;
  *) sed -n 2,18p "$0"; exit 1 ;;
esac
