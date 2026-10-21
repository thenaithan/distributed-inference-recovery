#!/usr/bin/env bash
# Start / stop / inspect an N-node exo cluster in rootless Podman on one Linux host.
#
#   ./cluster.sh up [N]        start N nodes (default 3) on an IPv6-enabled podman network
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

cmd=${1:-}; arg=${2:-}

ensure_net() {
  podman network exists "$NET" 2>/dev/null || podman network create --ipv6 "$NET" >/dev/null
}

case "$cmd" in
  up)
    n=${arg:-$N_DEFAULT}
    ensure_net
    for i in $(seq 0 $((n-1))); do
      podman volume exists "exo-data-$i" 2>/dev/null || podman volume create "exo-data-$i" >/dev/null
      # /tmp on tmpfs: MLX's CPU backend JIT-compiles kernels to /tmp/mlx/<ver>/cpu with
      # names up to 252 bytes, which the overlay /tmp can refuse ("File name too long")
      # depending on the host filesystem. exec, since MLX dlopens what it compiles there.
      podman run -d --name "exo-$i" --network "$NET" \
        --tmpfs /tmp:rw,exec,mode=1777 \
        -v "exo-data-$i:/data" \
        -e EXO_ZENOH_NAMESPACE="$NS" \
        $( [ "$i" = 0 ] && echo "-p 52415:52415" ) \
        "$IMG" >/dev/null
      echo "started exo-$i"
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
  *) sed -n 2,16p "$0"; exit 1 ;;
esac
