#!/usr/bin/env bash
# Genera carga CPU-bound contra /burn con `hey` para saturar la(s) tarea(s)
# de Fargate Spot, mientras (en otra terminal) corres probe/probe.py para medir
# cómo se degrada la disponibilidad/latencia.
#
# Instalar hey:
#   Fedora:  sudo dnf install hey        (o descarga el binario de github.com/rakyll/hey)
#   Go:      go install github.com/rakyll/hey@latest
#
# Uso:
#   ./load/hey_burn.sh <URL_BASE> [CONCURRENCIA] [DURACION] [MS_POR_REQUEST]
# Ejemplo:
#   ./load/hey_burn.sh http://alb-fargate-spot-1875440544.us-east-2.elb.amazonaws.com 50 2m 200

set -euo pipefail

URL="${1:?Falta la URL base, p.ej. http://<alb-dns>}"
CONCURRENCY="${2:-50}"
DURATION="${3:-2m}"
BURN_MS="${4:-200}"

TARGET="${URL%/}/burn?ms=${BURN_MS}"

echo "Cargando ${TARGET}"
echo "  concurrencia=${CONCURRENCY}  duracion=${DURATION}  ms/req=${BURN_MS}"
echo

# -z: duracion  -c: concurrentes  -disable-keepalive para forzar mas conexiones
exec hey -z "${DURATION}" -c "${CONCURRENCY}" "${TARGET}"
