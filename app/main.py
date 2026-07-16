"""API de prueba de resiliencia para Fargate Spot.

Expone un endpoint que recibe un `id`, mide cuánto tarda en atenderlo y
devuelve, junto con metadata de la tarea de ECS, para que el probe pueda
detectar reemplazos de tarea (interrupciones de Spot) al ver que cambia el
`task_id`.

También maneja SIGTERM (la señal que Fargate envía 2 min antes de interrumpir
una tarea Spot) para hacer un apagado ordenado: eso es, precisamente, la pieza
de resiliencia que este test valida.
"""

from __future__ import annotations

import asyncio
import os
import signal
import socket
import time
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI
from fastapi.responses import JSONResponse

# Endpoint de metadata que ECS inyecta en cada contenedor (task metadata v4).
# En local no existe, así que los campos quedan en None.
ECS_METADATA_URI = os.getenv("ECS_CONTAINER_METADATA_URI_V4")

# Segundos que la tarea sigue viva tras recibir SIGTERM antes de apagarse:
# ventana para que el ALB la saque de rotación (deregistration delay) y para
# terminar las peticiones en vuelo. Debe ser < stopTimeout de la task de ECS.
DRAIN_SECONDS = float(os.getenv("DRAIN_SECONDS", "5"))

# Caché de la metadata de la tarea: no cambia durante la vida del contenedor.
_task_meta: dict[str, str | None] = {
    "task_id": None,
    "task_arn": None,
    "az": None,
    "container_id": None,
    "hostname": socket.gethostname(),
}

# Flag que se activa al recibir SIGTERM. Mientras esté en True, /health
# responde 503 para que el balanceador saque esta tarea de rotación antes
# de que Spot la mate.
_draining = False


async def _load_ecs_metadata() -> None:
    """Consulta el endpoint de metadata de ECS y cachea task_id / AZ."""
    if not ECS_METADATA_URI:
        return
    try:
        async with httpx.AsyncClient(timeout=2.0) as client:
            task = (await client.get(f"{ECS_METADATA_URI}/task")).json()
            container = (await client.get(ECS_METADATA_URI)).json()
        task_arn = task.get("TaskARN", "")
        _task_meta.update(
            task_id=task_arn.rsplit("/", 1)[-1] or None,
            task_arn=task_arn or None,
            az=task.get("AvailabilityZone"),
            container_id=container.get("DockerId"),
        )
    except Exception:
        # La metadata es best-effort: si falla, el test sigue funcionando.
        pass


def _handle_sigterm() -> None:
    """Drenado ordenado: marca la tarea como no-saludable y programa el apagado.

    NO detiene uvicorn de inmediato: mantiene el servicio unos segundos para que
    el balanceador la desregistre y las peticiones en vuelo terminen. Pasado el
    margen, dispara SIGINT — cuyo handler es el de uvicorn — para un apagado
    limpio. (Si interceptáramos SIGTERM sin re-disparar el apagado, la tarea
    nunca terminaría sola y ECS acabaría haciéndole SIGKILL.)
    """
    global _draining
    if _draining:
        return
    _draining = True
    loop = asyncio.get_running_loop()
    loop.call_later(DRAIN_SECONDS, lambda: signal.raise_signal(signal.SIGINT))


@asynccontextmanager
async def lifespan(_: FastAPI):
    await _load_ecs_metadata()
    # Sobrescribimos SÓLO SIGTERM (uvicorn ya instaló sus handlers antes de
    # arrancar el lifespan). SIGINT conserva el handler de uvicorn, que es el
    # que realmente ejecuta el apagado ordenado.
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGTERM, _handle_sigterm)
    yield


app = FastAPI(title="Fargate Spot Resilience Probe", lifespan=lifespan)


@app.get("/health")
async def health() -> JSONResponse:
    """Health check. Devuelve 503 mientras la tarea está drenando (post-SIGTERM)."""
    if _draining:
        return JSONResponse({"status": "draining"}, status_code=503)
    return JSONResponse({"status": "ok", **_task_meta})


@app.get("/check/{item_id}")
async def check(item_id: str, delay_ms: int = 0, fail: bool = False):
    """Atiende un `id` y devuelve trazabilidad + tiempo de respuesta.

    Query params opcionales para simular condiciones y probar el probe:
      - delay_ms: introduce latencia artificial.
      - fail:     fuerza un 500 (para verificar que el probe cuenta fallos).
    """
    start = time.perf_counter()

    if delay_ms > 0:
        await asyncio.sleep(delay_ms / 1000)

    duration_ms = round((time.perf_counter() - start) * 1000, 3)
    status_code = 500 if fail else 200

    body = {
        "id": item_id,
        "status_code": status_code,
        "duration_ms": duration_ms,
        "timestamp": time.time(),
        "draining": _draining,
        **_task_meta,
    }
    return JSONResponse(body, status_code=status_code)
