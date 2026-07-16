# Fargate Spot — Test de resiliencia

API sencilla + probe para medir cómo aguanta un servicio las interrupciones de
**Fargate Spot**. La API recibe un `id` y devuelve su `status_code`, el tiempo
que tardó el request y metadata de la tarea de ECS; el probe le pega en loop y
agrega métricas de resiliencia (disponibilidad, latencias, interrupciones, MTTR).

## Componentes

```
app/main.py     API FastAPI (se despliega en Fargate Spot)
probe/probe.py  Cliente que mide en loop (correr FUERA de Spot)
Dockerfile      Imagen de la API
docker-compose.yml  Levantar la API en local
```

## La API

| Endpoint | Qué hace |
|---|---|
| `GET /check/{id}` | Devuelve `id`, `status_code`, `duration_ms`, `timestamp` y metadata de la tarea (`task_id`, `az`, `container_id`, `hostname`). |
| `GET /health` | `200` normalmente; `503 {"status":"draining"}` tras recibir SIGTERM (para que el ALB la saque de rotación). |

Query params de `/check` para simular condiciones y probar el probe:
`?delay_ms=200` (latencia artificial), `?fail=true` (fuerza un 500).

**Resiliencia integrada:**
- Lee la metadata de ECS (`ECS_CONTAINER_METADATA_URI_V4`) para exponer `task_id`
  y `az`. Cuando el probe ve cambiar el `task_id`, sabe que Spot reemplazó la tarea.
- Maneja **SIGTERM** (la señal que Fargate manda 2 min antes de interrumpir una
  tarea Spot): marca la tarea como *draining*, espera `DRAIN_SECONDS` para drenar
  conexiones y luego se apaga de forma limpia. Configurable con la env var
  `DRAIN_SECONDS` (default 5; debe ser menor que el `stopTimeout` de la task).

## El probe

Le pega a `/check/{id}` a un ritmo fijo y registra cada request. Detecta:
- **Reemplazos de tarea** (interrupciones de Spot) por cambio de `task_id`.
- **Ventanas de caída** y calcula el **MTTR** (tiempo de recuperación).
- **Disponibilidad %** y latencias **p50 / p95 / p99**.

Salida: `<output>.jsonl` (una línea por request) y `<output>.summary.json` (agregado).

## Uso en local

```bash
python -m venv .venv && .venv/bin/pip install -r requirements.txt

# Terminal 1: API
.venv/bin/uvicorn app.main:app --host 0.0.0.0 --port 8000
#   o con Docker:  docker compose up --build api

# Terminal 2: probe
.venv/bin/python probe/probe.py --url http://localhost:8000 --rps 5 --duration 60
```

Opciones del probe: `--url`, `--rps`, `--duration` (s), `--timeout` (s), `--output`.

## Despliegue en Fargate Spot (esquema)

1. `docker build -t <ecr-repo>:latest . && docker push <ecr-repo>:latest`
2. Task definition con `capacityProviderStrategy: FARGATE_SPOT`, un `stopTimeout`
   holgado (p.ej. 120s) y health check apuntando a `/health`.
3. Correr el **probe desde fuera de Spot** (Fargate normal, una EC2, o tu máquina)
   apuntando al DNS del ALB, durante un rato largo, para capturar interrupciones
   reales de Spot y medir la recuperación.

## Ideas para ampliar

- Publicar métricas a **CloudWatch** en vez de (o además de) archivos.
- Suscribirse al evento de EventBridge de **spot interruption** para correlacionar.
- Concurrencia real en el probe (varias corrutinas) para simular carga.
