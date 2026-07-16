"""Probe de resiliencia: le pega a la API en loop, mide cada request y agrega
métricas que reflejan cómo aguanta el servicio las interrupciones de Fargate Spot.

Idealmente se ejecuta FUERA de Spot (o desde otra tarea/máquina) para poder
observar la caída y recuperación del servicio bajo prueba.

Qué mide:
  - Por request: id, status, duración, éxito, error, task_id, AZ.
  - Detección de interrupción: cuando cambia el task_id => Spot reemplazó la tarea.
  - Recuperación (MTTR): tiempo desde el primer fallo hasta la siguiente respuesta OK.
  - Disponibilidad %, y latencias p50/p95/p99 sobre requests exitosos.

Salida:
  - stdout: una línea por request + resumen final.
  - <output>.jsonl: una línea JSON por request (para análisis posterior).
  - <output>.summary.json: métricas agregadas.

Uso:
  python probe.py --url http://localhost:8000 --rps 5 --duration 60
"""

from __future__ import annotations

import argparse
import asyncio
import json
import signal
import statistics
import time
import uuid
from pathlib import Path

import httpx


def percentile(values: list[float], pct: float) -> float | None:
    """Percentil (interpolación lineal) sobre una lista ya ordenada-o-no."""
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    rank = (pct / 100) * (len(ordered) - 1)
    low = int(rank)
    high = min(low + 1, len(ordered) - 1)
    frac = rank - low
    return round(ordered[low] + (ordered[high] - ordered[low]) * frac, 3)


class ResilienceProbe:
    def __init__(self, url: str, rps: float, duration: float, timeout: float,
                 output: Path):
        self.url = url.rstrip("/")
        self.interval = 1.0 / rps if rps > 0 else 0
        self.duration = duration
        self.timeout = timeout
        self.output = output
        self.records: list[dict] = []
        self._stop = False
        # Estado para detectar interrupciones y medir recuperación.
        self._last_task_id: str | None = None
        self.task_changes: list[dict] = []
        self._down_since: float | None = None
        self.outages: list[dict] = []

    def request_stop(self, *_: object) -> None:
        self._stop = True

    async def _one_request(self, client: httpx.AsyncClient) -> dict:
        req_id = uuid.uuid4().hex[:12]
        start = time.perf_counter()
        record: dict = {
            "probe_id": req_id,
            "ts": time.time(),
            "success": False,
            "status_code": None,
            "duration_ms": None,
            "error": None,
            "task_id": None,
            "az": None,
        }
        try:
            resp = await client.get(f"{self.url}/check/{req_id}",
                                    timeout=self.timeout)
            record["duration_ms"] = round(
                (time.perf_counter() - start) * 1000, 3)
            record["status_code"] = resp.status_code
            record["success"] = resp.status_code < 500
            try:
                body = resp.json()
                record["task_id"] = body.get("task_id")
                record["az"] = body.get("az")
            except Exception:
                pass
        except httpx.TimeoutException:
            record["duration_ms"] = round((time.perf_counter() - start) * 1000, 3)
            record["error"] = "timeout"
        except httpx.ConnectError:
            record["error"] = "connection_refused"
        except Exception as exc:  # noqa: BLE001
            record["error"] = type(exc).__name__
        return record

    def _track_transitions(self, record: dict) -> None:
        """Detecta reemplazo de tarea (Spot) y ventanas de indisponibilidad."""
        # Reemplazo de tarea: el task_id cambió respecto al anterior visto.
        tid = record["task_id"]
        if tid and self._last_task_id and tid != self._last_task_id:
            self.task_changes.append({
                "ts": record["ts"],
                "from": self._last_task_id,
                "to": tid,
            })
        if tid:
            self._last_task_id = tid

        # Ventanas de caída: del primer fallo hasta el siguiente éxito (MTTR).
        if not record["success"]:
            if self._down_since is None:
                self._down_since = record["ts"]
        else:
            if self._down_since is not None:
                self.outages.append({
                    "start": self._down_since,
                    "end": record["ts"],
                    "recovery_s": round(record["ts"] - self._down_since, 3),
                })
                self._down_since = None

    async def run(self) -> None:
        started = time.time()
        jsonl = self.output.with_suffix(".jsonl")
        jsonl.parent.mkdir(parents=True, exist_ok=True)
        n = 0
        async with httpx.AsyncClient() as client:
            with jsonl.open("w") as fh:
                while not self._stop and (time.time() - started) < self.duration:
                    loop_start = time.perf_counter()
                    record = await self._one_request(client)
                    self._track_transitions(record)
                    self.records.append(record)
                    fh.write(json.dumps(record) + "\n")
                    fh.flush()
                    n += 1

                    flag = "OK " if record["success"] else "ERR"
                    detail = record["error"] or record["status_code"]
                    dur = record["duration_ms"]
                    print(f"[{n:5d}] {flag} {detail}  "
                          f"{dur if dur is not None else '-':>8} ms  "
                          f"task={record['task_id']}")

                    sleep = self.interval - (time.perf_counter() - loop_start)
                    if sleep > 0:
                        await asyncio.sleep(sleep)

    def summarize(self) -> dict:
        total = len(self.records)
        ok = [r for r in self.records if r["success"]]
        latencies = [r["duration_ms"] for r in ok if r["duration_ms"] is not None]
        errors: dict[str, int] = {}
        for r in self.records:
            if not r["success"]:
                key = r["error"] or f"http_{r['status_code']}"
                errors[key] = errors.get(key, 0) + 1
        recoveries = [o["recovery_s"] for o in self.outages]

        return {
            "requests": total,
            "successful": len(ok),
            "failed": total - len(ok),
            "availability_pct": round(100 * len(ok) / total, 3) if total else 0,
            "errors_by_type": errors,
            "latency_ms": {
                "p50": percentile(latencies, 50),
                "p95": percentile(latencies, 95),
                "p99": percentile(latencies, 99),
                "max": max(latencies) if latencies else None,
                "mean": round(statistics.mean(latencies), 3) if latencies else None,
            },
            "task_replacements": len(self.task_changes),
            "task_replacement_events": self.task_changes,
            "outages": len(self.outages),
            "mttr_s": round(statistics.mean(recoveries), 3) if recoveries else None,
            "max_recovery_s": max(recoveries) if recoveries else None,
            "outage_events": self.outages,
        }


async def main() -> None:
    parser = argparse.ArgumentParser(description="Probe de resiliencia Fargate Spot")
    parser.add_argument("--url", default="http://localhost:8000",
                        help="URL base de la API")
    parser.add_argument("--rps", type=float, default=5.0,
                        help="Requests por segundo")
    parser.add_argument("--duration", type=float, default=60.0,
                        help="Duración del test en segundos")
    parser.add_argument("--timeout", type=float, default=5.0,
                        help="Timeout por request en segundos")
    parser.add_argument("--output", default="results/run",
                        help="Prefijo de archivos de salida")
    args = parser.parse_args()

    probe = ResilienceProbe(
        url=args.url, rps=args.rps, duration=args.duration,
        timeout=args.timeout, output=Path(args.output),
    )

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, probe.request_stop)

    print(f"Probe -> {args.url}  ({args.rps} rps, {args.duration}s)\n")
    await probe.run()

    summary = probe.summarize()
    summary_path = Path(args.output).with_suffix(".summary.json")
    summary_path.write_text(json.dumps(summary, indent=2))

    print("\n===== RESUMEN =====")
    print(f"Requests:          {summary['requests']}")
    print(f"Disponibilidad:    {summary['availability_pct']}%")
    print(f"Fallos:            {summary['failed']}  {summary['errors_by_type']}")
    print(f"Latencia p50/p95/p99: "
          f"{summary['latency_ms']['p50']} / "
          f"{summary['latency_ms']['p95']} / "
          f"{summary['latency_ms']['p99']} ms")
    print(f"Reemplazos de tarea (Spot): {summary['task_replacements']}")
    print(f"Interrupciones:    {summary['outages']}  (MTTR={summary['mttr_s']}s)")
    print(f"\nDetalle en {summary_path} y {Path(args.output).with_suffix('.jsonl')}")


if __name__ == "__main__":
    asyncio.run(main())
