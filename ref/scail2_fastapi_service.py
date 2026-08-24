"""FastAPI service for one persistent two-rank SCAIL-2 worker.

Only rank 0 starts Uvicorn. A dedicated runtime thread owns CUDA initialization
and every inference call, while rank 1 runs only the distributed worker loop.
The HTTP layer accepts local paths under the read-only input mount; production
object-store adapters can download objects there before submitting a job.
"""

from __future__ import annotations

import logging
import os
import threading
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from queue import Empty, Full, Queue
from typing import Any

import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, ConfigDict, Field

from scail2_inference import (
    EngineConfig,
    InferenceJob,
    InferenceResult,
    JobBackend,
    ProductionProfile,
    Scail2DistributedRuntime,
    Scail2InferenceEngine,
    __version__,
)
from scail2_inference.errors import InputValidationError


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class InferenceRequest(BaseModel):
    """The five service-owned inputs for one inference request."""

    model_config = ConfigDict(extra="forbid")

    reference_image: str = Field(min_length=1, max_length=4096)
    reference_mask: str = Field(min_length=1, max_length=4096)
    driving_video: str = Field(min_length=1, max_length=4096)
    driving_mask: str = Field(min_length=1, max_length=4096)
    prompt: str = Field(min_length=1, max_length=16384)


@dataclass
class JobRecord:
    job: InferenceJob
    status: str
    queued_at: str
    started_at: str | None = None
    finished_at: str | None = None
    result: dict[str, Any] | None = None
    error_type: str | None = None
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "job_id": self.job.job_id,
            "status": self.status,
            "output_path": str(self.job.output_path),
            "queued_at": self.queued_at,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "result": self.result,
            "error_type": self.error_type,
            "error": self.error,
        }


class ServiceQueueFullError(RuntimeError):
    pass


class ServiceStoppingError(RuntimeError):
    pass


_STOP = object()


class FastApiJobBackend(JobBackend):
    """Bounded in-memory queue and status store owned exclusively by rank 0."""

    def __init__(self, *, max_queue_size: int = 8):
        if max_queue_size <= 0:
            raise ValueError("max_queue_size must be positive")
        self._queue: Queue[InferenceJob | object] = Queue(maxsize=max_queue_size)
        self._records: dict[str, JobRecord] = {}
        self._lock = threading.Lock()
        self._stopping = False
        self._active_job_id: str | None = None

    @property
    def stopping(self) -> bool:
        with self._lock:
            return self._stopping

    def submit(self, job: InferenceJob) -> dict[str, Any]:
        record = JobRecord(job=job, status="queued", queued_at=utc_now())
        with self._lock:
            if self._stopping:
                raise ServiceStoppingError("The worker is stopping")
            self._records[job.job_id] = record
            try:
                self._queue.put_nowait(job)
            except Full:
                del self._records[job.job_id]
                raise ServiceQueueFullError("The inference queue is full") from None
            return record.to_dict()

    def acquire(self) -> InferenceJob | None:
        value = self._queue.get()
        if value is _STOP:
            return None
        if not isinstance(value, InferenceJob):
            raise TypeError("FastAPI queue contained an invalid job")
        return value

    def mark_running(self, job: InferenceJob) -> None:
        with self._lock:
            record = self._records[job.job_id]
            record.status = "running"
            record.started_at = utc_now()
            self._active_job_id = job.job_id

    def mark_success(self, job: InferenceJob, result: InferenceResult) -> None:
        with self._lock:
            record = self._records[job.job_id]
            record.status = result.status
            record.finished_at = utc_now()
            record.result = result.to_dict()
            self._active_job_id = None

    def mark_failed(
        self,
        job: InferenceJob,
        error: BaseException,
        traceback_text: str,
    ) -> None:
        logging.error(
            "SCAIL2_JOB_FAILED job_id=%s error_type=%s error=%s\n%s",
            job.job_id,
            type(error).__name__,
            error,
            traceback_text,
        )
        with self._lock:
            record = self._records[job.job_id]
            record.status = "failed"
            record.finished_at = utc_now()
            record.error_type = type(error).__name__
            record.error = str(error)
            self._active_job_id = None

    def get(self, job_id: str) -> dict[str, Any] | None:
        with self._lock:
            record = self._records.get(job_id)
            return None if record is None else record.to_dict()

    def request_stop(self) -> None:
        with self._lock:
            if self._stopping:
                return
            self._stopping = True
        while True:
            try:
                value = self._queue.get_nowait()
            except Empty:
                break
            if isinstance(value, InferenceJob):
                with self._lock:
                    record = self._records[value.job_id]
                    record.status = "failed"
                    record.finished_at = utc_now()
                    record.error_type = "ServiceStoppingError"
                    record.error = "The worker stopped before this job started"
        self._queue.put_nowait(_STOP)

    def health(self) -> dict[str, Any]:
        with self._lock:
            return {
                "queue_depth": self._queue.qsize(),
                "active_job_id": self._active_job_id,
                "stopping": self._stopping,
                "known_jobs": len(self._records),
            }


def resolve_input_path(input_dir: Path, value: str) -> Path:
    raw_path = Path(value.strip())
    candidate = raw_path if raw_path.is_absolute() else input_dir / raw_path
    resolved = candidate.resolve(strict=False)
    if not resolved.is_relative_to(input_dir):
        raise ValueError(f"Input path is outside {input_dir}: {value}")
    if not resolved.is_file() or resolved.stat().st_size == 0:
        raise ValueError(f"Input file is missing or empty: {resolved}")
    return resolved


def build_job(
    request: InferenceRequest,
    *,
    input_dir: Path,
    output_dir: Path,
) -> InferenceJob:
    job_id = uuid.uuid4().hex
    job = InferenceJob(
        job_id=job_id,
        reference_image=resolve_input_path(input_dir, request.reference_image),
        reference_mask=resolve_input_path(input_dir, request.reference_mask),
        driving_video=resolve_input_path(input_dir, request.driving_video),
        driving_mask=resolve_input_path(input_dir, request.driving_mask),
        prompt=request.prompt.strip(),
        output_path=output_dir / f"{job_id}.mp4",
        overwrite=False,
        metadata={"source": "fastapi"},
    )
    if job.reference_mask.suffix.lower() != ".png":
        raise ValueError("reference_mask must be a lossless PNG file")
    job.validate(check_paths=True)
    return job


def create_app(
    backend: FastApiJobBackend,
    runtime: Scail2DistributedRuntime,
    *,
    input_dir: Path,
    output_dir: Path,
) -> FastAPI:
    app = FastAPI(
        title="SCAIL-2 Inference Service",
        version=__version__,
    )

    @app.get("/")
    async def root() -> dict[str, str]:
        return {"service": "scail2-inference", "version": __version__}

    @app.get("/v1/health")
    async def health() -> JSONResponse:
        engine_health = runtime.engine.health()
        payload = {
            "service": "scail2-inference",
            "version": __version__,
            "runtime_ready": runtime.ready,
            "runtime_stopped": runtime.stopped,
            "runtime_failure": None
            if runtime.failure is None
            else f"{type(runtime.failure).__name__}: {runtime.failure}",
            "engine": engine_health,
            "queue": backend.health(),
        }
        status_code = 200 if runtime.ready else 503
        return JSONResponse(status_code=status_code, content=payload)

    @app.post("/v1/jobs", status_code=202)
    async def submit(request: InferenceRequest) -> JSONResponse:
        if not runtime.ready:
            raise HTTPException(status_code=503, detail="The model is not ready")
        try:
            job = build_job(request, input_dir=input_dir, output_dir=output_dir)
            payload = backend.submit(job)
        except (ValueError, InputValidationError) as error:
            raise HTTPException(status_code=422, detail=str(error)) from error
        except ServiceQueueFullError as error:
            raise HTTPException(status_code=429, detail=str(error)) from error
        except ServiceStoppingError as error:
            raise HTTPException(status_code=503, detail=str(error)) from error
        return JSONResponse(status_code=202, content=payload)

    @app.get("/v1/jobs/{job_id}")
    async def job_status(job_id: str) -> dict[str, Any]:
        payload = backend.get(job_id)
        if payload is None:
            raise HTTPException(status_code=404, detail="Unknown job_id")
        return payload

    @app.get("/v1/jobs/{job_id}/result")
    async def job_result(job_id: str) -> FileResponse:
        payload = backend.get(job_id)
        if payload is None:
            raise HTTPException(status_code=404, detail="Unknown job_id")
        if payload["status"] not in {"success", "skipped"}:
            raise HTTPException(status_code=409, detail="The result is not ready")
        output_path = Path(payload["output_path"]).resolve(strict=False)
        if not output_path.is_relative_to(output_dir) or not output_path.is_file():
            raise HTTPException(status_code=500, detail="Result file is unavailable")
        return FileResponse(
            output_path,
            media_type="video/mp4",
            filename=output_path.name,
        )

    return app


def required_path(name: str) -> Path:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Required environment variable is missing: {name}")
    return Path(value)


def positive_float_env(name: str, default: float) -> float:
    value = float(os.getenv(name, str(default)))
    if value <= 0:
        raise RuntimeError(f"{name} must be positive")
    return value


def positive_int_env(name: str, default: int) -> int:
    value = int(os.getenv(name, str(default)))
    if value <= 0:
        raise RuntimeError(f"{name} must be positive")
    return value


def create_runtime() -> Scail2DistributedRuntime:
    profile = ProductionProfile.from_name(
        os.getenv("SCAIL2_PROFILE", "scail2-512p-bf16-v1")
    )
    config_path = os.getenv("SCAIL2_ARCHITECTURE_CONFIG")
    config = EngineConfig(
        checkpoint_dir=required_path("SCAIL2_CHECKPOINT_DIR"),
        scail_checkpoint=required_path("SCAIL2_DIT_CHECKPOINT"),
        scail_config_path=None if config_path is None else Path(config_path),
        profile=profile,
        expected_world_size=2,
        t5_fsdp=True,
        dit_fsdp=True,
        offload_model=False,
        output_audio_mode=os.getenv("SCAIL2_OUTPUT_AUDIO_MODE", "driving"),
    )
    return Scail2DistributedRuntime(
        Scail2InferenceEngine(config),
        control_poll_seconds=positive_float_env(
            "SCAIL2_CONTROL_HEARTBEAT_SECONDS", 1.0
        ),
        control_timeout_seconds=positive_float_env(
            "SCAIL2_CONTROL_TIMEOUT_SECONDS", 120.0
        ),
    )


def main() -> None:
    # Validate shared mounts in both ranks before either rank starts the model.
    # An asymmetric startup failure would otherwise leave the peer blocked in a
    # distributed collective until the process supervisor terminates it.
    input_dir = Path(os.getenv("SCAIL2_INPUT_DIR", "/inputs")).resolve(strict=True)
    if not input_dir.is_dir():
        raise RuntimeError(f"Invalid input directory: {input_dir}")
    output_dir = Path(os.getenv("SCAIL2_OUTPUT_DIR", "/outputs")).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    if not output_dir.is_dir():
        raise RuntimeError(f"Invalid output directory: {output_dir}")

    runtime = create_runtime()
    if not runtime.engine.is_primary:
        runtime.run(None)
        return

    backend = FastApiJobBackend(
        max_queue_size=positive_int_env("SCAIL2_MAX_QUEUE_SIZE", 8)
    )

    def run_runtime() -> None:
        try:
            runtime.run(backend)
        except BaseException:
            logging.exception("SCAIL2_RUNTIME_FAILED")

    runtime_thread = threading.Thread(
        target=run_runtime,
        name="scail2-runtime",
        daemon=False,
    )
    runtime_thread.start()

    startup_timeout = positive_float_env("SCAIL2_STARTUP_TIMEOUT_SECONDS", 1200.0)
    if not runtime.wait_until_ready(startup_timeout):
        backend.request_stop()
        runtime_thread.join(timeout=30)
        if runtime.failure is not None:
            raise RuntimeError("SCAIL-2 runtime failed during startup") from runtime.failure
        raise RuntimeError(
            f"SCAIL-2 runtime did not become ready within {startup_timeout:g}s"
        )

    app = create_app(
        backend,
        runtime,
        input_dir=input_dir,
        output_dir=output_dir,
    )
    host = os.getenv("SCAIL2_HTTP_HOST", "0.0.0.0")
    port = positive_int_env("SCAIL2_HTTP_PORT", 8000)
    server = uvicorn.Server(
        uvicorn.Config(
            app,
            host=host,
            port=port,
            workers=1,
            log_level=os.getenv("SCAIL2_HTTP_LOG_LEVEL", "info"),
        )
    )

    def monitor_runtime() -> None:
        runtime.wait_until_stopped()
        server.should_exit = True

    threading.Thread(
        target=monitor_runtime,
        name="scail2-runtime-monitor",
        daemon=True,
    ).start()
    print(
        f"SCAIL2_FASTAPI_READY rank=0 host={host} port={port}",
        flush=True,
    )
    try:
        server.run()
    finally:
        backend.request_stop()
        runtime_thread.join(
            timeout=positive_float_env("SCAIL2_SHUTDOWN_TIMEOUT_SECONDS", 300.0)
        )
    if runtime_thread.is_alive():
        raise RuntimeError("SCAIL-2 runtime did not stop before shutdown timeout")
    if runtime.failure is not None:
        raise RuntimeError("SCAIL-2 runtime stopped after a fatal error") from runtime.failure


if __name__ == "__main__":
    main()
