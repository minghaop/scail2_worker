from __future__ import annotations

import asyncio
import threading
import time
import unittest
from collections.abc import Callable
from pathlib import Path
from unittest.mock import patch

import scail2_worker_service as worker


def valid_message() -> dict:
    return {
        "action": "submitTask",
        "workflow": worker.WORKFLOW,
        "handle": "dispatcher-task-1",
        "params": {
            "reference_image": "reference.png",
            "reference_mask": "reference_mask.png",
            "driving_video": "driving.mp4",
            "driving_mask": "driving_mask.mp4",
            "prompt": "A person moving naturally.",
        },
        "s3": {
            "downloads": [
                {
                    "key": "inputs/reference.png",
                    "local_file": "reference.png",
                    "url": "http://objects/reference",
                },
                {
                    "key": "inputs/reference_mask.png",
                    "local_file": "reference_mask.png",
                    "url": "http://objects/reference-mask",
                },
                {
                    "key": "inputs/driving.mp4",
                    "local_file": "driving.mp4",
                    "url": "http://objects/driving",
                },
                {
                    "key": "inputs/driving_mask.mp4",
                    "local_file": "driving_mask.mp4",
                    "url": "http://objects/driving-mask",
                },
            ],
            "relative_path_fields": list(worker.PATH_PARAM_FIELDS),
            "uploads": ["http://objects/output"],
        },
    }


class SubmissionValidationTests(unittest.TestCase):
    def test_valid_submission(self) -> None:
        submission = worker.validate_submission(valid_message())
        self.assertEqual(submission.handle, "dispatcher-task-1")
        self.assertEqual(len(submission.downloads), 4)
        self.assertEqual(submission.upload_url, "http://objects/output")

    def test_requires_exactly_one_upload(self) -> None:
        message = valid_message()
        message["s3"]["uploads"] = []
        with self.assertRaisesRegex(worker.SubmissionError, "exactly one"):
            worker.validate_submission(message)

    def test_rejects_download_path_traversal(self) -> None:
        message = valid_message()
        message["s3"]["downloads"][0]["local_file"] = "../reference.png"
        with self.assertRaisesRegex(worker.SubmissionError, "invalid path"):
            worker.validate_submission(message)

    def test_each_path_parameter_must_reference_a_download(self) -> None:
        message = valid_message()
        message["params"]["reference_image"] = "missing.png"
        with self.assertRaisesRegex(worker.SubmissionError, "does not reference"):
            worker.validate_submission(message)


class BackendTests(unittest.TestCase):
    def test_reservation_rejects_concurrent_task(self) -> None:
        backend = worker.DispatcherJobBackend()
        backend.reserve("one")
        with self.assertRaises(worker.WorkerBusyError):
            backend.reserve("two")
        backend.cancel_reservation("one")
        backend.reserve("two")

    def test_acquire_blocks_until_shutdown(self) -> None:
        backend = worker.DispatcherJobBackend()
        result: list[object] = []
        thread = threading.Thread(target=lambda: result.append(backend.acquire()))
        thread.start()
        time.sleep(0.05)
        self.assertTrue(thread.is_alive())
        backend.request_stop()
        thread.join(timeout=1)
        self.assertEqual(result, [None])


class FakeInferenceJob:
    def __init__(self, **values: object) -> None:
        self.__dict__.update(values)

    def validate(self, *, check_paths: bool) -> None:
        if not check_paths:
            raise AssertionError("The service must validate local input paths")


class FakeInferenceResult:
    status = "success"

    def __init__(self, output_path: Path) -> None:
        self.output_path = output_path

    def to_dict(self) -> dict:
        return {"status": self.status, "output_path": str(self.output_path)}


class FakeWebSocket:
    def __init__(
        self,
        on_acknowledgement: Callable[[], None] | None = None,
    ) -> None:
        self.messages: list[dict] = []
        self.on_acknowledgement = on_acknowledgement

    async def send_json(self, message: dict) -> None:
        if (
            self.on_acknowledgement is not None
            and message.get("type") == "return"
            and message.get("action") == "submitTask"
        ):
            callback = self.on_acknowledgement
            self.on_acknowledgement = None
            callback()
        self.messages.append(message)


class ProtocolOrderingTests(unittest.TestCase):
    def test_submit_return_precedes_all_task_events(self) -> None:
        asyncio.run(self._assert_submit_return_precedes_all_task_events())

    async def _assert_submit_return_precedes_all_task_events(self) -> None:
        backend = worker.DispatcherJobBackend()
        service = worker.DispatcherWorkerService(
            backend,
            FakeInferenceJob,
            sdk_version="test",
        )

        def finish_synchronously_during_acknowledgement() -> None:
            job = backend.acquire()
            backend.mark_running(job)
            backend.mark_success(job, FakeInferenceResult(job.output_path))

        websocket = FakeWebSocket(finish_synchronously_during_acknowledgement)

        async def run_inline(function: object, *args: object) -> object:
            return function(*args)

        with (
            patch.object(worker, "clear_work_directory"),
            patch.object(worker, "download_inputs"),
            patch.object(worker, "upload_output"),
            patch.object(worker.asyncio, "to_thread", new=run_inline),
        ):
            await asyncio.wait_for(
                service._handle_submission(websocket, valid_message()),
                timeout=5,
            )

        self.assertGreaterEqual(len(websocket.messages), 2)
        acknowledgement = websocket.messages[0]
        self.assertEqual(acknowledgement["type"], "return")
        self.assertEqual(acknowledgement["action"], "submitTask")
        job_id = acknowledgement["id"]

        events = [item for item in websocket.messages[1:] if item["type"] == "event"]
        self.assertTrue(events)
        self.assertTrue(all(item["data"]["task_id"] == job_id for item in events))
        completed = [item for item in events if item["data"]["event"] == "completed"]
        self.assertEqual(len(completed), 1)
        self.assertEqual(completed[0]["data"]["state"], "succeed")


if __name__ == "__main__":
    unittest.main()
