from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

import blake3

from datajig import ConsumptionStateError, open_consumption
from datajig.integrations.huggingface import iterable_dataset as huggingface_dataset
from datajig.integrations.torch import iterable_dataset as torch_dataset
from tests.cli_harness import DataJigCliTestCase


class TrainingConsumptionTests(DataJigCliTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.initialize_jsonl(
            [
                {"id": "a", "text": "alpha"},
                {"id": "b", "text": "beta"},
                {"id": "c", "text": "gamma"},
            ]
        )
        exported = self.run_cli(
            "export",
            "--state",
            self.state,
            "--output",
            self.root / "bundle",
            "--split",
            "train=10000",
            "--max-shard-records",
            "100",
        ).payload["artifact"]
        self.manifest = Path(exported["manifest"])

    def test_early_stop_and_exception_publish_no_receipt(self) -> None:
        with self.consumption("early") as (run, runtime):
            iterator = run.iter_records()
            self.assertIsInstance(next(iterator), dict)
            iterator.close()

            self.assertIsNone(run.receipt)
            self.assertFalse((Path(runtime["run_dir"]) / "datajig.consumed.json").exists())
            self.assertEqual([], list((Path(runtime["run_dir"]) / "shards").iterdir()))

        with self.consumption("exception") as (run, runtime):
            iterator = run.iter_records()
            next(iterator)
            try:
                raise RuntimeError("trainer failed")
            except RuntimeError:
                iterator.close()

            self.assertIsNone(run.receipt)
            self.assertFalse((Path(runtime["run_dir"]) / "datajig.consumed.json").exists())

    def test_requesting_eof_publishes_exact_receipt(self) -> None:
        with self.consumption("complete") as (run, runtime):
            records = list(run.iter_records())

            self.assertEqual(3, len(records))
            receipt_path = run.receipt
            self.assertIsNotNone(receipt_path)
            assert receipt_path is not None
            receipt = json.loads(receipt_path.read_text())
            self.assertTrue(receipt["consumption_receipt_id"].startswith("consumed_"))
            self.assertEqual(runtime["consumption_plan_id"], receipt["consumption_plan_id"])
            self.assertEqual("complete", receipt["run_id"])
            self.assertEqual("python", receipt["consumer"])
            self.assertEqual("train", receipt["split"])
            self.assertEqual(3, receipt["records"])
            self.assertEqual(1, receipt["shards"])
            self.assertEqual(
                "all_verified_split_records_crossed_adapter_boundary_at_least_once",
                receipt["claim"],
            )

    def test_close_after_final_yield_still_publishes_no_marker(self) -> None:
        with self.consumption("final-yield") as (run, runtime):
            iterator = run.iter_records()
            for _ in range(3):
                next(iterator)
            iterator.close()

            self.assertIsNone(run.receipt)
            self.assertEqual([], list((Path(runtime["run_dir"]) / "shards").iterdir()))

    def test_workers_publish_disjoint_markers_and_race_to_one_receipt(self) -> None:
        records = [{"id": f"row-{index:03}", "text": "x"} for index in range(101)]
        plan, planned, inspected = self.create_consumption("workers", records=records)
        completed = SimpleNamespace(returncode=0, stdout=json.dumps(inspected), stderr="")
        with patch("datajig.consumption.run_native", return_value=completed):
            first = open_consumption(plan, accept_plan=planned["consumption_plan_id"])
            second = open_consumption(plan, accept_plan=planned["consumption_plan_id"])
        failures: list[BaseException] = []

        def consume(run: Any, index: int) -> None:
            try:
                list(run._iter_records(worker_index=index, worker_count=2))
            except BaseException as exc:
                failures.append(exc)

        threads = [
            threading.Thread(target=consume, args=(first, 0)),
            threading.Thread(target=consume, args=(second, 1)),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        self.assertEqual([], failures)
        run_dir = Path(inspected["artifact"]["run_dir"])
        self.assertEqual(2, len(list((run_dir / "shards").glob("*.json"))))
        self.assertTrue((run_dir / "datajig.consumed.json").is_file())

    def test_forged_symlinked_or_unknown_run_state_fails_closed(self) -> None:
        with self.consumption("unknown-state") as (run, runtime):
            (Path(runtime["run_dir"]) / "rogue.json").write_text("{}")
            with self.assertRaises(ConsumptionStateError):
                list(run.iter_records())

        plan, planned, inspected = self.create_consumption("symlink-state")
        completed = SimpleNamespace(returncode=0, stdout=json.dumps(inspected), stderr="")
        with patch("datajig.consumption.run_native", return_value=completed):
            run = open_consumption(plan, accept_plan=planned["consumption_plan_id"])
        shard_id = inspected["artifact"]["shards"][0]["shard_id"]
        marker = Path(inspected["artifact"]["run_dir"]) / "shards" / f"{shard_id}.json"
        marker.symlink_to(plan)
        with self.assertRaises(ConsumptionStateError):
            list(run.iter_records())

    def test_completed_run_reopens_without_live_bundle(self) -> None:
        plan, planned, inspected = self.create_consumption("reopen-complete")
        completed = SimpleNamespace(returncode=0, stdout=json.dumps(inspected), stderr="")
        with patch("datajig.consumption.run_native", return_value=completed):
            run = open_consumption(plan, accept_plan=planned["consumption_plan_id"])
            list(run.iter_records())
        expected = run.receipt
        self.assertIsNotNone(expected)
        for path in (self.root / "bundle").rglob("*"):
            if path.is_file():
                path.unlink()

        with patch("datajig.consumption.run_native", side_effect=AssertionError("native called")):
            reopened = open_consumption(plan, accept_plan=planned["consumption_plan_id"])

        self.assertEqual(expected, reopened.receipt)
        with self.assertRaises(ConsumptionStateError):
            list(reopened.iter_records())

    def test_completion_recovers_missing_receipt_without_live_bundle(self) -> None:
        plan, planned, inspected = self.create_consumption("recover-receipt")
        completed = SimpleNamespace(returncode=0, stdout=json.dumps(inspected), stderr="")
        with patch("datajig.consumption.run_native", return_value=completed):
            run = open_consumption(plan, accept_plan=planned["consumption_plan_id"])
            list(run.iter_records())
        receipt = run.receipt
        self.assertIsNotNone(receipt)
        assert receipt is not None
        receipt.unlink()
        for path in (self.root / "bundle").rglob("*"):
            if path.is_file():
                path.unlink()

        with patch("datajig.consumption.run_native", side_effect=AssertionError("native called")):
            recovered = open_consumption(plan, accept_plan=planned["consumption_plan_id"])

        self.assertEqual(receipt, recovered.receipt)
        self.assertTrue(receipt.is_file())

    def test_completed_run_reopens_through_relocated_plan(self) -> None:
        plan, planned, inspected = self.create_consumption("relocated-plan")
        completed = SimpleNamespace(returncode=0, stdout=json.dumps(inspected), stderr="")
        with patch("datajig.consumption.run_native", return_value=completed):
            run = open_consumption(plan, accept_plan=planned["consumption_plan_id"])
            list(run.iter_records())
        receipt = run.receipt
        self.assertIsNotNone(receipt)
        assert receipt is not None
        receipt_bytes = receipt.read_bytes()
        relocated = self.root / "copied" / plan.name
        relocated.parent.mkdir()
        shutil.copyfile(plan, relocated)

        with patch("datajig.consumption.run_native", side_effect=AssertionError("native called")):
            reopened = open_consumption(
                relocated, accept_plan=planned["consumption_plan_id"]
            )

        self.assertEqual(receipt, reopened.receipt)
        self.assertEqual(receipt_bytes, receipt.read_bytes())

    def test_non_python_plan_cannot_bypass_its_named_adapter(self) -> None:
        plan, planned, inspected = self.create_consumption(
            "adapter-bypass", consumer="pytorch"
        )
        completed = SimpleNamespace(returncode=0, stdout=json.dumps(inspected), stderr="")
        with patch("datajig.consumption.run_native", return_value=completed):
            run = open_consumption(plan, accept_plan=planned["consumption_plan_id"])

        with self.assertRaises(ValueError):
            list(run.iter_records())
        self.assertIsNone(run.receipt)

    def test_type_invalid_plan_uses_stable_domain_error(self) -> None:
        plan, _, _ = self.create_consumption("invalid-type")
        raw = json.loads(plan.read_text())
        raw["consumer"] = 7
        identity = {key: value for key, value in raw.items() if key != "consumption_plan_id"}
        digest = blake3.blake3()
        digest.update(b"datajig-training-consumption-plan-v1\0")
        digest.update(
            json.dumps(identity, ensure_ascii=False, separators=(",", ":")).encode()
        )
        plan_id = f"consume_{digest.hexdigest()}"
        raw["consumption_plan_id"] = plan_id
        plan.write_text(json.dumps(raw, ensure_ascii=False, separators=(",", ":")))

        with self.assertRaises(ConsumptionStateError) as raised:
            open_consumption(plan, accept_plan=plan_id)

        self.assertEqual("INVALID_CONSUMPTION_PLAN", raised.exception.code)

    def test_incomplete_run_requires_live_reverification(self) -> None:
        plan, planned, inspected = self.create_consumption("reopen-incomplete")
        completed = SimpleNamespace(returncode=0, stdout=json.dumps(inspected), stderr="")
        with patch("datajig.consumption.run_native", return_value=completed):
            run = open_consumption(plan, accept_plan=planned["consumption_plan_id"])
            iterator = run.iter_records()
            next(iterator)
            iterator.close()

        with patch(
            "datajig.consumption.run_native",
            side_effect=ConsumptionStateError("STALE_CONSUMPTION_INPUT", "changed"),
        ), self.assertRaises(ConsumptionStateError):
            open_consumption(plan, accept_plan=planned["consumption_plan_id"])

    def test_run_directory_replacement_fails_before_exposing_records(self) -> None:
        with self.consumption("replaced-run") as (run, runtime):
            run_dir = Path(runtime["run_dir"])
            displaced = run_dir.with_name("replaced-run.original")
            run_dir.rename(displaced)
            shutil.copytree(displaced, run_dir)

            iterator = run.iter_records()
            with self.assertRaises(ConsumptionStateError):
                next(iterator)

    def test_framework_adapters_track_the_named_boundary_and_keep_legacy_inputs(self) -> None:
        class FakeIterableDataset:
            def __iter__(self) -> Iterator[dict[str, Any]]:
                raise NotImplementedError

        fake_torch = SimpleNamespace(
            utils=SimpleNamespace(
                data=SimpleNamespace(
                    IterableDataset=FakeIterableDataset,
                    get_worker_info=lambda: SimpleNamespace(id=0, num_workers=1),
                )
            )
        )
        plan, planned, inspected = self.create_consumption(
            "pytorch-adapter", consumer="pytorch"
        )
        completed = SimpleNamespace(returncode=0, stdout=json.dumps(inspected), stderr="")
        with (
            patch("datajig.consumption.run_native", return_value=completed),
            patch.dict(sys.modules, {"torch": fake_torch}),
        ):
            run = open_consumption(plan, accept_plan=planned["consumption_plan_id"])
            self.assertEqual(3, len(list(torch_dataset(run))))

        class FakeHuggingFaceDataset:
            @staticmethod
            def from_generator(factory: Any) -> Any:
                return factory()

        fake_datasets = SimpleNamespace(IterableDataset=FakeHuggingFaceDataset)
        plan, planned, inspected = self.create_consumption(
            "huggingface-adapter", consumer="huggingface"
        )
        completed = SimpleNamespace(returncode=0, stdout=json.dumps(inspected), stderr="")
        with (
            patch("datajig.consumption.run_native", return_value=completed),
            patch.dict(sys.modules, {"datasets": fake_datasets}),
        ):
            run = open_consumption(plan, accept_plan=planned["consumption_plan_id"])
            self.assertEqual(3, len(list(huggingface_dataset(run))))

        plan, planned, inspected = self.create_consumption(
            "wrong-adapter", consumer="python"
        )
        completed = SimpleNamespace(returncode=0, stdout=json.dumps(inspected), stderr="")
        with (
            patch("datajig.consumption.run_native", return_value=completed),
            patch.dict(sys.modules, {"torch": fake_torch}),
        ):
            run = open_consumption(plan, accept_plan=planned["consumption_plan_id"])
            with self.assertRaises(ValueError):
                torch_dataset(run)
            legacy = run._bundle
            self.assertIsNotNone(legacy)
            assert legacy is not None
            self.assertEqual(3, len(list(torch_dataset(legacy, split="train"))))

    def test_python_cli_exposes_consumption_plan_and_info(self) -> None:
        plan = self.root / "cli.consume.json"
        run_dir = self.root / "cli.run"
        planned = self.run_cli(
            "consume-plan",
            self.manifest,
            "--split",
            "train",
            "--consumer",
            "python",
            "--run-id",
            "cli-public-run",
            "--output",
            run_dir,
            "--plan",
            plan,
        ).payload
        plan_id = planned["artifact"]["consumption_plan_id"]

        inspected = self.run_cli(
            "consume-info", plan, "--verify", "--accept-plan", plan_id
        ).payload

        self.assertEqual("training_consumption_planned", planned["kind"])
        self.assertEqual("training_consumption_info", inspected["kind"])
        self.assertTrue(inspected["artifact"]["verified"])

    def test_end_to_end_cli_plan_to_adapter_receipt_and_idempotent_reopen(self) -> None:
        plan = self.root / "e2e.consume.json"
        run_dir = self.root / "e2e.run"
        planned = self.run_cli(
            "consume-plan",
            self.manifest,
            "--split",
            "train",
            "--consumer",
            "python",
            "--run-id",
            "e2e-training-run",
            "--output",
            run_dir,
            "--plan",
            plan,
        ).payload["artifact"]

        with patch.dict(os.environ, {"DATAJIG_NATIVE": str(self.native)}):
            run = open_consumption(plan, accept_plan=planned["consumption_plan_id"])
            self.assertEqual(3, len(list(run.iter_records())))
            receipt = run.receipt
            self.assertIsNotNone(receipt)
            assert receipt is not None
            receipt_bytes = receipt.read_bytes()
            reopened = open_consumption(plan, accept_plan=planned["consumption_plan_id"])

        self.assertEqual(receipt, reopened.receipt)
        self.assertEqual(receipt_bytes, receipt.read_bytes())
        payload = json.loads(receipt_bytes)
        self.assertEqual(planned["consumption_plan_id"], payload["consumption_plan_id"])
        self.assertEqual(3, payload["records"])
        self.assertEqual(
            "all_verified_split_records_crossed_adapter_boundary_at_least_once",
            payload["claim"],
        )

    @contextmanager
    def consumption(self, run_id: str) -> Iterator[tuple[Any, dict[str, Any]]]:
        plan, planned, inspected = self.create_consumption(run_id)
        runtime = inspected["artifact"]
        completed = SimpleNamespace(returncode=0, stdout=json.dumps(inspected), stderr="")
        with patch("datajig.consumption.run_native", return_value=completed):
            yield open_consumption(plan, accept_plan=planned["consumption_plan_id"]), runtime

    def create_consumption(
        self,
        run_id: str,
        *,
        records: list[dict[str, str]] | None = None,
        consumer: str = "python",
    ) -> tuple[Path, dict[str, Any], dict[str, Any]]:
        manifest = self.manifest
        if records is not None:
            suffix = self.root / run_id
            source = suffix / "dataset.jsonl"
            state = suffix / "state"
            bundle = suffix / "bundle"
            self.write_jsonl(source, records)
            self.run_native_json("init", source, "--id-field", "id", "--state", state)
            exported = self.run_native_json(
                "export",
                "--state",
                state,
                "--output",
                bundle,
                "--split",
                "train=10000",
                "--max-shard-records",
                "100",
            )
            manifest = Path(exported["artifact"]["manifest"])
        plan = self.root / f"{run_id}.consume.json"
        run_dir = self.root / f"{run_id}.run"
        planned = self.run_native_json(
            "consume-plan",
            manifest,
            "--split",
            "train",
            "--consumer",
            consumer,
            "--run-id",
            run_id,
            "--output",
            run_dir,
            "--plan",
            plan,
        )["artifact"]
        inspected = self.run_native_json(
            "consume-info",
            plan,
            "--verify",
            "--accept-plan",
            planned["consumption_plan_id"],
        )
        return plan, planned, inspected

    def run_native_json(self, *arguments: object) -> dict[str, Any]:
        completed = subprocess.run(
            [str(self.native), *(str(value) for value in arguments)],
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(0, completed.returncode, completed.stderr)
        value = json.loads(completed.stdout)
        self.assertIsInstance(value, dict)
        return value


if __name__ == "__main__":
    import unittest

    unittest.main()
