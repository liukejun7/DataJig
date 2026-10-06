from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import unquote, urlparse

from tests.cli_harness import DataJigCliTestCase

COMMIT = "a" * 40
CONTENT = b'{"id":"one"}\n'
OID = "b" * 40
CSV_A = b"id,text\na,alpha\n"
CSV_Z = b"id,text\nz,omega\n"
FILES = {
    "data/train.jsonl": (CONTENT, OID),
    "data/a.csv": (CSV_A, "c" * 40),
    "data/z.csv": (CSV_Z, "d" * 40),
}


class HubHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_GET(self) -> None:
        path = unquote(urlparse(self.path).path)
        if path == "/api/datasets/owner/dataset/revision/main":
            self._json({"id": "owner/dataset", "sha": COMMIT})
            return
        if path == f"/api/datasets/owner/dataset/tree/{COMMIT}":
            self._json(
                [
                    {
                        "type": "file",
                        "oid": oid,
                        "size": len(content),
                        "path": file_path,
                    }
                    for file_path, (content, oid) in FILES.items()
                ]
            )
            return
        prefix = f"/datasets/owner/dataset/resolve/{COMMIT}/"
        if path.startswith(prefix) and path[len(prefix) :] in FILES:
            content, oid = FILES[path[len(prefix) :]]
            requested_range = self.headers.get("Range")
            body = content[:1] if requested_range == "bytes=0-0" else content
            self.send_response(206)
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Content-Range", f"bytes 0-{len(body) - 1}/{len(content)}")
            self.send_header("ETag", f'"{oid}"')
            self.send_header("X-Repo-Commit", COMMIT)
            self.end_headers()
            self.wfile.write(body)
            return
        self.send_error(404)

    def log_message(self, _format: str, *_args: object) -> None:
        return

    def _json(self, value: object) -> None:
        payload = json.dumps(value, separators=(",", ":")).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)


class HuggingFaceImportCliTests(DataJigCliTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), HubHandler)
        self.server_thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.server_thread.start()
        self.addCleanup(self._stop_server)

    def _stop_server(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.server_thread.join(timeout=5)

    def hub_environment(self) -> dict[str, str]:
        host, port = self.server.server_address
        return {
            "HF_ENDPOINT": f"http://{host}:{port}",
            "HF_HOME": str(self.root / "hf-home"),
            "HF_TOKEN": "",
            "NO_PROXY": f"{host},127.0.0.1,localhost",
            "no_proxy": f"{host},127.0.0.1,localhost",
            "HTTP_PROXY": "",
            "HTTPS_PROXY": "",
            "ALL_PROXY": "",
            "http_proxy": "",
            "https_proxy": "",
            "all_proxy": "",
        }

    def test_python_cli_plans_and_applies_one_immutable_hub_commit(self) -> None:
        plan = self.root / "artifacts" / "dataset.hf-plan.json"
        output = self.root / "raw" / "dataset"
        output.parent.mkdir()

        planned = self.run_cli(
            "hf-import-plan",
            "owner/dataset",
            "--revision",
            "main",
            "--include",
            "data/**",
            "--output",
            output,
            "--plan",
            plan,
            extra_env=self.hub_environment(),
        )
        self.assertEqual("hugging_face_import_planned", planned.payload["kind"])
        self.assertEqual("apply", planned.payload["decision"])
        self.assertEqual(COMMIT, planned.payload["artifact"]["resolved_commit"])
        plan_id = planned.payload["artifact"]["plan_id"]

        applied = self.run_cli(
            "hf-import-apply",
            plan,
            "--accept-plan",
            plan_id,
            extra_env=self.hub_environment(),
        )
        self.assertEqual("hugging_face_import_applied", applied.payload["kind"])
        self.assertEqual("ready", applied.payload["decision"])
        self.assertEqual("applied", applied.payload["artifact"]["outcome"])
        self.assertEqual(
            [{"command": "artifact-schema", "args": ["prepare-recipe"]}],
            applied.payload["next_actions"],
        )
        self.assertEqual(CONTENT, (output / "data" / "train.jsonl").read_bytes())
        self.assertTrue((output / "datajig.hf-import.json").is_file())

    def test_apply_rejects_a_different_plan_identity_with_json_error(self) -> None:
        plan = self.root / "artifacts" / "dataset.hf-plan.json"
        output = self.root / "raw" / "dataset"
        output.parent.mkdir()
        self.run_cli(
            "hf-import-plan",
            "owner/dataset",
            "--output",
            output,
            "--plan",
            plan,
            extra_env=self.hub_environment(),
        )

        rejected = self.run_cli(
            "hf-import-apply",
            plan,
            "--accept-plan",
            f"hfplan_{'0' * 64}",
            expected_returncode=2,
            extra_env=self.hub_environment(),
        )

        self.assertEqual("HF_IMPORT_NOT_AUTHORIZED", rejected.payload["error"]["code"])
        self.assertFalse(output.exists())

    def test_imported_shards_prepare_and_initialize_without_external_scripts(self) -> None:
        import_plan = self.root / "artifacts" / "dataset.hf-plan.json"
        import_root = self.root / "raw" / "dataset"
        import_root.parent.mkdir()
        planned_import = self.run_cli(
            "hf-import-plan",
            "owner/dataset",
            "--include",
            "data/*.csv",
            "--output",
            import_root,
            "--plan",
            import_plan,
            extra_env=self.hub_environment(),
        ).payload["artifact"]
        self.run_cli(
            "hf-import-apply",
            import_plan,
            "--accept-plan",
            planned_import["plan_id"],
            extra_env=self.hub_environment(),
        )

        recipe = self.root / "recipe.json"
        recipe.write_text(
            json.dumps(
                {
                    "namespace": "datajig",
                    "kind": "prepare",
                    "schema_version": 1,
                    "source": {
                        "format": "csv",
                        "include": ["data/*.csv"],
                        "ignore": [],
                    },
                    "output": {"format": "jsonl"},
                    "id_field": "id",
                    "steps": [],
                }
            )
        )
        prepared = self.root / "prepared" / "dataset.jsonl"
        prepare_plan = self.root / "artifacts" / "dataset.prepare-plan.json"
        planned_prepare = self.run_cli(
            "prepare-plan",
            import_root / "datajig.hf-import.json",
            "--recipe",
            recipe,
            "--output",
            prepared,
            "--plan",
            prepare_plan,
        ).payload["artifact"]
        self.assertTrue(planned_prepare["source_content_id"].startswith("hfimport_"))
        self.assertEqual(2, planned_prepare["source_rows"])
        self.run_cli(
            "prepare-apply",
            prepare_plan,
            "--accept-plan",
            planned_prepare["plan_id"],
        )
        self.assertEqual(
            b'{"id":"a","text":"alpha"}\n{"id":"z","text":"omega"}\n',
            prepared.read_bytes(),
        )

        initialized = self.run_cli(
            "init", prepared, "--id-field", "id", "--state", self.state
        ).payload["artifact"]
        self.assertIsInstance(initialized["head_revision_id"], str)
        self.assertTrue(initialized["head_revision_id"])
        status = self.status()
        self.assertEqual(initialized["head_revision_id"], status["head_revision_id"])
        self.assertTrue(status["clean"])


if __name__ == "__main__":
    import unittest

    unittest.main()
