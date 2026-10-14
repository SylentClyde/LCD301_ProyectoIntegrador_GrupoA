"""Verificación HTTP de la fuente y, opcionalmente, KPIs publicados en PostgreSQL."""
from __future__ import annotations

import argparse
import json
import urllib.error
import urllib.request
from pathlib import Path


def verify(base_url: str, project_root: Path, published: str | None = None) -> dict:
    expected = json.loads((project_root / "data" / "source" / "expected.json").read_text(encoding="utf-8"))
    observations = []

    def get(path: str) -> dict:
        with urllib.request.urlopen(base_url.rstrip("/") + path, timeout=60) as response:
            assert response.status == 200
            observations.append({"path": path, "status": response.status})
            return json.load(response)

    health = get("/health")
    assert health["status"] == "ok" and health["source_available"]
    page = get("/events?cursor=0&limit=1000")
    replay = get("/events?cursor=0&limit=1000")
    assert page == replay
    assert page["source"] == "rest_api" and page["cursor"] == 0 and page["next_cursor"] == 1000
    assert len(page["events"]) == 1000
    complete = get("/events?cursor=0&limit=50000")
    assert len(complete["events"]) == expected["initial"]["source_record_count"]
    assert complete["next_cursor"] is None
    delta = get("/events/delta?cursor=0&limit=50000")
    assert len(delta["events"]) == expected["delta"]["source_record_count"]
    assert delta["next_cursor"] is None and delta["batch_id"] != complete["batch_id"]
    schema = get("/schema/event.v1")
    assert schema["type"] == "object" and "items" in schema["required"]
    for path in ("/events?cursor=-1", "/events?limit=50001", "/events?dataset=unknown"):
        try:
            get(path)
            raise AssertionError("Parámetro inválido aceptado")
        except urllib.error.HTTPError as error:
            assert error.code == 422
            observations.append({"path": path, "status": error.code})
    kpis_result = "not_requested"
    if published:
        kpis = get("/kpis")
        baseline = expected[published]
        for field in ("gross_cents", "refund_cents", "net_cents"):
            assert kpis[field] == baseline[field], (field, kpis[field], baseline[field])
        assert kpis["paid_orders"] == baseline["counts_by_type"]["paid"]
        assert kpis["currency"] == "BOB"
        kpis_result = "reconciled_" + published
    return {"status": "passed", "api_base_url": base_url,
            "initial_records": len(complete["events"]), "delta_records": len(delta["events"]),
            "deterministic_replay": True, "invalid_query_status": 422,
            "initial_batch_id": complete["batch_id"], "delta_batch_id": delta["batch_id"],
            "postgresql_kpis": kpis_result, "http_checks": observations}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://127.0.0.1:18080")
    parser.add_argument("--project-root", type=Path, default=Path(__file__).resolve().parents[2])
    parser.add_argument("--expect-published", choices=("initial", "after_delta"))
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    output = json.dumps(verify(args.base_url, args.project_root.resolve(), args.expect_published),
                        ensure_ascii=False, indent=2) + "\n"
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(output, encoding="utf-8")
    print(output)
