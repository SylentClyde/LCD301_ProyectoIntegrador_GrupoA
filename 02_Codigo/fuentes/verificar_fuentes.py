"""Comprueba reconciliación y ciclo de vida de todos los eventos sintéticos válidos."""
from __future__ import annotations

import argparse
import csv
import importlib.util
import json
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path


def verify(project_root: Path) -> dict:
    source = project_root / "data" / "source"
    expected = json.loads((source / "expected.json").read_text(encoding="utf-8"))
    initial = json.loads((source / "events_initial.json").read_text(encoding="utf-8"))
    delta = json.loads((source / "events_delta.json").read_text(encoding="utf-8"))
    products = {p["product_id"]: p for p in csv.DictReader((source / "catalogo_productos.csv").open(encoding="utf-8", newline=""))}
    assert len(products) == 100
    invalid_ids = {p["event_id"] for p in expected["invalid_cases"]}
    valid_initial = [x for x in initial if x.get("event_id") not in invalid_ids]
    unique = {x["event_id"]: x for x in valid_initial}
    assert len(valid_initial) - len(unique) == expected["initial"]["duplicate_events"]
    assert len(initial) == expected["initial"]["source_record_count"]
    assert len(unique) == expected["initial"]["accepted_events"]
    checks = 0
    for label, records in (("initial", list(unique.values())), ("delta", delta)):
        expected_set = expected[label]
        assert Counter(x["event_type"] for x in records) == expected_set["counts_by_type"]
        assert sum(x["total_cents"] for x in records if x["event_type"] == "paid") == expected_set["gross_cents"]
        assert sum(x["total_cents"] for x in records if x["event_type"] == "refunded") == expected_set["refund_cents"]
        assert {x["channel"] for x in records} == {"web", "movil", "tienda"}
        checks += 4
    assert set(unique).isdisjoint(x["event_id"] for x in delta)
    records = sorted([*unique.values(), *delta], key=lambda x: (x["event_time"], x["event_id"]))
    created, payments, cancelled = {}, {}, set()
    refunded_qty, refunded_cents = defaultdict(int), defaultdict(int)
    for value in records:
        kind, oid = value["event_type"], value["order_id"]
        timestamp = datetime.fromisoformat(value["event_time"].replace("Z", "+00:00"))
        assert timestamp.year == 2026 and 4 <= timestamp.month <= 9
        assert value["currency"] == "BOB"
        assert len({x["order_line_id"] for x in value["items"]}) == len(value["items"])
        for item in value["items"]:
            assert item["product_id"] in products
            assert item["quantity"] > 0
        if kind == "created":
            assert oid not in created
            created[oid] = value
        elif kind == "paid":
            assert oid in created and oid not in payments and oid not in cancelled
            assert value["items"] == created[oid]["items"]
            payments[oid] = value
        elif kind == "cancelled":
            assert oid in created and oid not in payments and oid not in cancelled
            cancelled.add(oid)
        elif kind == "refunded":
            assert oid in payments
            original = payments[oid]
            assert value["original_payment_id"] == original["event_id"]
            original_items = {x["order_line_id"]: x for x in original["items"]}
            for item in value["items"]:
                base = original_items[item["order_line_id"]]
                assert item["product_id"] == base["product_id"]
                assert item["unit_price_cents"] == base["unit_price_cents"]
                key = (original["event_id"], item["order_line_id"])
                refunded_qty[key] += item["quantity"]
                refunded_cents[key] += item["refund_cents"]
                assert refunded_qty[key] <= base["quantity"]
                original_net = base["quantity"] * base["unit_price_cents"] - base["discount_cents"]
                assert refunded_cents[key] <= original_net * refunded_qty[key] // base["quantity"]
            assert value["total_cents"] == sum(x["refund_cents"] for x in value["items"])
        if kind == "cancelled":
            assert value["total_cents"] == 0
        if kind not in ("refunded", "cancelled"):
            assert value["total_cents"] == sum(x["quantity"] * x["unit_price_cents"] - x["discount_cents"] for x in value["items"])
        checks += 1
    assert {datetime.fromisoformat(x["event_time"].replace("Z", "+00:00")).month for x in unique.values()} == set(range(4, 10))
    assert len({x["customer_id"] for x in unique.values()}) == 1000
    schema_result = "not_run_jsonschema_unavailable"
    if importlib.util.find_spec("jsonschema"):
        from jsonschema import Draft7Validator
        schema = json.loads((project_root / "02_Codigo" / "fuentes" / "event.v1.schema.json").read_text(encoding="utf-8"))
        validator = Draft7Validator(schema)
        invalid_schema = [x for x in initial if not validator.is_valid(x)]
        assert len(invalid_schema) == expected["initial"]["schema_invalid_events"]
        assert all(validator.is_valid(x) for x in records)
        schema_result = {"schema_rejections": len(invalid_schema), "valid_records_verified": len(records)}
    return {"status": "passed", "checks": checks, "initial_accepted": len(unique), "delta_accepted": len(delta),
            "orders_after_delta": len(created), "paid_orders_after_delta": len(payments),
            "duplicates_verified": len(valid_initial) - len(unique), "schema_validation": schema_result,
            "initial_net_cents": expected["initial"]["net_cents"], "after_delta_net_cents": expected["after_delta"]["net_cents"]}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project-root", type=Path, default=Path(__file__).resolve().parents[2])
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    result = verify(args.project_root.resolve())
    output = json.dumps(result, ensure_ascii=False, indent=2) + "\n"
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(output, encoding="utf-8")
    print(output)
