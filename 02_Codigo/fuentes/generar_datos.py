"""Fuentes reproducibles de e-commerce. Sólo usa la biblioteca estándar."""
from __future__ import annotations

import argparse
import copy
import csv
import hashlib
import json
import random
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

SEED = 301
UTC = timezone.utc
START = datetime(2026, 4, 1, tzinfo=UTC)
END = datetime(2026, 9, 30, 23, 59, 59, tzinfo=UTC)
CATEGORIES = ("electronica", "ropa", "hogar", "belleza", "deportes")


def iso(value: datetime) -> str:
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def canonical(value: object) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def event(event_id: str, kind: str, order_id: str, customer_id: str,
          at: datetime, channel: str, items: list[dict], payment_id: str | None = None) -> dict:
    lines = copy.deepcopy(items)
    total = sum(x["refund_cents"] for x in lines) if kind == "refunded" else sum(
        x["quantity"] * x["unit_price_cents"] - x["discount_cents"] for x in lines)
    if kind == "cancelled":
        total = 0
    return {"event_id": event_id, "event_type": kind, "order_id": order_id,
            "customer_id": customer_id, "event_time": iso(at), "channel": channel,
            "currency": "BOB", "total_cents": total, "original_payment_id": payment_id,
            "items": lines}


def line(order_id: str, product_id: str, quantity: int, price: int,
         discount: int = 0, number: int = 1, refund: int | None = None) -> dict:
    return {"order_line_id": f"{order_id}-L{number:02}", "product_id": product_id,
            "quantity": quantity, "unit_price_cents": price,
            "discount_cents": discount, "refund_cents": refund}


def make_catalog(rng: random.Random) -> list[dict]:
    products = [{"product_id": f"PRD{i:03}", "name": f"Producto sintético {i:03}",
                 "category": CATEGORIES[(i - 1) % len(CATEGORIES)], "active": "true",
                 "catalog_version": 1, "list_price_cents": rng.randrange(10, 751) * 100}
                for i in range(1, 101)]
    products[0]["list_price_cents"] = 5000
    products[1]["list_price_cents"] = 3000
    return products


def control_events() -> list[dict]:
    result: list[dict] = []
    scenarios = [
        ("ORD-CONTROL-001", "CUST0001", "web", datetime(2026, 4, 30, 23, 55, tzinfo=UTC),
         "PRD001", 2, 5000, 0),
        ("ORD-CONTROL-002", "CUST0002", "movil", datetime(2026, 5, 31, 23, 55, tzinfo=UTC),
         "PRD002", 3, 3000, 500),
        ("ORD-CONTROL-003", "CUST0003", "tienda", datetime(2026, 6, 30, 23, 55, tzinfo=UTC),
         "PRD003", 1, 7000, 0),
        ("ORD-CONTROL-004", "CUST0001", "web", datetime(2026, 7, 1, 10, 0, tzinfo=UTC),
         "PRD001", 3, 5000, 0),
    ]
    for order_id, customer, channel, at, product, qty, price, discount in scenarios:
        items = [line(order_id, product, qty, price, discount)]
        result.append(event(f"EVT-{order_id}-CREATED", "created", order_id, customer, at, channel, items))
        if order_id.endswith("003"):
            result.append(event(f"EVT-{order_id}-CANCELLED", "cancelled", order_id, customer,
                                at + timedelta(minutes=10), channel, items))
            continue
        paid_id = f"EVT-{order_id}-PAID"
        result.append(event(paid_id, "paid", order_id, customer, at + timedelta(minutes=10), channel, items))
        refund_days = [15] if order_id.endswith("001") else [5, 12] if order_id.endswith("004") else []
        for n, day in enumerate(refund_days, 1):
            refund_lines = [line(order_id, product, 1, price, 0, refund=5000)]
            result.append(event(f"EVT-{order_id}-REFUND-{n}", "refunded", order_id, customer,
                                at + timedelta(days=day), channel, refund_lines, paid_id))
    return result


def make_valid(rng: random.Random, products: list[dict]) -> list[dict]:
    result = control_events()
    paid: list[dict] = []
    customers = [f"CUST{i:04}" for i in range(1, 1001)]
    customer_weights = [(i + 10) ** -1.5 for i in range(1, 1001)]
    # 14.996 pedidos aleatorios + cuatro pedidos de control = 15.000.
    creation_span = int((datetime(2026, 9, 14, tzinfo=UTC) - START).total_seconds())
    for i in range(1, 14997):
        order_id = f"ORD-{i:06}"
        # Asegurar cobertura y conservar una cola de compradores ocasionales.
        customer = customers[i - 1] if i <= 1000 else rng.choices(customers, weights=customer_weights, k=1)[0]
        at = START + timedelta(seconds=rng.randrange(creation_span))
        channel = rng.choices(("web", "movil", "tienda"), weights=(52, 33, 15), k=1)[0]
        selected = rng.sample(products, rng.randint(1, 4))
        items = []
        for n, product in enumerate(selected, 1):
            qty = rng.randint(1, 4)
            price = product["list_price_cents"]
            discount = (qty * price * rng.choice((0, 0, 0, 5, 10, 15))) // 100
            items.append(line(order_id, product["product_id"], qty, price, discount, n))
        result.append(event(f"EVT-{i:06}-CREATED", "created", order_id, customer, at, channel, items))
        if i <= 12797:
            paid_event = event(f"EVT-{i:06}-PAID", "paid", order_id, customer,
                               at + timedelta(minutes=rng.randint(1, 90)), channel, items)
            result.append(paid_event)
            paid.append(paid_event)
        elif i <= 13996:
            result.append(event(f"EVT-{i:06}-CANCELLED", "cancelled", order_id, customer,
                                at + timedelta(hours=rng.randint(1, 24)), channel, items))
    # Los tres reembolsos de control y estos 1.997 suman 2.000 reembolsos.
    for n, payment in enumerate(rng.sample(paid, 1997), 1):
        refund_items = []
        for original in rng.sample(payment["items"], rng.randint(1, min(2, len(payment["items"])))):
            qty = rng.randint(1, original["quantity"])
            cents = ((original["quantity"] * original["unit_price_cents"] -
                      original["discount_cents"]) * qty) // original["quantity"]
            refund_items.append({**original, "quantity": qty, "discount_cents": 0,
                                 "refund_cents": cents})
        at = datetime.fromisoformat(payment["event_time"].replace("Z", "+00:00"))
        at = min(at + timedelta(days=rng.randint(1, 16)), END)
        result.append(event(f"EVT-REFUND-{n:05}", "refunded", payment["order_id"],
                            payment["customer_id"], at, payment["channel"], refund_items,
                            payment["event_id"]))
    assert Counter(x["event_type"] for x in result) == {
        "created": 15000, "paid": 12800, "cancelled": 1200, "refunded": 2000}
    return result


def make_invalid(valid: list[dict]) -> tuple[list[dict], list[dict]]:
    result, evidence = [], []
    template = next(x for x in valid if x["event_type"] == "created" and x["order_id"].startswith("ORD-000"))
    schema_mutations = [
        ("missing_event_id", lambda x: x.pop("event_id")),
        ("empty_items", lambda x: x.update(items=[])),
        ("zero_quantity", lambda x: x["items"][0].update(quantity=0)),
        ("unknown_event_type", lambda x: x.update(event_type="shipped")),
        ("invalid_time_format", lambda x: x.update(event_time="30/04/2026")),
        ("unsupported_currency", lambda x: x.update(currency="USD")),
        ("unknown_channel", lambda x: x.update(channel="fax")),
        ("fractional_total", lambda x: x.update(total_cents=1.5)),
        ("missing_customer_id", lambda x: x.pop("customer_id")),
        ("negative_price", lambda x: x["items"][0].update(unit_price_cents=-100)),
        ("negative_discount", lambda x: x["items"][0].update(discount_cents=-5)),
        ("null_total", lambda x: x.update(total_cents=None)),
        ("payment_reference_on_created", lambda x: x.update(original_payment_id="EVT-OTHER-PAID")),
        ("items_not_array", lambda x: x.update(items="bad")),
        ("string_quantity", lambda x: x["items"][0].update(quantity="2")),
        ("unknown_field", lambda x: x.update(unexpected_field=True)),
        ("missing_product_id", lambda x: x["items"][0].pop("product_id")),
        ("negative_refund", lambda x: (x.update(event_type="refunded", original_payment_id="EVT-OTHER-PAID"),
                                      x["items"][0].update(refund_cents=-1))),
        ("missing_refund_reference", lambda x: (x.update(event_type="refunded", original_payment_id=None),
                                              x["items"][0].update(refund_cents=1))),
        ("empty_order_id", lambda x: x.update(order_id="")),
    ]
    for n, (reason, mutation) in enumerate(schema_mutations, 1):
        value = copy.deepcopy(template)
        value.update(event_id=f"EVT-INVALID-{n:03}", order_id=f"ORD-INVALID-{n:03}")
        mutation(value)
        result.append(value)
        evidence.append({"case": n, "event_id": value.get("event_id"), "stage": "schema", "reason": reason})
    for n in range(21, 31):
        value = copy.deepcopy(template)
        value.update(event_id=f"EVT-INVALID-{n:03}", order_id=f"ORD-INVALID-{n:03}")
        value["items"][0]["product_id"] = "PRD999"
        result.append(value)
        evidence.append({"case": n, "event_id": value["event_id"], "stage": "business", "reason": "unknown_product"})
    for n in range(31, 36):
        value = copy.deepcopy(template)
        value.update(event_id=f"EVT-INVALID-{n:03}", order_id=f"ORD-INVALID-{n:03}",
                     total_cents=value["total_cents"] + 1)
        result.append(value)
        evidence.append({"case": n, "event_id": value["event_id"], "stage": "business", "reason": "total_mismatch"})
    payment = next(x for x in valid if x["event_id"] == "EVT-ORD-CONTROL-001-PAID")
    for n in (36, 37):
        value = event(f"EVT-INVALID-{n:03}", "refunded", payment["order_id"], payment["customer_id"],
                      datetime(2026, 5, 28, tzinfo=UTC), payment["channel"],
                      [line(payment["order_id"], "PRD001", 1, 5000, refund=5000)], "EVT-PAYMENT-NOT-EXISTS")
        result.append(value)
        evidence.append({"case": n, "event_id": value["event_id"], "stage": "business", "reason": "payment_reference_invalid_or_pending"})
    for n, qty, cents, reason in ((38, 3, 15000, "refund_quantity_exceeded"),
                                  (39, 1, 10001, "refund_amount_exceeded")):
        value = event(f"EVT-INVALID-{n:03}", "refunded", payment["order_id"], payment["customer_id"],
                      datetime(2026, 5, 28, tzinfo=UTC), payment["channel"],
                      [line(payment["order_id"], "PRD001", qty, 5000, refund=cents)], payment["event_id"])
        result.append(value)
        evidence.append({"case": n, "event_id": value["event_id"], "stage": "business", "reason": reason})
    value = copy.deepcopy(payment)
    value.update(event_id="EVT-INVALID-040", event_time="2026-05-29T00:00:00Z")
    result.append(value)
    evidence.append({"case": 40, "event_id": value["event_id"], "stage": "business", "reason": "second_payment_for_order"})
    assert len(result) == 40
    return result, evidence


def make_delta() -> list[dict]:
    result = []
    for n, channel, product, qty, price in ((1, "web", "PRD001", 4, 5000),
                                          (2, "movil", "PRD002", 4, 3000),
                                          (3, "tienda", "PRD002", 2, 3000)):
        order_id = f"ORD-DELTA-{n:03}"
        at = datetime(2026, 9, 29, 10 + n, tzinfo=UTC)
        items = [line(order_id, product, qty, price)]
        result.append(event(f"EVT-DELTA-{n:03}-CREATED", "created", order_id, "CUST0001", at, channel, items))
        result.append(event(f"EVT-DELTA-{n:03}-PAID", "paid", order_id, "CUST0001",
                            at + timedelta(minutes=5), channel, items))
    result.append(event("EVT-DELTA-001-REFUND", "refunded", "ORD-DELTA-001", "CUST0001",
                        datetime(2026, 9, 30, 12, tzinfo=UTC), "web",
                        [line("ORD-DELTA-001", "PRD001", 2, 5000, refund=10000)], "EVT-DELTA-001-PAID"))
    return result


def summary(events: list[dict]) -> dict:
    counts = dict(sorted(Counter(x["event_type"] for x in events).items()))
    gross = sum(x["total_cents"] for x in events if x["event_type"] == "paid")
    refunds = sum(x["total_cents"] for x in events if x["event_type"] == "refunded")
    paid_customers: dict[str, set[str]] = defaultdict(set)
    for value in events:
        if value["event_type"] == "paid":
            paid_customers[value["customer_id"]].add(value["order_id"])
    return {"accepted_events": len(events), "counts_by_type": counts,
            "gross_cents": gross, "refund_cents": refunds, "net_cents": gross - refunds,
            "paid_units": sum(i["quantity"] for e in events if e["event_type"] == "paid" for i in e["items"]),
            "refunded_units": sum(i["quantity"] for e in events if e["event_type"] == "refunded" for i in e["items"]),
            "customers_created": len(set(x["customer_id"] for x in events)),
            "paying_customers": len(paid_customers),
            "repeat_customers": sum(len(orders) >= 2 for orders in paid_customers.values()),
            "first_event_time": min(x["event_time"] for x in events),
            "last_event_time": max(x["event_time"] for x in events)}


def write_json(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def generate(project_root: Path) -> dict:
    source = project_root.resolve() / "data" / "source"
    source.mkdir(parents=True, exist_ok=True)
    rng = random.Random(SEED)
    catalog = make_catalog(rng)
    valid = make_valid(rng, catalog)
    invalid, invalid_evidence = make_invalid(valid)
    duplicates = copy.deepcopy(valid[:25])
    initial = valid + invalid + duplicates
    rng.shuffle(initial)  # Prueba intencional de eventos fuera de orden.
    delta = make_delta()
    write_json(source / "events_initial.json", initial)
    write_json(source / "events_delta.json", delta)
    write_json(source / "control_events.json", control_events())
    fields = ["product_id", "name", "category", "active", "catalog_version", "list_price_cents"]
    for filename, rows in (("catalogo_productos.csv", catalog),
                           ("catalogo_productos_invalido.csv", [{**catalog[0], "list_price_cents": -1}])):
        with (source / filename).open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fields)
            writer.writeheader()
            writer.writerows(rows)
    expected = {"seed": SEED, "currency": "BOB", "timezone": "UTC",
                "period": {"start": iso(START), "end": iso(END)},
                "catalog": {"valid_products": 100, "version": 1, "invalid_rows": 1},
                "initial": {**summary(valid), "source_record_count": len(initial),
                            "schema_invalid_events": 20, "business_invalid_or_pending_events": 20,
                            "duplicate_events": len(duplicates),
                            "sha256": hashlib.sha256(canonical(initial)).hexdigest()},
                "delta": {**summary(delta), "source_record_count": len(delta),
                          "sha256": hashlib.sha256(canonical(delta)).hexdigest()},
                "after_delta": summary(valid + delta),
                "controls": {"summary": summary(control_events()),
                             "orders": {"ORD-CONTROL-001": {"paid_cents": 10000, "refund_cents": 5000, "net_cents": 5000, "paid_units": 2, "refund_units": 1},
                                        "ORD-CONTROL-002": {"paid_cents": 8500, "refund_cents": 0, "net_cents": 8500, "paid_units": 3, "refund_units": 0},
                                        "ORD-CONTROL-003": {"paid_cents": 0, "refund_cents": 0, "net_cents": 0, "cancelled": True},
                                        "ORD-CONTROL-004": {"paid_cents": 15000, "refund_cents": 10000, "net_cents": 5000, "paid_units": 3, "refund_units": 2, "refund_events": 2}}},
                "invalid_cases": invalid_evidence,
                "duplicate_event_ids": [x["event_id"] for x in duplicates],
                "note": "Los dos reembolsos con referencia inexistente pueden quedar pendientes; nunca deben publicar importes."
                }
    write_json(source / "expected.json", expected)
    write_json(source / "dataset_manifest.json", {
        "generator": "generar_datos.py", "seed": SEED, "synthetic_only": True,
        "files": [{"name": p.name, "bytes": p.stat().st_size,
                   "sha256": hashlib.sha256(p.read_bytes()).hexdigest()}
                  for p in sorted(source.iterdir()) if p.is_file() and p.name != "dataset_manifest.json"]})
    return expected


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project-root", type=Path, default=Path(__file__).resolve().parents[2])
    args = parser.parse_args()
    expected = generate(args.project_root)
    print(json.dumps({"source_directory": str(args.project_root.resolve() / "data" / "source"),
                      "initial": expected["initial"], "delta": expected["delta"]}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
