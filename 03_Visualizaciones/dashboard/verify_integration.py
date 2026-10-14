"""Verifica callbacks contra PostgreSQL real, sin insertar ni modificar datos.

Ejecutar después de publicar un lote: python verify_integration.py
El reporte contiene resultados observados, no métricas estimadas.
"""
import json
from datetime import datetime, timezone
from pathlib import Path

import app


def assert_that(condition, message):
    if not condition:
        raise AssertionError(message)


def main():
    report = {"verified_at_utc": datetime.now(timezone.utc).isoformat(), "checks": []}
    health = app.server.test_client().get("/health")
    assert_that(health.status_code == 200, "La base debe estar disponible")
    assert_that(health.json["persisted_events"] > 0, "Publica primero un lote: no hay eventos persistidos")
    report["persisted_events"] = health.json["persisted_events"]
    report["checks"].append("Salud HTTP 200 y conteo persistido positivo")

    with app.connection() as conn:
        meta = app.metadata(conn)
        expected_net = app.rows(conn, """SELECT sum(CASE WHEN event_type='paid' THEN total_cents
          WHEN event_type='refunded' THEN -total_cents ELSE 0 END)/100.0 AS net
          FROM ecommerce.fact_event""")[0]["net"]
    all_result = app.refresh_business(0, None, None, None, None)
    assert_that(len(all_result) == 8, "El callback debe producir seis figuras, tarjetas y estado")
    actual_net = sum(float(value) for trace in all_result[0].data for value in trace.y)
    assert_that(round(actual_net, 2) == round(float(expected_net), 2), "El ingreso neto del gráfico no coincide con los hechos financieros")
    assert_that(all(len(fig.data) for fig in all_result[:6]), "Los seis tipos de gráfico deben contener datos publicados")
    report["net_bob"] = round(actual_net, 2)
    report["cutoff_utc"] = meta["cutoff"].isoformat()
    report["checks"].append("Seis tipos de gráfico y reconciliación de ingreso neto contra fact_event")

    category = str(all_result[1].data[0].customdata[0][0])
    frame, *_ = app.business_data(None, None, None, category)
    channel = str(frame[frame.event_type == "paid"].iloc[0].channel)
    filtered_frame, *_ = app.business_data(None, None, channel, category)
    assert_that(set(filtered_frame.category) == {category}, "La categoría debe filtrar todas las líneas")
    assert_that(set(filtered_frame.channel) == {channel}, "El canal debe filtrar todas las líneas")
    filtered = app.refresh_business(0, None, None, channel, category)
    assert_that(set(filtered[1].data[0].x) == {category}, "Las barras deben reflejar la categoría seleccionada")
    assert_that(set(filtered[3].data[0].labels) == {app.CHANNELS[channel]}, "La dona debe reflejar el canal seleccionado")
    report["checks"].append("Filtros de canal y categoría aplicados a consultas y visualizaciones")

    drill = app.selected_drill("category-bar", {"points": [{"customdata": [category]}]}, None, None, {})
    products, _, breadcrumb = app.refresh_detail(drill, 0, None, None, None, None)
    assert_that(len(products.data) > 0, "La categoría debe abrir sus productos")
    product = str(products.data[0].customdata[0][0])
    drill = app.selected_drill("product-bar", None, {"points": [{"customdata": [product]}]}, None, drill)
    _, orders, breadcrumb = app.refresh_detail(drill, 0, None, None, None, None)
    assert_that(len(orders) > 0, "El producto debe abrir pedidos publicados")
    selected_order = orders[0]["order_id"]
    events, heading = app.refresh_events(selected_order, 0)
    assert_that(len(events) > 0, "El pedido debe abrir su historial real")
    assert_that(any(event["event_type"] == "Pagado" for event in events), "El historial debe contener su pago")
    reset = app.selected_drill("channel", None, None, None, drill)
    assert_that(reset["product"] is None, "Cambiar filtros debe limpiar el producto anterior")
    report["drilldown"] = {"category": category, "product_id": product, "order_id": selected_order, "event_count": len(events)}
    report["checks"].append("Recorrido categoría → producto → pedido → eventos y limpieza de selección")

    operations = app.refresh_operations(0)
    assert_that(len(operations) == 7, "El callback operacional debe producir siete salidas")
    assert_that(len(operations[1].data) > 0, "La serie operacional debe medir publicaciones reales")
    assert_that(len(operations[3].data) > 0, "El histograma debe medir latencias de hechos persistidos")
    assert_that(len(operations[5]) > 0, "La tabla debe contener intentos registrados")
    report["operational"] = {"recent_runs": len(operations[5]), "publication_chart_traces": len(operations[1].data),
                             "latency_chart_traces": len(operations[3].data), "nifi_queue_traces": len(operations[4].data)}
    report["checks"].append("Publicación en registros/s, latencia y ejecuciones leídas de tablas reales")
    report["nifi_queue_available"] = len(operations[4].data) > 0
    report["status"] = "passed"
    target = Path(__file__).with_name("verification_report.json")
    target.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(report, indent=2, ensure_ascii=True))


if __name__ == "__main__":
    main()
