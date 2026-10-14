"""Pruebas de semántica de gráficos, navegación y respuesta sin base disponible.

Los pequeños datos definidos aquí son fixtures de prueba; nunca se muestran en
la aplicación. Ejecutar: python -m unittest discover -s dashboard -v
"""
import unittest
from contextlib import contextmanager
from datetime import date
from unittest.mock import patch

import pandas as pd
import psycopg2

import app


class DashboardTests(unittest.TestCase):
    def sample(self):
        records = [
            ("p1", "paid", "o1", "c1", "2026-04-01T12:00:00Z", "web", "sku1", "Producto 1", "Hogar", 2, 6000),
            ("p1", "paid", "o1", "c1", "2026-04-01T12:00:00Z", "web", "sku2", "Producto 2", "Hogar", 1, 4000),
            ("r1", "refunded", "o1", "c1", "2026-04-03T12:00:00Z", "web", "sku1", "Producto 1", "Hogar", 1, -1500),
        ]
        frame = pd.DataFrame(records, columns=["event_id", "event_type", "order_id", "customer_id", "event_time", "channel", "product_id", "product_name", "category", "quantity", "net_cents"])
        frame["event_time"] = pd.to_datetime(frame["event_time"], utc=True)
        return frame

    def test_partial_refund_and_order_grain(self):
        figures, orders = app.business_figures(self.sample())
        self.assertEqual(len(figures), 6)
        self.assertEqual(len(orders), 1, "Las líneas no deben duplicar el pedido pagado")
        self.assertEqual(orders[0]["importe"], 100)
        self.assertEqual(sum(figures[0].data[0].y), 85, "El reembolso parcial reduce el ingreso neto")
        self.assertEqual(sum(figures[1].data[0].y), 3, "Las ventas brutas conservan las unidades pagadas")
        self.assertEqual(list(figures[4].data[0].x), [1])
        self.assertEqual(list(figures[4].data[0].y), [85])
        self.assertEqual(list(figures[5].data[0].x), [100])
        self.assertEqual(len(figures[2].data[0].z), 7)

    def test_drilldown_and_filter_reset(self):
        selected = app.selected_drill("category-bar", {"points": [{"customdata": ["Hogar"]}]}, None, None, {})
        self.assertEqual(selected, {"category": "Hogar", "product": None})
        selected = app.selected_drill("product-bar", None, {"points": [{"customdata": ["sku1"]}]}, None, selected)
        self.assertEqual(selected["product"], "sku1")
        reset = app.selected_drill("channel", None, None, "Tecnología", selected)
        self.assertEqual(reset, {"category": "Tecnología", "product": None})

    def test_filters_are_parameterized(self):
        hostile = "web'; DROP TABLE fact_event; --"
        sql, params = app.line_filter("2026-04-01", "2026-05-01", hostile, "Hogar")
        self.assertNotIn(hostile, sql)
        self.assertIn("l.channel = %s", sql)
        self.assertEqual(params, ["2026-04-01", "2026-05-01", hostile, "Hogar"])

    def test_refresh_does_not_clear_a_users_drilldown(self):
        @contextmanager
        def fake_connection():
            yield object()
        with patch.object(app, "connection", fake_connection), patch.object(app, "metadata", return_value={"first_day": date(2026, 4, 1), "last_day": date(2026, 9, 30)}), patch.object(app, "rows", return_value=[{"category": "Hogar"}]):
            response = app.refresh_filters(1, "2026-04-01", "2026-09-30", {"first": "2026-04-01", "last": "2026-09-30"})
            self.assertIs(response[3], app.no_update)
            self.assertIs(response[4], app.no_update)
            self.assertIs(response[5], app.no_update)

    def test_default_period_follows_new_published_boundaries(self):
        @contextmanager
        def fake_connection():
            yield object()
        with patch.object(app, "connection", fake_connection), patch.object(app, "metadata", return_value={"first_day": date(2026, 3, 30), "last_day": date(2026, 9, 30)}), patch.object(app, "rows", return_value=[]):
            response = app.refresh_filters(2, "2026-04-01", "2026-09-28", {"first": "2026-04-01", "last": "2026-09-28"})
            self.assertEqual(response[3:6], ("2026-03-30", "2026-09-30", {"first": "2026-03-30", "last": "2026-09-30"}))

    def test_manual_period_is_preserved_when_extent_changes(self):
        @contextmanager
        def fake_connection():
            yield object()
        with patch.object(app, "connection", fake_connection), patch.object(app, "metadata", return_value={"first_day": date(2026, 3, 30), "last_day": date(2026, 9, 30)}), patch.object(app, "rows", return_value=[]):
            response = app.refresh_filters(2, "2026-05-01", "2026-08-31", {"first": "2026-04-01", "last": "2026-09-28"})
            self.assertIs(response[3], app.no_update)
            self.assertIs(response[4], app.no_update)
            self.assertEqual(response[5], {"first": "2026-03-30", "last": "2026-09-30"})

    def test_db_outage_is_visible_without_fake_metrics(self):
        with patch.object(app.psycopg2, "connect", side_effect=psycopg2.OperationalError("offline")):
            result = app.refresh_business(0, None, None, None, None)
            self.assertEqual(len(result), 8)
            self.assertIn("Sin conexión", str(result[-2]))
            response = app.server.test_client().get("/health")
            self.assertEqual(response.status_code, 503)
            self.assertEqual(response.json["status"], "database_unavailable")

    def test_layout_and_callback_graph_can_load_without_db(self):
        with patch.object(app.psycopg2, "connect", side_effect=AssertionError("Unexpected connection")):
            client = app.server.test_client()
            self.assertEqual(client.get("/").status_code, 200)
            self.assertEqual(client.get("/_dash-layout").status_code, 200)
            self.assertEqual(client.get("/_dash-dependencies").status_code, 200)


if __name__ == "__main__":
    unittest.main()
