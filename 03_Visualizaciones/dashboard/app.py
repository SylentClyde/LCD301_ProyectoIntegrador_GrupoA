"""Dashboards de Grupo A. Todos los valores visibles provienen de PostgreSQL.

Períodos de negocio en UTC, moneda BOB, operación en reloj de publicación.
La aplicación puede importarse sin consultar la base ni abrir conexiones de red.
"""
from __future__ import annotations

import os
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import psycopg2
from psycopg2.extras import RealDictCursor
from dash import Dash, Input, Output, State, ctx, dash_table, dcc, html, no_update


DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://lcd301:lcd301_local@127.0.0.1:55432/lcd301")
COLORS = ["#087f8c", "#f0a44b", "#7979c7", "#3693c7", "#c66a68", "#5b9a6f"]
CHANNELS = {"web": "Web", "movil": "Móvil", "tienda": "Tienda"}
EVENTS = {"created": "Creado", "paid": "Pagado", "cancelled": "Cancelado", "refunded": "Reembolso"}
CONFIG = {"displaylogo": False, "responsive": True, "modeBarButtonsToRemove": ["lasso2d", "select2d"]}
px.defaults.color_discrete_sequence = COLORS


@contextmanager
def connection():
    conn = psycopg2.connect(DATABASE_URL, connect_timeout=4, application_name="grupo_a_dashboard", options="-c timezone=UTC")
    conn.set_session(readonly=True, isolation_level="REPEATABLE READ", autocommit=False)
    try:
        with conn:
            yield conn
    finally:
        conn.close()


def rows(conn, sql, params=()):
    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute("SET LOCAL statement_timeout = '12s'")
        cursor.execute(sql, params)
        return [dict(row) for row in cursor.fetchall()]


def metadata(conn):
    return rows(conn, """SELECT min((event_time AT TIME ZONE 'UTC')::date) AS first_day,
      max((event_time AT TIME ZONE 'UTC')::date) AS last_day,
      max(event_time) AS cutoff, max(persisted_at) AS published_at,
      count(*) AS records FROM ecommerce.fact_event""")[0]


def bounds(start, end, meta):
    first = date.fromisoformat(start[:10]) if start else meta["first_day"]
    last = date.fromisoformat(end[:10]) if end else meta["last_day"]
    if first is None or last is None:
        first = last = datetime.now(timezone.utc).date()
    if first > last:
        raise ValueError("La fecha inicial debe ser anterior a la fecha final.")
    return f"{first.isoformat()}T00:00:00+00:00", f"{(last + timedelta(days=1)).isoformat()}T00:00:00+00:00"


def line_filter(start, end, channel, category, alias="l"):
    clauses = [f"{alias}.event_time >= %s::timestamptz", f"{alias}.event_time < %s::timestamptz"]
    params = [start, end]
    if channel:
        clauses.append(f"{alias}.channel = %s")
        params.append(channel)
    if category:
        clauses.append(f"{alias}.category = %s")
        params.append(category)
    return " AND ".join(clauses), params


def business_data(start, end, channel, category):
    with connection() as conn:
        meta = metadata(conn)
        lower, upper = bounds(start, end, meta)
        where, params = line_filter(lower, upper, channel, category)
        facts = rows(conn, f"""SELECT l.event_id,l.event_type,l.order_id,l.customer_id,
          l.event_time,l.channel,l.product_id,l.product_name,l.category,l.quantity,l.net_cents
          FROM ecommerce.fact_event_line l WHERE {where}
          AND l.event_type IN ('paid','refunded') ORDER BY l.event_time""", params)
        cohort_filter = "o.event_type='created' AND o.event_time >= %s::timestamptz AND o.event_time < %s::timestamptz"
        cohort_params = [lower, upper]
        if channel:
            cohort_filter += " AND o.channel = %s"
            cohort_params.append(channel)
        if category:
            # La cohorte parte de creaciones únicas y aplica filtros antes de agregar.
            cohort_filter += """ AND o.order_id IN (SELECT cl.order_id FROM ecommerce.fact_event_line cl
              WHERE cl.event_type='created' AND cl.category=%s)"""
            cohort_params.append(category)
        cancellation = rows(conn, f"""SELECT count(*) AS created_orders,
          count(*) FILTER (WHERE c.event_id IS NOT NULL AND p.event_id IS NULL) AS cancelled_orders
          FROM ecommerce.event_registry o
          LEFT JOIN ecommerce.event_registry c ON c.order_id=o.order_id AND c.event_type='cancelled'
          LEFT JOIN ecommerce.event_registry p ON p.order_id=o.order_id AND p.event_type='paid'
          WHERE {cohort_filter}""", cohort_params)[0]
        # Only paid cohorts that have 30 complete days of observation enter this rate.
        returns = rows(conn, f"""WITH mature AS (
          SELECT l.* FROM ecommerce.fact_event_line l WHERE {where} AND l.event_type='paid'
            AND l.event_time <= %s::timestamptz - interval '30 days'
        ), returned AS (
          SELECT p.event_id,p.order_line_id,coalesce(sum(r.quantity),0) AS returned_units
          FROM mature p LEFT JOIN ecommerce.fact_event_line r
            ON r.original_payment_id=p.event_id AND r.order_line_id=p.order_line_id
            AND r.event_type='refunded' AND r.event_time >= p.event_time
            AND r.event_time <= p.event_time + interval '30 days'
            AND r.event_time <= %s::timestamptz GROUP BY p.event_id,p.order_line_id
        ) SELECT coalesce(sum(p.quantity),0) AS mature_units,
          coalesce(sum(r.returned_units),0) AS returned_units
          FROM mature p JOIN returned r USING(event_id,order_line_id)""", params + [meta["cutoff"], meta["cutoff"]])[0]
        latest = rows(conn, """SELECT batch_id,finished_at,published_count FROM ecommerce.pipeline_runs
          WHERE status IN ('success','completed') ORDER BY finished_at DESC NULLS LAST LIMIT 1""")
    columns = ["event_id", "event_type", "order_id", "customer_id", "event_time", "channel", "product_id", "product_name", "category", "quantity", "net_cents"]
    frame = pd.DataFrame(facts, columns=columns)
    if not frame.empty:
        frame["event_time"] = pd.to_datetime(frame["event_time"], utc=True)
        frame["net_cents"] = pd.to_numeric(frame["net_cents"])
        frame["quantity"] = pd.to_numeric(frame["quantity"])
    return frame, meta, cancellation, returns, latest


def empty_figure(message="Sin datos para el período seleccionado"):
    fig = go.Figure()
    fig.add_annotation(text=message, x=0.5, y=0.5, xref="paper", yref="paper", showarrow=False,
                       font={"size": 15, "color": "#82919c"})
    fig.update_xaxes(visible=False)
    fig.update_yaxes(visible=False)
    return decorate(fig)


def decorate(fig, height=310):
    fig.update_layout(template="plotly_white", height=height, margin={"l": 45, "r": 20, "t": 15, "b": 48},
                      font={"family": "Segoe UI, Arial, sans-serif", "size": 12, "color": "#526372"},
                      paper_bgcolor="white", plot_bgcolor="white", colorway=COLORS,
                      legend={"orientation": "h", "y": 1.12, "x": 0}, hovermode="closest")
    fig.update_xaxes(gridcolor="#edf2f4", zeroline=False)
    fig.update_yaxes(gridcolor="#edf2f4", zeroline=False)
    return fig


def number(value, decimals=0):
    if value is None:
        return "—"
    return f"{float(value):,.{decimals}f}".replace(",", "_").replace(".", ",").replace("_", ".")


def rate(numerator, denominator):
    return f"{number(100 * numerator / denominator, 1)} %" if denominator else "Sin datos"


def card(label, value, note, tone=""):
    return html.Div([html.Span(label, className="kpi-label"), html.Strong(value, className="kpi-value"),
                     html.Small(note, className="kpi-note")], className=f"kpi {tone}")


def graph_panel(graph_id, title, subtitle="", wide=False):
    return html.Div([html.Div([html.H3(title), html.P(subtitle)], className="panel-title"),
                     dcc.Graph(id=graph_id, figure=empty_figure(), config=CONFIG, responsive=True,
                               style={"height": "310px", "width": "100%"})], className="panel wide" if wide else "panel")


def business_figures(frame):
    if frame.empty:
        return [empty_figure() for _ in range(6)], []
    df = frame.copy()
    df["importe"] = df["net_cents"] / 100
    df["day"] = df["event_time"].dt.strftime("%Y-%m-%d")
    daily = df.groupby(["day", "channel"], as_index=False)["importe"].sum()
    daily["Canal"] = daily["channel"].map(CHANNELS)
    line = px.line(daily, x="day", y="importe", color="Canal", labels={"day": "Fecha del evento · UTC", "importe": "Ingreso neto · BOB"})
    paid = df[df.event_type == "paid"].copy()
    if paid.empty:
        return [decorate(line)] + [empty_figure("Sin pagos confirmados en el período") for _ in range(5)], []
    categories = paid.groupby("category", as_index=False)["quantity"].sum().sort_values("quantity", ascending=False)
    bar = px.bar(categories, x="category", y="quantity", custom_data=["category"],
                 labels={"category": "Categoría · clic para explorar", "quantity": "Unidades pagadas"})
    bar.update_traces(marker_color=COLORS[0], hovertemplate="%{x}<br>%{y} unidades<extra></extra>")
    orders = paid.groupby(["order_id", "customer_id", "channel"], as_index=False)["importe"].sum()
    activity = paid[["order_id", "event_time"]].drop_duplicates("order_id").copy()
    activity["weekday"] = activity["event_time"].dt.dayofweek
    activity["hour"] = activity["event_time"].dt.hour
    heat_data = activity.groupby(["weekday", "hour"]).size().unstack(fill_value=0).reindex(index=range(7), columns=range(24), fill_value=0)
    heat = go.Figure(go.Heatmap(z=heat_data.values, x=list(range(24)), y=["Lun", "Mar", "Mié", "Jue", "Vie", "Sáb", "Dom"],
                                colorscale=[[0, "#f2f7f7"], [1, "#087f8c"]], colorbar={"title": "Pedidos"},
                                hovertemplate="%{y} · %{x}:00 UTC<br>%{z} pedidos<extra></extra>"))
    heat.update_xaxes(title="Hora del pago · UTC")
    channels = orders.groupby("channel", as_index=False).size()
    channels["Canal"] = channels["channel"].map(CHANNELS)
    donut = px.pie(channels, values="size", names="Canal", hole=0.62)
    donut.update_traces(textinfo="percent+label", hovertemplate="%{label}<br>%{value} pedidos pagados<extra></extra>")
    customers = df.groupby("customer_id", as_index=False)["importe"].sum()
    frequencies = orders.groupby("customer_id").size().rename("pedidos")
    customers = customers.merge(frequencies, on="customer_id", how="left").fillna({"pedidos": 0})
    scatter = px.scatter(customers, x="pedidos", y="importe", hover_name="customer_id", custom_data=["customer_id"],
                         labels={"pedidos": "Pedidos pagados en el período", "importe": "Ingreso neto observado · BOB"}, opacity=0.72)
    scatter.update_traces(marker={"size": 7, "color": COLORS[0]})
    hist = px.histogram(orders, x="importe", nbins=35,
                        labels={"importe": "Importe pagado por pedido · BOB", "count": "Pedidos"})
    hist.update_yaxes(title="Pedidos")
    hist.update_traces(marker_color=COLORS[1])
    return [decorate(fig) for fig in [line, bar, heat, donut, scatter, hist]], orders.to_dict("records")


TABLE_STYLE = {"style_table": {"overflowX": "auto"}, "style_cell": {"fontFamily": "Segoe UI, Arial", "fontSize": 12, "padding": "11px", "textAlign": "left", "border": "none", "borderBottom": "1px solid #edf2f4", "whiteSpace": "normal", "height": "auto"}, "style_header": {"fontWeight": "600", "backgroundColor": "#f3f7f8", "color": "#526372"}, "style_data_conditional": [{"if": {"state": "active"}, "backgroundColor": "#e6f4f3", "border": "1px solid #087f8c"}], "page_size": 8, "sort_action": "native"}

app = Dash(__name__, title="Grupo A | Comercio en datos", update_title=None)
server = app.server
app.layout = html.Div([
    dcc.Interval(id="refresh", interval=15_000, n_intervals=0),
    dcc.Store(id="drill", data={"category": None, "product": None}),
    dcc.Store(id="selected-order"),
    dcc.Store(id="previous-extent", data={}),
    html.Header([html.Div([html.Span("LCD301 / GRUPO A", className="eyebrow"), html.H1("Comercio en datos"),
                         html.P("Ventas, devoluciones y clientes en una sola vista.")]),
                 html.Div([html.Span("BOB", className="currency"), html.Span("Actualización cada 15 s", className="live-label")], className="header-right")], className="masthead"),
    html.Main([
        dcc.Tabs(id="view", value="business", className="navigation", children=[
            dcc.Tab(label="Negocio", value="business", className="nav-tab", selected_className="nav-selected"),
            dcc.Tab(label="Operación", value="operations", className="nav-tab", selected_className="nav-selected")]),
        html.Div([html.Div([html.Label("Período · UTC"), dcc.DatePickerRange(id="date-range", display_format="DD/MM/YYYY", minimum_nights=0)], className="filter date-filter"),
                  html.Div([html.Label("Canal"), dcc.Dropdown(id="channel", options=[{"label": value, "value": key} for key, value in CHANNELS.items()], placeholder="Todos los canales", clearable=True)], className="filter"),
                  html.Div([html.Label("Categoría"), dcc.Dropdown(id="category", options=[], placeholder="Todas las categorías", clearable=True)], className="filter"),
                  html.Div([html.Span("Filtros de negocio", className="filter-hint"), html.Small("La operación muestra las últimas 24 horas.")], className="filter-help")], id="filters", className="filter-bar"),
        html.Div(id="data-status", className="status-strip"),
        html.Section([
            html.Div([html.Div([html.H2("El pulso del negocio"), html.P("Ingresos según la fecha de cada pago y reembolso.")]), html.Span("Sin impuestos ni envío", className="section-note")], className="section-heading"),
            html.Div(id="business-kpis", className="kpi-grid"),
            html.Div([
                graph_panel("sales-line", "Evolución de ingresos", "Pagos confirmados menos reembolsos, por canal.", wide=True),
                graph_panel("category-bar", "Ventas por categoría", "Haz clic en una barra para explorar los productos."),
                graph_panel("activity-heat", "Cuándo compran los clientes", "Pedidos pagados por día de semana y hora · UTC."),
                graph_panel("channel-donut", "Participación de canales", "Porcentaje de pedidos pagados."),
                graph_panel("customer-scatter", "Frecuencia y valor de clientes", "Valor neto observado en el período; no es CLV predictivo."),
                graph_panel("ticket-hist", "Distribución de tickets", "Importes de pagos confirmados por pedido.", wide=True)
            ], className="chart-grid"),
            html.Div([html.Div([html.H2("Del resumen al pedido"), html.P("Categoría → producto → pedido → historial de eventos.")]), html.Button("Limpiar selección", id="reset-drill", n_clicks=0, className="quiet-button")], className="section-heading"),
            html.Div(id="drill-breadcrumb", className="drill-breadcrumb"),
            html.Div([
                graph_panel("product-bar", "Productos de la selección", "Haz clic en un producto para ver sus pedidos."),
                html.Div([html.Div([html.H3("Pedidos del producto"), html.P("Importes del producto seleccionado. Selecciona una fila para abrir el historial del pedido completo; hasta 200 pedidos.")], className="panel-title"),
                          dash_table.DataTable(id="orders-table", columns=[{"name": name, "id": key} for name, key in [("Pedido", "order_id"), ("Cliente", "customer_id"), ("Canal", "channel"), ("Pagado BOB", "paid_bob"), ("Devuelto BOB", "refund_bob")]], data=[], **TABLE_STYLE)], className="panel")
            ], className="chart-grid"),
            html.Div([html.Div([html.H3("Historial del pedido"), html.P(id="order-heading", children="Selecciona un pedido en la tabla.")], className="panel-title"),
                      dash_table.DataTable(id="events-table", columns=[{"name": name, "id": key} for name, key in [("Evento", "event_id"), ("Tipo", "event_type"), ("Fecha UTC", "event_time"), ("Importe BOB", "amount"), ("Lote", "batch_id")]], data=[], **TABLE_STYLE)], className="panel history-panel"),
            html.P("Devolución a 30 días: unidades reembolsadas / unidades pagadas de cohortes con 30 días completos. Cancelación: pedidos cancelados sin pago / pedidos creados en el período. Las tasas sin denominador se muestran como «Sin datos».", className="method-note")
        ], id="business-view"),
        html.Section([
            html.Div([html.Div([html.H2("El pulso del sistema"), html.P("Registros de negocio, colas y calidad de las últimas 24 horas.")]), html.Span("Lecturas reales del pipeline", className="section-note")], className="section-heading"),
            html.Div(id="operational-kpis", className="kpi-grid op-kpis"),
            html.Div([
                graph_panel("throughput-line", "Publicación de registros", "Registros persistidos por segundo, promedio de cada minuto."),
                graph_panel("errors-bar", "Errores y rechazos por etapa", "Conteos de calidad de Spark y del colector de ingesta."),
                graph_panel("latency-hist", "Latencia de publicación", "Persistencia menos recepción; incluye la espera del procesamiento batch."),
                graph_panel("queue-line", "Colas de ingesta", "FlowFiles en espera. Un FlowFile puede contener muchos registros.")
            ], className="chart-grid"),
            html.Div([html.Div([html.H3("Ejecuciones recientes"), html.P("Conteos por intento. Los duplicados no vuelven a publicarse.")], className="panel-title"),
                      dash_table.DataTable(id="runs-table", columns=[{"name": name, "id": key} for name, key in [("Lote", "batch_id"), ("Estado", "status"), ("Entrada", "input_count"), ("Publicados", "published_count"), ("Rechazados", "rejected_count"), ("Pendientes", "pending_count"), ("Duplicados", "duplicate_count"), ("Duración s", "duration_seconds")]], data=[], **TABLE_STYLE)], className="panel history-panel"),
            html.P(id="operations-note", className="method-note")
        ], id="operations-view", style={"display": "none"}),
        html.Footer([html.Span("Grupo A · Proyecto integrador LCD301"), html.Span("Fuentes sintéticas · Moneda BOB · Fechas de negocio UTC")])
    ], className="main-container")
], className="app-shell")


@app.callback(Output("business-view", "style"), Output("operations-view", "style"), Output("filters", "style"), Input("view", "value"))
def switch_view(view):
    return ({"display": "block"}, {"display": "none"}, {}) if view == "business" else ({"display": "none"}, {"display": "block"}, {"display": "none"})


@app.callback(Output("category", "options"), Output("date-range", "min_date_allowed"), Output("date-range", "max_date_allowed"),
              Output("date-range", "start_date"), Output("date-range", "end_date"), Output("previous-extent", "data"),
              Input("refresh", "n_intervals"), State("date-range", "start_date"), State("date-range", "end_date"),
              State("previous-extent", "data"))
def refresh_filters(_tick, start, end, previous_extent):
    try:
        with connection() as conn:
            meta = metadata(conn)
            categories = rows(conn, "SELECT DISTINCT category FROM ecommerce.dim_product ORDER BY category")
        first = meta["first_day"].isoformat() if meta["first_day"] else None
        last = meta["last_day"].isoformat() if meta["last_day"] else None
        previous_extent = previous_extent or {}
        extent = {"first": first, "last": last}
        # Follow a moving boundary only when the user still selected that
        # previous boundary. Unchanged dates preserve their drilldown.
        start_value = first if not start or (first and start == previous_extent.get("first") and first != start) else no_update
        end_value = last if not end or (last and end == previous_extent.get("last") and last != end) else no_update
        return [{"label": r["category"], "value": r["category"]} for r in categories], first, last, start_value, end_value, no_update if previous_extent == extent else extent
    except (psycopg2.Error, ValueError):
        return [], None, None, no_update, no_update, no_update


@app.callback(*[Output(graph_id, "figure") for graph_id in ["sales-line", "category-bar", "activity-heat", "channel-donut", "customer-scatter", "ticket-hist"]],
              Output("business-kpis", "children"), Output("data-status", "children"),
              Input("refresh", "n_intervals"), Input("date-range", "start_date"), Input("date-range", "end_date"), Input("channel", "value"), Input("category", "value"))
def refresh_business(_tick, start, end, channel, category):
    try:
        df, meta, cancellations, returns, latest = business_data(start, end, channel, category)
        figures, paid_orders = business_figures(df)
        paid = df[df.event_type == "paid"] if not df.empty else df
        refund = df[df.event_type == "refunded"] if not df.empty else df
        customers = pd.DataFrame(paid_orders).groupby("customer_id").size() if paid_orders else pd.Series(dtype="int64")
        kpis = [card("Ingreso neto", f"{number(df.net_cents.sum() / 100, 2)} BOB", "Pagos menos reembolsos", "accent"),
                card("Pedidos pagados", number(paid.order_id.nunique()), "Pedidos distintos en el período"),
                card("Importe reembolsado", f"{number(-refund.net_cents.sum() / 100, 2)} BOB", "Incluye devoluciones parciales"),
                card("Clientes recurrentes", rate(int((customers >= 2).sum()), len(customers)), f"{number((customers >= 2).sum())} de {number(len(customers))} clientes con pagos"),
                card("Cancelación", rate(cancellations["cancelled_orders"], cancellations["created_orders"]), f"{number(cancellations['cancelled_orders'])} / {number(cancellations['created_orders'])} pedidos creados"),
                card("Devolución a 30 días", rate(returns["returned_units"], returns["mature_units"]), f"{number(returns['returned_units'])} / {number(returns['mature_units'])} unidades maduras")]
        cutoff = meta["cutoff"].strftime("%d/%m/%Y %H:%M UTC") if meta["cutoff"] else "sin eventos"
        batch = latest[0]["batch_id"] if latest else "sin lote publicado"
        status = [html.Span(className="status-dot"), html.Strong("Datos conectados"), html.Span(f"{number(meta['records'])} eventos persistidos"),
                  html.Span(f"Corte de fuente: {cutoff}"), html.Span(f"Lote: {batch}"), html.Span(f"Lectura: {datetime.now(timezone.utc).strftime('%H:%M:%S')} UTC")]
        return *figures, kpis, status
    except (psycopg2.Error, ValueError, KeyError) as exc:
        message = "Datos no disponibles. Comprueba la base y ejecuta la carga del pipeline."
        if isinstance(exc, ValueError):
            message = str(exc)
        return *[empty_figure(message) for _ in range(6)], [card("Estado de los datos", "Sin conexión", "Los valores no se sustituyen por datos de ejemplo.")], [html.Span(className="status-dot offline"), message]


def selected_drill(trigger, category_click, product_click, category, current):
    """Estado de navegación: cambiar filtros invalida producto/categoría antiguos."""
    current = current or {"category": None, "product": None}
    if trigger == "category-bar" and category_click and category_click.get("points"):
        point = category_click["points"][0]
        selected = point.get("customdata", [point.get("x")])[0]
        return {"category": selected, "product": None}
    if trigger == "product-bar" and product_click and product_click.get("points"):
        custom = product_click["points"][0].get("customdata", [])
        if custom:
            return {"category": current.get("category") or category, "product": custom[0]}
    return {"category": category or None, "product": None}


@app.callback(Output("drill", "data"), Input("category-bar", "clickData"), Input("product-bar", "clickData"), Input("reset-drill", "n_clicks"),
              Input("date-range", "start_date"), Input("date-range", "end_date"), Input("channel", "value"), Input("category", "value"), State("drill", "data"))
def update_drill(category_click, product_click, _reset, _start, _end, _channel, category, current):
    return selected_drill(ctx.triggered_id, category_click, product_click, category, current)


@app.callback(Output("product-bar", "figure"), Output("orders-table", "data"), Output("drill-breadcrumb", "children"),
              Input("drill", "data"), Input("refresh", "n_intervals"), State("date-range", "start_date"), State("date-range", "end_date"), State("channel", "value"), State("category", "value"))
def refresh_detail(drill, _tick, start, end, channel, category):
    drill = drill or {}
    category = drill.get("category") or category
    product = drill.get("product")
    try:
        with connection() as conn:
            meta = metadata(conn)
            lower, upper = bounds(start, end, meta)
            where, params = line_filter(lower, upper, channel, category)
            products = rows(conn, f"""SELECT product_id,product_name,sum(quantity) AS units FROM ecommerce.fact_event_line l
              WHERE {where} AND event_type='paid' GROUP BY product_id,product_name ORDER BY units DESC LIMIT 20""", params)
            selected_rows = []
            if product:
                selected_rows = rows(conn, f"""WITH selected AS (
                  SELECT order_id,sum(CASE WHEN event_type='paid' THEN net_cents ELSE 0 END)/100.0 AS paid_bob,
                    -sum(CASE WHEN event_type='refunded' THEN net_cents ELSE 0 END)/100.0 AS refund_bob,
                    min(customer_id) AS customer_id,min(channel) AS channel,max(event_time) AS latest
                  FROM ecommerce.fact_event_line l WHERE {where} AND product_id=%s
                    AND event_type IN ('paid','refunded') GROUP BY order_id)
                  SELECT order_id,customer_id,channel,paid_bob,refund_bob FROM selected ORDER BY latest DESC LIMIT 200""", params + [product])
        fig = px.bar(pd.DataFrame(products), x="product_name", y="units", custom_data=["product_id"], labels={"product_name": "Producto · clic para ver pedidos", "units": "Unidades pagadas"}) if products else empty_figure()
        if products:
            fig.update_traces(marker_color=COLORS[0])
            fig = decorate(fig, 330)
        data = [{"id": r["order_id"], "order_id": r["order_id"], "customer_id": r["customer_id"], "channel": CHANNELS.get(r["channel"], r["channel"]),
                 "paid_bob": float(r["paid_bob"]), "refund_bob": float(r["refund_bob"])} for r in selected_rows]
        breadcrumb = f"{category or 'Todas las categorías'}  →  {product or 'Selecciona un producto'}"
        return fig, data, breadcrumb
    except (psycopg2.Error, ValueError):
        return empty_figure("Detalle no disponible"), [], "Esperando datos persistidos"


@app.callback(Output("selected-order", "data"), Input("orders-table", "active_cell"), Input("drill", "data"), State("orders-table", "data"))
def select_order(cell, _drill, table):
    if ctx.triggered_id != "orders-table" or not cell:
        return None
    candidate = cell.get("row_id")
    valid = {row["order_id"] for row in (table or [])}
    return candidate if candidate in valid else None


@app.callback(Output("events-table", "data"), Output("order-heading", "children"), Input("selected-order", "data"), Input("refresh", "n_intervals"))
def refresh_events(order_id, _tick):
    if not order_id:
        return [], "Selecciona un pedido en la tabla."
    try:
        with connection() as conn:
            events = rows(conn, """SELECT event_id,event_type,event_time,total_cents,batch_id
              FROM ecommerce.fact_event WHERE order_id=%s ORDER BY event_time,event_id""", [order_id])
        return [{"event_id": row["event_id"], "event_type": EVENTS.get(row["event_type"], row["event_type"]), "event_time": row["event_time"].astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                 "amount": float(row["total_cents"] / 100), "batch_id": row["batch_id"]} for row in events], f"Pedido {order_id} · historial completo, incluidos eventos fuera del período filtrado."
    except psycopg2.Error:
        return [], f"No se pudo consultar el pedido {order_id}."


@app.callback(Output("operational-kpis", "children"), Output("throughput-line", "figure"), Output("errors-bar", "figure"), Output("latency-hist", "figure"), Output("queue-line", "figure"),
              Output("runs-table", "data"), Output("operations-note", "children"), Input("refresh", "n_intervals"))
def refresh_operations(_tick):
    try:
        with connection() as conn:
            runs = rows(conn, """SELECT batch_id,status,input_count,published_count,rejected_count,pending_count,duplicate_count,
              duration_seconds,throughput_per_second,latency_median_ms,latency_p95_ms,finished_at
              FROM ecommerce.pipeline_runs WHERE started_at >= now()-interval '24 hours' ORDER BY started_at DESC LIMIT 50""")
            publication = rows(conn, """WITH counts AS (
              SELECT date_trunc('minute',persisted_at) AS minute,count(*)/60.0 AS records_per_second
              FROM ecommerce.fact_event WHERE persisted_at >= now()-interval '24 hours' GROUP BY 1
            ), minutes AS (
              SELECT generate_series(min(minute),date_trunc('minute',now()),interval '1 minute') AS minute FROM counts
            ) SELECT m.minute,coalesce(c.records_per_second,0) AS records_per_second
              FROM minutes m LEFT JOIN counts c USING(minute) ORDER BY m.minute""")
            latency = rows(conn, """SELECT extract(epoch FROM persisted_at-ingested_at)*1000 AS ms
              FROM ecommerce.fact_event WHERE persisted_at>=now()-interval '24 hours' ORDER BY persisted_at DESC LIMIT 50000""")
            quality = rows(conn, """SELECT 'Spark / '||stage AS stage,count(*) AS errors
              FROM ecommerce.quality_reject WHERE rejected_at>=now()-interval '24 hours' GROUP BY stage""")
            metrics = rows(conn, """SELECT sampled_at,source,metric,value,labels FROM ecommerce.system_metric
              WHERE sampled_at>=now()-interval '24 hours' ORDER BY sampled_at""")
        latest = next((run for run in runs if run["finished_at"]), None)
        frame_latency = pd.DataFrame(latency)
        if latency:
            frame_latency["ms"] = pd.to_numeric(frame_latency["ms"])
        med = float(frame_latency.ms.median()) if latency else None
        p95 = float(frame_latency.ms.quantile(0.95)) if latency else None
        queue_metrics = [r for r in metrics if r["metric"] in {"queue_flowfiles", "queued_flowfiles", "flowfiles_queued"}]
        last_sample = max((r["sampled_at"] for r in queue_metrics), default=None)
        current_queue = sum(float(r["value"]) for r in queue_metrics if r["sampled_at"] == last_sample) if last_sample else None
        kpis = [card("Rendimiento del último job", f"{number(latest['published_count'] / latest['duration_seconds'], 2)} reg/s" if latest and latest["duration_seconds"] else "Sin datos", "Publicados / duración del job", "accent"),
                card("Latencia mediana", f"{number(med, 0)} ms" if med is not None else "Sin datos", "Desde registro en NiFi hasta persistencia"),
                card("Latencia p95", f"{number(p95, 0)} ms" if p95 is not None else "Sin datos", "Incluye la espera batch"),
                card("Cola de ingesta", f"{number(current_queue)} FlowFiles" if current_queue is not None else "Sin lectura", "Última muestra del colector")]
        throughput = px.line(pd.DataFrame(publication), x="minute", y="records_per_second", labels={"minute": "Hora de publicación · UTC", "records_per_second": "Registros / segundo"}, markers=True) if publication else empty_figure("Sin publicaciones en las últimas 24 h")
        # Collector reports a snapshot counter; use its latest value per source/stage.
        error_counts = {}
        for metric in metrics:
            if metric["metric"] in {"errors", "error_count", "rejected_records", "quality_rejects", "transport_errors", "bulletin_errors"}:
                stage = metric['labels'].get('stage', metric['metric'])
                stage = {"bulletin_errors": "boletines", "http_error": "API", "transport_errors": "transporte"}.get(stage, stage)
                label = f"{metric['source']} / {stage}"
                error_counts[label] = float(metric["value"])
        error_rows = quality + [{"stage": label, "errors": value} for label, value in error_counts.items()]
        errors = px.bar(pd.DataFrame(error_rows), x="stage", y="errors", labels={"stage": "Fuente / etapa", "errors": "Registros o errores"}) if error_rows else empty_figure("Sin errores registrados")
        hist = px.histogram(frame_latency, x="ms", nbins=35, labels={"ms": "Latencia de publicación · ms"}) if latency else empty_figure("Sin registros para calcular latencia")
        hist.update_yaxes(title="Registros")
        if queue_metrics:
            queue_frame = pd.DataFrame(queue_metrics)
            queue_frame["value"] = queue_frame["value"].astype(float)
            queue_frame["Cola"] = queue_frame.apply(lambda row: str(row["labels"].get("connection", row["source"])), axis=1)
            queue = px.line(queue_frame, x="sampled_at", y="value", color="Cola", labels={"sampled_at": "Hora de muestreo · UTC", "value": "FlowFiles"}, markers=True)
        else:
            queue = empty_figure("El colector aún no ha registrado muestras de cola")
        table = [{key: (number(value, 2) if key == "duration_seconds" and value is not None else value) for key, value in run.items() if key not in {"finished_at", "throughput_per_second", "latency_median_ms", "latency_p95_ms"}} for run in runs[:20]]
        sample_text = last_sample.astimezone(timezone.utc).strftime("%d/%m/%Y %H:%M:%S UTC") if last_sample else "sin muestra de cola"
        note = f"Última muestra de cola: {sample_text}. Latencias: {number(len(latency))} registros publicados (máximo 50.000 recientes), desde registro en NiFi hasta persistencia; incluyen la espera batch y excluyen la solicitud HTTP y la cola anterior a Split. El rendimiento cuenta eventos de negocio, no FlowFiles. Los errores NiFi son el último snapshot de boletines del colector; los rechazos Spark son conteos de registros. Los pendientes aguardan su creación o pago antecedente: se reintentan y no generan ingresos ni se cuentan como errores. Fuente histórica y reloj de ingesta se presentan por separado."
        return kpis, decorate(throughput), decorate(errors), decorate(hist), decorate(queue), table, note
    except (psycopg2.Error, ValueError, KeyError):
        message = "Operación no disponible. Comprueba la base y el colector."
        return [card("Estado", "Sin conexión", "No se inventan métricas operacionales.")], *[empty_figure(message) for _ in range(4)], [], message


@server.get("/health")
def health():
    try:
        with connection() as conn:
            count = rows(conn, "SELECT count(*) AS events FROM ecommerce.fact_event")[0]["events"]
        return {"status": "ok", "persisted_events": count}, 200
    except psycopg2.Error:
        return {"status": "database_unavailable"}, 503


if __name__ == "__main__":
    app.run(host=os.getenv("DASH_HOST", "127.0.0.1"), port=int(os.getenv("DASH_PORT", "18050")), debug=False)
