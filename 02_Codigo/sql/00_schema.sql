CREATE SCHEMA IF NOT EXISTS ecommerce;
SET search_path TO ecommerce,public;
CREATE TABLE IF NOT EXISTS dim_product (
 product_id text PRIMARY KEY, name text NOT NULL, category text NOT NULL,
 active boolean NOT NULL, catalog_version integer NOT NULL, list_price_cents bigint NOT NULL CHECK(list_price_cents>=0),
 updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS dim_customer (customer_id text PRIMARY KEY);
CREATE TABLE IF NOT EXISTS pipeline_runs (
 run_id text PRIMARY KEY, batch_id text NOT NULL, started_at timestamptz NOT NULL,
 finished_at timestamptz, status text NOT NULL,
 input_count bigint NOT NULL DEFAULT 0, accepted_count bigint NOT NULL DEFAULT 0,
 rejected_count bigint NOT NULL DEFAULT 0, duplicate_count bigint NOT NULL DEFAULT 0,
 published_count bigint NOT NULL DEFAULT 0, pending_count bigint NOT NULL DEFAULT 0,
 duration_seconds double precision, throughput_per_second double precision,
 latency_median_ms double precision, latency_p95_ms double precision,
 error_message text
);
CREATE TABLE IF NOT EXISTS event_registry (
 event_id text PRIMARY KEY, event_type text NOT NULL CHECK(event_type IN ('created','paid','cancelled','refunded')),
 order_id text NOT NULL, customer_id text NOT NULL REFERENCES dim_customer,
 event_time timestamptz NOT NULL, channel text NOT NULL CHECK(channel IN ('web','movil','tienda')),
 currency text NOT NULL CHECK(currency='BOB'), total_cents bigint NOT NULL CHECK(total_cents>=0),
 original_payment_id text, payload_hash text NOT NULL, items jsonb NOT NULL,
 batch_id text NOT NULL, run_id text NOT NULL REFERENCES pipeline_runs,
 ingested_at timestamptz NOT NULL, persisted_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX IF NOT EXISTS registry_order ON event_registry(order_id,event_time);
CREATE UNIQUE INDEX IF NOT EXISTS one_created_per_order ON event_registry(order_id) WHERE event_type='created';
CREATE UNIQUE INDEX IF NOT EXISTS one_payment_per_order ON event_registry(order_id) WHERE event_type='paid';
CREATE UNIQUE INDEX IF NOT EXISTS one_cancel_per_order ON event_registry(order_id) WHERE event_type='cancelled';
CREATE TABLE IF NOT EXISTS fact_event (
 event_month date NOT NULL, event_id text NOT NULL REFERENCES event_registry,
 event_type text NOT NULL, order_id text NOT NULL, customer_id text NOT NULL,
 event_time timestamptz NOT NULL, channel text NOT NULL, currency text NOT NULL,
 total_cents bigint NOT NULL, original_payment_id text, batch_id text NOT NULL,
 ingested_at timestamptz NOT NULL, persisted_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(event_month,event_id)
) PARTITION BY RANGE(event_month);
CREATE INDEX IF NOT EXISTS fact_event_time_channel ON fact_event(event_time,channel);
CREATE INDEX IF NOT EXISTS fact_event_order ON fact_event(order_id);
CREATE TABLE IF NOT EXISTS fact_event_line (
 event_month date NOT NULL, event_id text NOT NULL REFERENCES event_registry, event_type text NOT NULL,
 order_id text NOT NULL, order_line_id text NOT NULL, customer_id text NOT NULL,
 event_time timestamptz NOT NULL, channel text NOT NULL, product_id text NOT NULL,
 product_name text NOT NULL, category text NOT NULL, catalog_version integer NOT NULL,
 quantity integer NOT NULL CHECK(quantity>0), unit_price_cents bigint NOT NULL,
 discount_cents bigint NOT NULL, quantity_signed integer NOT NULL,
 net_cents bigint NOT NULL, original_payment_id text,
 PRIMARY KEY(event_month,event_id,order_line_id)
) PARTITION BY RANGE(event_month);
CREATE INDEX IF NOT EXISTS event_line_date_product ON fact_event_line(event_time,product_id);
CREATE INDEX IF NOT EXISTS event_line_payment ON fact_event_line(original_payment_id,order_line_id);
CREATE TABLE IF NOT EXISTS quality_reject (
 reject_id bigserial PRIMARY KEY, run_id text NOT NULL REFERENCES pipeline_runs,
 event_id text, reason text NOT NULL, payload jsonb NOT NULL, rejected_at timestamptz NOT NULL DEFAULT now(),
 stage text NOT NULL DEFAULT 'business'
);
CREATE TABLE IF NOT EXISTS pending_event (
 event_id text PRIMARY KEY, payload jsonb NOT NULL, payload_hash text NOT NULL,
 reason text NOT NULL, first_seen_at timestamptz NOT NULL DEFAULT now(), last_seen_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS stage_event (
 run_id text NOT NULL, event_id text, event_type text, order_id text, customer_id text,
 event_time text, channel text, currency text, total_cents bigint,
 original_payment_id text, payload_hash text, items_json text, ingested_at text, source_file text
);
CREATE TABLE IF NOT EXISTS stage_line (
 run_id text NOT NULL, event_id text, order_line_id text, product_id text, quantity integer,
 unit_price_cents bigint, discount_cents bigint, refund_cents bigint,
 product_name text, category text, catalog_version integer
);
CREATE TABLE IF NOT EXISTS system_metric (
 sampled_at timestamptz NOT NULL, source text NOT NULL, metric text NOT NULL,
 value numeric NOT NULL, labels jsonb NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS system_metric_time ON system_metric(sampled_at);
CREATE OR REPLACE VIEW v_daily_sales AS
 SELECT (event_time AT TIME ZONE 'UTC')::date AS day,channel,
 sum(CASE WHEN event_type='paid' THEN total_cents ELSE 0 END)/100.0 AS gross_bob,
 sum(CASE WHEN event_type='refunded' THEN total_cents ELSE 0 END)/100.0 AS refund_bob,
 sum(CASE WHEN event_type='paid' THEN total_cents WHEN event_type='refunded' THEN -total_cents ELSE 0 END)/100.0 AS net_bob,
 count(*) FILTER (WHERE event_type='paid') AS paid_orders
 FROM fact_event GROUP BY 1,2;
CREATE OR REPLACE VIEW v_category_sales AS
 SELECT category,product_id,product_name,channel,
 sum(quantity) FILTER (WHERE event_type='paid') AS sold_units,
 coalesce(sum(quantity) FILTER (WHERE event_type='refunded'),0) AS refunded_units,
 sum(CASE WHEN event_type='paid' THEN net_cents ELSE 0 END)/100.0 AS gross_bob,
 -sum(CASE WHEN event_type='refunded' THEN net_cents ELSE 0 END)/100.0 AS refund_bob,
 sum(net_cents)/100.0 AS net_bob FROM fact_event_line
 WHERE event_type IN ('paid','refunded') GROUP BY 1,2,3,4;
CREATE OR REPLACE VIEW v_order_summary AS
 SELECT order_id,min(customer_id) AS customer_id,min(channel) AS channel,
 min(event_time) FILTER (WHERE event_type='created') AS created_at,
 min(event_time) FILTER (WHERE event_type='paid') AS paid_at,
 min(event_time) FILTER (WHERE event_type='cancelled') AS cancelled_at,
 max(event_time) FILTER (WHERE event_type='refunded') AS last_refund_at,
 max(total_cents) FILTER (WHERE event_type='paid')/100.0 AS paid_bob,
 coalesce(sum(total_cents) FILTER (WHERE event_type='refunded'),0)/100.0 AS refund_bob,
 count(*) FILTER (WHERE event_type='refunded') AS refund_events
 FROM fact_event GROUP BY order_id;
CREATE OR REPLACE VIEW v_customer_value AS
 SELECT customer_id,count(DISTINCT order_id) FILTER (WHERE event_type='paid') AS paid_orders,
 sum(CASE WHEN event_type='paid' THEN total_cents WHEN event_type='refunded' THEN -total_cents ELSE 0 END)/100.0 AS net_bob,
 min(event_time) FILTER (WHERE event_type='paid') AS first_paid_at,
 max(event_time) FILTER (WHERE event_type='paid') AS last_paid_at FROM fact_event GROUP BY customer_id;
CREATE OR REPLACE VIEW v_operational_metrics AS
 SELECT *,published_count/nullif(duration_seconds,0) AS published_per_second FROM pipeline_runs;
