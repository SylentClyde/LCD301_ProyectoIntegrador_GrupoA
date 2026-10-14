-- Valor neto observado del período (no predicción CLV). Tasa entre clientes pagadores.
WITH customers AS (
 SELECT customer_id,count(*) FILTER(WHERE event_type='paid') AS paid_orders,
 sum(CASE WHEN event_type='paid' THEN total_cents WHEN event_type='refunded' THEN -total_cents ELSE 0 END)/100.0 AS net_bob
 FROM ecommerce.fact_event WHERE event_time>=%(start)s AND event_time<%(end)s GROUP BY customer_id
), repeat_rate AS (
 SELECT count(*) FILTER(WHERE paid_orders>=2)::numeric/nullif(count(*) FILTER(WHERE paid_orders>=1),0) AS repeat_purchase_rate
 FROM customers
)
SELECT customer_id,paid_orders,net_bob,repeat_purchase_rate
FROM customers CROSS JOIN repeat_rate WHERE paid_orders>0 ORDER BY net_bob DESC,customer_id;
