-- Cohorte de creación; cada pedido es un denominador, corte temporal explícito.
WITH created AS (
 SELECT order_id,channel,event_time AS created_at FROM ecommerce.fact_event
 WHERE event_type='created' AND event_time>=%(start)s AND event_time<%(end)s AND event_time<%(as_of)s
), cancelled AS (
 SELECT order_id FROM ecommerce.fact_event WHERE event_type='cancelled' AND event_time<%(as_of)s
)
SELECT date_trunc('month',created_at AT TIME ZONE 'UTC')::date AS creation_month,channel,
 count(*) AS created_orders,count(cancelled.order_id) AS cancelled_orders,
 count(cancelled.order_id)::numeric/nullif(count(*),0) AS cancellation_rate,
 %(as_of)s::timestamptz AS cutoff_at
FROM created LEFT JOIN cancelled USING(order_id) GROUP BY 1,2 ORDER BY 1,2;
