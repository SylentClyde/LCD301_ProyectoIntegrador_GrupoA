-- Unidades pagadas y devueltas separadas; los snapshots conservan categoría de compra.
SELECT category,product_id,min(product_name) AS product_name,
 coalesce(sum(quantity) FILTER(WHERE event_type='paid'),0) AS sold_units,
 coalesce(sum(quantity) FILTER(WHERE event_type='refunded'),0) AS refunded_units,
 sum(CASE WHEN event_type='paid' THEN net_cents ELSE 0 END)/100.0 AS gross_bob,
 -sum(CASE WHEN event_type='refunded' THEN net_cents ELSE 0 END)/100.0 AS refund_bob,
 sum(net_cents)/100.0 AS net_bob
FROM ecommerce.fact_event_line
WHERE event_type IN ('paid','refunded') AND event_time>=%(start)s AND event_time<%(end)s
 AND event_month>=date_trunc('month',%(start)s::timestamptz AT TIME ZONE 'UTC')::date
 AND event_month<date_trunc('month',(%(end)s::timestamptz-interval '1 microsecond') AT TIME ZONE 'UTC')::date+interval '1 month'
GROUP BY 1,2 ORDER BY sold_units DESC,product_id;
