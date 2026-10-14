-- Ingreso por fecha del evento financiero; puede ser negativo en un día.
SELECT (event_time AT TIME ZONE 'UTC')::date AS day,channel,
 coalesce(sum(total_cents) FILTER (WHERE event_type='paid'),0)/100.0 AS gross_bob,
 coalesce(sum(total_cents) FILTER (WHERE event_type='refunded'),0)/100.0 AS refund_bob,
 sum(CASE WHEN event_type='paid' THEN total_cents ELSE -total_cents END)/100.0 AS net_bob,
 count(*) FILTER (WHERE event_type='paid') AS paid_orders
FROM ecommerce.fact_event
WHERE event_type IN ('paid','refunded') AND event_time>=%(start)s AND event_time<%(end)s
 AND event_month>=date_trunc('month',%(start)s::timestamptz AT TIME ZONE 'UTC')::date
 AND event_month<date_trunc('month',(%(end)s::timestamptz-interval '1 microsecond') AT TIME ZONE 'UTC')::date+interval '1 month'
GROUP BY 1,2 ORDER BY 1,2;
