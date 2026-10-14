-- Una fila por pago/pedido; evita multiplicar el ticket al unir líneas.
SELECT channel,count(*) AS paid_orders,sum(total_cents)/100.0 AS gross_bob,
 avg(total_cents)/100.0 AS average_ticket_bob,
 percentile_cont(0.5) WITHIN GROUP(ORDER BY total_cents)/100.0 AS median_ticket_bob
FROM ecommerce.fact_event
WHERE event_type='paid' AND event_time>=%(start)s AND event_time<%(end)s
 AND event_month>=date_trunc('month',%(start)s::timestamptz AT TIME ZONE 'UTC')::date
 AND event_month<date_trunc('month',(%(end)s::timestamptz-interval '1 microsecond') AT TIME ZONE 'UTC')::date+interval '1 month'
GROUP BY channel ORDER BY channel;
