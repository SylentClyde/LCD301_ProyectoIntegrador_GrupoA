-- Denominador de cohortes maduras: pago + 30 días <= corte. La fecha es la del pago.
WITH mature_paid AS (
 SELECT event_id,order_line_id,product_id,product_name,category,quantity,event_time
 FROM ecommerce.fact_event_line WHERE event_type='paid'
 AND event_time>=%(start)s AND event_time<%(end)s AND event_time+interval '30 days'<=%(as_of)s
), returned AS (
 SELECT p.event_id,p.order_line_id,sum(r.quantity) AS returned_30d_units
 FROM mature_paid p LEFT JOIN ecommerce.fact_event_line r
 ON r.event_type='refunded' AND r.original_payment_id=p.event_id AND r.order_line_id=p.order_line_id
 AND r.event_time>=p.event_time AND r.event_time<=p.event_time+interval '30 days' AND r.event_time<%(as_of)s
 GROUP BY p.event_id,p.order_line_id
)
SELECT p.product_id,min(p.product_name) AS product_name,p.category,
 sum(p.quantity) AS mature_paid_units,coalesce(sum(r.returned_30d_units),0) AS returned_30d_units,
 coalesce(sum(r.returned_30d_units),0)::numeric/nullif(sum(p.quantity),0) AS return_rate_30d,
 %(as_of)s::timestamptz AS cutoff_at
FROM mature_paid p LEFT JOIN returned r USING(event_id,order_line_id)
GROUP BY p.product_id,p.category ORDER BY return_rate_30d DESC,p.product_id;
