"""Publicación atómica, registro global e invariantes del ciclo del pedido."""
import json
from datetime import datetime, timezone, date
from pathlib import Path
import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb


def connect(db_url):
    return psycopg.connect(db_url, row_factory=dict_row,
                           options='-c search_path=ecommerce,public -c timezone=UTC')


def initialize(db_url):
    schema = Path(__file__).resolve().parents[1] / 'sql' / '00_schema.sql'
    with connect(db_url) as conn:
        conn.execute(schema.read_text(encoding='utf-8'))


def utc_time(value):
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc)
    return datetime.fromisoformat(str(value).replace('Z', '+00:00')).astimezone(timezone.utc)


def create_partitions(conn, months):
    for month in sorted(set(months)):
        last = date(month.year + (month.month == 12), month.month % 12 + 1, 1)
        suffix = month.strftime('%Y%m')
        for table in ('fact_event', 'fact_event_line'):
            conn.execute(f"CREATE TABLE IF NOT EXISTS ecommerce.{table}_{suffix} "
                         f"PARTITION OF ecommerce.{table} FOR VALUES FROM ('{month}') TO ('{last}')")


def same_lines(left, right):
    keys = ('order_line_id', 'product_id', 'quantity', 'unit_price_cents', 'discount_cents')
    def canonical(items):
        return sorted(tuple(item[k] for k in keys) for item in items)
    return canonical(left) == canonical(right)


def decide(event, registry, order_events, paid_lines, returned):
    """Devuelve accepted/rejected/pending y snapshots de línea para publicación."""
    event_id, kind, order = event['event_id'], event['event_type'], event['order_id']
    if event_id in registry:
        return ('duplicate' if registry[event_id]['payload_hash'] == event['payload_hash']
                else 'rejected'), 'event_id_conflicting_payload', []
    previous = order_events.get(order, [])
    created = next((x for x in previous if x['event_type'] == 'created'), None)
    payment = next((x for x in previous if x['event_type'] == 'paid'), None)
    cancelled = next((x for x in previous if x['event_type'] == 'cancelled'), None)
    event_dt = utc_time(event['event_time'])
    if kind == 'created':
        if created:
            return 'rejected', 'second_created_event_for_order', []
        return 'accepted', None, event['_lines']
    if created is None:
        return 'pending', 'missing_created_event', []
    if event_dt < utc_time(created['event_time']):
        return 'rejected', 'event_before_order_creation', []
    if (event['customer_id'], event['channel'], event['currency']) != (
            created['customer_id'], created['channel'], created['currency']):
        return 'rejected', 'order_customer_channel_currency_mismatch', []
    if kind == 'cancelled':
        if payment or cancelled:
            return 'rejected', 'cancel_requires_unpaid_uncancelled_order', []
        if event['total_cents'] != 0:
            return 'rejected', 'cancel_must_have_zero_amount', []
        return 'accepted', None, []
    if kind == 'paid':
        if payment or cancelled:
            return 'rejected', 'payment_requires_unpaid_uncancelled_order', []
        if event['total_cents'] != created['total_cents'] or not same_lines(event['items'], created['items']):
            return 'rejected', 'payment_does_not_match_created_order', []
        return 'accepted', None, event['_lines']
    payment = registry.get(event['original_payment_id'])
    if payment is None:
        return 'pending', 'missing_original_payment', []
    if payment['event_type'] != 'paid' or payment['order_id'] != order:
        return 'rejected', 'refund_payment_reference_mismatch', []
    if event_dt < utc_time(payment['event_time']):
        return 'rejected', 'refund_before_payment', []
    snapshots = []
    for item in event['items']:
        key = (payment['event_id'], item['order_line_id'])
        original = paid_lines.get(key)
        if original is None or original['product_id'] != item['product_id']:
            return 'rejected', 'refund_line_reference_mismatch', []
        prev_qty, prev_cents = returned.get(key, (0, 0))
        qty, cents = prev_qty + item['quantity'], prev_cents + item['refund_cents']
        if qty > original['quantity']:
            return 'rejected', 'refund_quantity_exceeds_paid_quantity', []
        allowed = original['net_cents'] * qty // original['quantity']
        if cents > allowed:
            return 'rejected', 'refund_amount_exceeds_proportional_paid_amount', []
        if item['unit_price_cents'] != original['unit_price_cents']:
            return 'rejected', 'refund_unit_price_differs_from_purchase', []
        snapshot = dict(original)
        snapshot.update(quantity=item['quantity'], refund_cents=item['refund_cents'])
        snapshots.append(snapshot)
    return 'accepted', None, snapshots


def publish(db_url, run_id, batch_id, failure_injection=False):
    counts = dict(published=0, rejected=0, duplicate=0, pending=0)
    with connect(db_url) as conn:
        conn.execute('SELECT pg_advisory_xact_lock(3012026)')
        registry = {row['event_id']: row for row in conn.execute('SELECT * FROM event_registry')}
        orders = {}
        for row in registry.values():
            orders.setdefault(row['order_id'], []).append(row)
        paid_lines, returned = {}, {}
        for row in conn.execute("SELECT * FROM fact_event_line WHERE event_type IN ('paid','refunded')"):
            if row['event_type'] == 'paid':
                paid_lines[(row['event_id'], row['order_line_id'])] = row
            else:
                key = (row['original_payment_id'], row['order_line_id'])
                qty, cents = returned.get(key, (0, 0))
                returned[key] = (qty + row['quantity'], cents - row['net_cents'])
        staged_lines = {}
        for row in conn.execute('SELECT * FROM stage_line WHERE run_id=%s', (run_id,)):
            staged_lines.setdefault(row['event_id'], []).append(row)
        events, pending_conflicts = {}, []
        for row in conn.execute('SELECT * FROM pending_event'):
            events[row['event_id']] = row['payload']
        for row in conn.execute('SELECT * FROM stage_event WHERE run_id=%s', (run_id,)):
            event = dict(row)
            event['items'] = json.loads(event.pop('items_json'))
            event['_lines'] = staged_lines.get(event['event_id'], [])
            previous_pending = events.get(event['event_id'])
            if previous_pending is not None:
                if previous_pending['payload_hash'] != event['payload_hash']:
                    pending_conflicts.append((run_id,event['event_id'],
                        'pending_event_id_conflicting_payload',Jsonb(event),'business'))
                # La primera versión conserva contenido y marca de ingesta mientras espera.
                continue
            events[event['event_id']] = event
        with conn.cursor() as cursor:
            cursor.executemany('INSERT INTO dim_customer(customer_id) VALUES(%s) ON CONFLICT DO NOTHING',
                               [(e['customer_id'],) for e in events.values()])
        accepted, fact_rows, line_rows, pending, removed = [], [], [], [], []
        rejects = pending_conflicts
        counts['rejected'] += len(pending_conflicts)
        priority = {'created': 0, 'paid': 1, 'cancelled': 2, 'refunded': 3}
        for event in sorted(events.values(), key=lambda e: (utc_time(e['event_time']), priority[e['event_type']], e['event_id'])):
            status, reason, snapshots = decide(event, registry, orders, paid_lines, returned)
            eid = event['event_id']
            if status == 'duplicate':
                counts['duplicate'] += 1
                removed.append((eid,))
                continue
            if status == 'rejected':
                counts['rejected'] += 1
                rejects.append((run_id, eid, reason, Jsonb(event), 'business'))
                removed.append((eid,))
                continue
            if status == 'pending':
                counts['pending'] += 1
                pending.append((eid, Jsonb(event), event['payload_hash'], reason))
                continue
            counts['published'] += 1
            timestamp = utc_time(event['event_time'])
            month = date(timestamp.year, timestamp.month, 1)
            ingested = utc_time(event['ingested_at'])
            kind = event['event_type']
            accepted.append((eid,kind,event['order_id'],event['customer_id'],timestamp,event['channel'],
                             event['currency'],event['total_cents'],event['original_payment_id'],event['payload_hash'],
                             Jsonb(event['items']),batch_id,run_id,ingested))
            fact_rows.append((month,eid,kind,event['order_id'],event['customer_id'],timestamp,event['channel'],
                              event['currency'],event['total_cents'],event['original_payment_id'],batch_id,ingested))
            removed.append((eid,))
            registry[eid] = event
            orders.setdefault(event['order_id'], []).append(event)
            for item in snapshots:
                qty = item['quantity']
                net = (qty * item['unit_price_cents'] - item['discount_cents']) if kind in ('created','paid') else -item['refund_cents']
                signed_qty = qty if kind == 'paid' else -qty if kind == 'refunded' else 0
                line_rows.append((month,eid,kind,event['order_id'],item['order_line_id'],event['customer_id'],timestamp,
                                  event['channel'],item['product_id'],item['product_name'],item['category'],item['catalog_version'],
                                  qty,item['unit_price_cents'],item['discount_cents'],signed_qty,net if kind!='created' else 0,
                                  event['original_payment_id']))
                if kind == 'paid':
                    original = dict(item)
                    original['net_cents'] = net
                    paid_lines[(eid,item['order_line_id'])] = original
                elif kind == 'refunded':
                    key = (event['original_payment_id'],item['order_line_id'])
                    old_qty, old_cents = returned.get(key, (0,0))
                    returned[key] = old_qty+qty,old_cents+item['refund_cents']
        create_partitions(conn, [row[0] for row in fact_rows])
        with conn.cursor() as cursor:
            cursor.executemany('INSERT INTO event_registry(event_id,event_type,order_id,customer_id,event_time,channel,currency,total_cents,original_payment_id,payload_hash,items,batch_id,run_id,ingested_at) VALUES('+','.join(['%s']*14)+')', accepted)
            if failure_injection:
                raise RuntimeError('Prueba controlada: fallo después del registro y antes de hechos')
            cursor.executemany('INSERT INTO fact_event(event_month,event_id,event_type,order_id,customer_id,event_time,channel,currency,total_cents,original_payment_id,batch_id,ingested_at) VALUES('+','.join(['%s']*12)+')', fact_rows)
            cursor.executemany('INSERT INTO fact_event_line(event_month,event_id,event_type,order_id,order_line_id,customer_id,event_time,channel,product_id,product_name,category,catalog_version,quantity,unit_price_cents,discount_cents,quantity_signed,net_cents,original_payment_id) VALUES('+','.join(['%s']*18)+')', line_rows)
            cursor.executemany('INSERT INTO quality_reject(run_id,event_id,reason,payload,stage) VALUES(%s,%s,%s,%s,%s)', rejects)
            cursor.executemany('DELETE FROM pending_event WHERE event_id=%s', removed)
            cursor.executemany('INSERT INTO pending_event(event_id,payload,payload_hash,reason) VALUES(%s,%s,%s,%s) ON CONFLICT(event_id) DO UPDATE SET payload=excluded.payload,reason=excluded.reason,last_seen_at=now()', pending)
        conn.execute('UPDATE pipeline_runs SET published_count=%s,pending_count=%s,rejected_count=rejected_count+%s,duplicate_count=duplicate_count+%s WHERE run_id=%s',
                     (counts['published'],counts['pending'],counts['rejected'],counts['duplicate'],run_id))
    return counts
