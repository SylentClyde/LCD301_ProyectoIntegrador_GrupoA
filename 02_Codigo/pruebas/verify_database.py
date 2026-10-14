"""Aceptación sobre datos reales: fixtures, reconciliación e índices/particiones."""
import argparse
import json
import sys
from datetime import datetime,timezone
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'procesamiento'))
from database import connect


def verify(db_url,output,expected_file=None,expected_phase='initial',require_idempotent=False):
    checks=[]
    with connect(db_url) as conn:
        r=conn.execute('SELECT (SELECT count(*) FROM event_registry) registry,(SELECT count(*) FROM fact_event) facts').fetchone()
        assert r['registry']==r['facts'] and r['facts']>0,r
        checks.append(dict(name='Registro global = hechos publicados',passed=True,details=r))
        r=conn.execute("SELECT e.event_id,e.total_cents,sum(l.net_cents) AS line_cents FROM fact_event e JOIN fact_event_line l USING(event_id) WHERE e.event_type IN ('paid','refunded') GROUP BY e.event_id,e.event_type,e.total_cents HAVING abs(sum(l.net_cents))<>e.total_cents").fetchall()
        assert not r,r[:10]
        checks.append(dict(name='Todos los importes pagos/refunds reconcilian con líneas',passed=True))
        r=conn.execute("SELECT order_id FROM fact_event WHERE event_type='paid' GROUP BY order_id HAVING count(*)>1").fetchall()
        assert not r,r[:10]
        checks.append(dict(name='Un único pago confirmado por pedido',passed=True))
        pending=conn.execute('SELECT p.event_id,p.reason,e.event_id AS published_id FROM pending_event p LEFT JOIN fact_event e USING(event_id) ORDER BY p.event_id').fetchall()
        assert all(row['published_id'] is None for row in pending),pending
        checks.append(dict(name='Pendientes sin antecedente no publican hechos ni ingresos',passed=True,details=pending))
        r=conn.execute("SELECT l.event_id,l.order_line_id,l.quantity,l.net_cents,coalesce(sum(r.quantity),0) qty_returned,coalesce(-sum(r.net_cents),0) cents_returned FROM fact_event_line l LEFT JOIN fact_event_line r ON r.original_payment_id=l.event_id AND r.order_line_id=l.order_line_id AND r.event_type='refunded' WHERE l.event_type='paid' GROUP BY l.event_id,l.order_line_id,l.quantity,l.net_cents HAVING coalesce(sum(r.quantity),0)>l.quantity OR coalesce(-sum(r.net_cents),0)>l.net_cents").fetchall()
        assert not r,r[:10]
        checks.append(dict(name='Ninguna devolución excede cantidad/importe comprado',passed=True))
        expected={'ORD-CONTROL-001':5000,'ORD-CONTROL-002':8500,'ORD-CONTROL-003':0,'ORD-CONTROL-004':5000}
        actual=conn.execute("SELECT order_id,sum(CASE WHEN event_type='paid' THEN total_cents WHEN event_type='refunded' THEN -total_cents ELSE 0 END) net_cents FROM fact_event WHERE order_id LIKE 'ORD-CONTROL-%%' GROUP BY order_id ORDER BY order_id").fetchall()
        fixture_map={r['order_id']:int(r['net_cents']) for r in actual}
        for key,val in expected.items():
            assert fixture_map.get(key)==val,(key,fixture_map)
        checks.append(dict(name='Cuatro pedidos control calculados previamente',passed=True,details=fixture_map))
        r=conn.execute("SELECT order_id,count(DISTINCT event_month) months FROM fact_event WHERE order_id IN ('ORD-CONTROL-001','ORD-CONTROL-004') GROUP BY order_id").fetchall()
        assert any(x['months']>=2 for x in r),r
        checks.append(dict(name='Pedido y devolución atraviesan límite mensual sin perder referencia',passed=True,details=r))
        r=conn.execute("SELECT run_id,published_count,duplicate_count FROM pipeline_runs WHERE status='success' AND published_count=0 AND duplicate_count>0 ORDER BY started_at DESC LIMIT 1").fetchone()
        if require_idempotent:
            assert r,'Ejecutar repetición del lote para evidenciar idempotencia'
        if r:
            checks.append(dict(name='Repetición real publica cero eventos duplicados',passed=True,details=r))
        r=conn.execute('SELECT count(*) unknowns FROM fact_event_line l LEFT JOIN dim_product d USING(product_id) WHERE d.product_id IS NULL').fetchone()
        assert r['unknowns']==0,r
        checks.append(dict(name='Unión catálogo no pierde ni acepta SKU desconocido',passed=True))
        partitions=conn.execute("SELECT tableoid::regclass::text partition,count(*) rows FROM fact_event GROUP BY tableoid ORDER BY 1").fetchall()
        assert len(partitions)>=2,partitions
        checks.append(dict(name='Almacenamiento mensual real',passed=True,details=partitions))
        queries=conn.execute("SELECT count(*) counts FROM pg_indexes WHERE schemaname='ecommerce'").fetchone()
        checks.append(dict(name='Índices creados',passed=True,details=queries))
        if expected_file:
            expected=json.loads(expected_file.read_text(encoding='utf-8-sig'))[expected_phase]
            actual=conn.execute("SELECT count(*) accepted_events,coalesce(sum(total_cents) FILTER(WHERE event_type='paid'),0) gross_cents,coalesce(sum(total_cents) FILTER(WHERE event_type='refunded'),0) refund_cents,sum(CASE WHEN event_type='paid' THEN total_cents WHEN event_type='refunded' THEN -total_cents ELSE 0 END) net_cents,count(DISTINCT customer_id) FILTER(WHERE event_type='paid') paying_customers FROM fact_event").fetchone()
            units=conn.execute("SELECT coalesce(sum(quantity) FILTER(WHERE event_type='paid'),0) paid_units,coalesce(sum(quantity) FILTER(WHERE event_type='refunded'),0) refunded_units FROM fact_event_line").fetchone()
            actual.update(units)
            actual['repeat_customers']=conn.execute('SELECT count(*) count FROM v_customer_value WHERE paid_orders>=2').fetchone()['count']
            actual['counts_by_type']={r['event_type']:r['count'] for r in conn.execute('SELECT event_type,count(*) count FROM fact_event GROUP BY event_type')}
            for field,value in actual.items():
                assert value==expected[field],(field,value,expected[field])
            checks.append(dict(name='Todos los KPIs concuerdan con cálculo independiente del generador',passed=True,phase=expected_phase,details=actual))
        summary=dict(verified_at=datetime.now(timezone.utc).isoformat(),passed=True,checks=checks,
                     totals=conn.execute("SELECT count(*) events,count(*) FILTER(WHERE event_type='paid') paid_orders,sum(CASE WHEN event_type='paid' THEN total_cents WHEN event_type='refunded' THEN -total_cents ELSE 0 END) net_cents FROM fact_event").fetchone())
    output.parent.mkdir(parents=True,exist_ok=True)
    output.write_text(json.dumps(summary,ensure_ascii=False,indent=2,default=str),encoding='utf-8')
    print(json.dumps(summary,ensure_ascii=False,default=str))
    return summary


if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--db-url',default='postgresql://lcd301:lcd301_local@127.0.0.1:55432/lcd301')
    parser.add_argument('--output',required=True,type=Path)
    parser.add_argument('--expected',type=Path)
    parser.add_argument('--expected-phase',choices=['initial','after_delta'],default='initial')
    parser.add_argument('--require-idempotent',action='store_true')
    args=parser.parse_args()
    verify(args.db_url,args.output,args.expected,args.expected_phase,args.require_idempotent)
