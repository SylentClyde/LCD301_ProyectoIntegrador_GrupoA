"""Plan real y equivalencia independiente del filtro de cancelaciones del dashboard."""
import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'procesamiento'))
from database import connect

OLD_SQL="""SELECT count(*) AS created_orders,
 count(*) FILTER(WHERE cancelled_at IS NOT NULL AND paid_at IS NULL) AS cancelled_orders
 FROM ecommerce.v_order_summary o WHERE o.created_at >= %s::timestamptz
 AND o.created_at < %s::timestamptz AND o.channel=%s
 AND EXISTS(SELECT 1 FROM ecommerce.fact_event_line cl
 WHERE cl.order_id=o.order_id AND cl.event_type='created' AND cl.category=%s)"""
NEW_SQL="""SELECT count(*) AS created_orders,
 count(*) FILTER(WHERE c.event_id IS NOT NULL AND p.event_id IS NULL) AS cancelled_orders
 FROM ecommerce.event_registry o
 LEFT JOIN ecommerce.event_registry c ON c.order_id=o.order_id AND c.event_type='cancelled'
 LEFT JOIN ecommerce.event_registry p ON p.order_id=o.order_id AND p.event_type='paid'
 WHERE o.event_type='created' AND o.event_time >= %s::timestamptz
 AND o.event_time < %s::timestamptz AND o.channel=%s
 AND o.order_id IN(SELECT cl.order_id FROM ecommerce.fact_event_line cl
 WHERE cl.event_type='created' AND cl.category=%s)"""


def verify(db_url, output):
    output.mkdir(parents=True,exist_ok=True)
    cases=[]
    with connect(db_url) as conn:
        conn.execute('SET TRANSACTION READ ONLY')
        conn.execute("SET LOCAL statement_timeout='12s'")
        categories=[row['category'] for row in conn.execute('SELECT DISTINCT category FROM dim_product ORDER BY 1')]
        params=('2026-04-01T00:00:00Z','2026-10-01T00:00:00Z','web','ropa')
        old_plan=conn.execute('EXPLAIN (FORMAT JSON) '+OLD_SQL,params).fetchone()['QUERY PLAN']
        (output/'cancellation_old_plan.json').write_text(json.dumps(old_plan,indent=2),encoding='utf-8')
        fixed_estimated=conn.execute('EXPLAIN (FORMAT JSON) '+NEW_SQL,params).fetchone()['QUERY PLAN']
        (output/'cancellation_fixed_estimated_plan.json').write_text(json.dumps(fixed_estimated,indent=2),encoding='utf-8')
        new_plan=conn.execute('EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) '+NEW_SQL,params).fetchone()['QUERY PLAN']
        (output/'cancellation_fixed_plan.json').write_text(json.dumps(new_plan,indent=2),encoding='utf-8')
        # Referencia independiente: materializa la semántica de la vista original, sin su filtro lento.
        summaries=conn.execute('SELECT * FROM ecommerce.v_order_summary').fetchall()
        category_orders={category:set() for category in categories}
        for row in conn.execute("SELECT DISTINCT order_id,category FROM ecommerce.fact_event_line WHERE event_type='created'"):
            category_orders[row['category']].add(row['order_id'])
        lower=datetime(2026,4,1,tzinfo=timezone.utc)
        upper=datetime(2026,10,1,tzinfo=timezone.utc)
        for channel in ('web','movil','tienda'):
            for category in categories:
                params=('2026-04-01T00:00:00Z','2026-10-01T00:00:00Z',channel,category)
                begin=time.perf_counter()
                actual=conn.execute(NEW_SQL,params).fetchone()
                milliseconds=(time.perf_counter()-begin)*1000
                selected=[row for row in summaries if row['created_at'] is not None
                    and lower<=row['created_at']<upper and row['channel']==channel
                    and row['order_id'] in category_orders[category]]
                expected=dict(created_orders=len(selected),cancelled_orders=sum(
                    row['cancelled_at'] is not None and row['paid_at'] is None for row in selected))
                assert actual==expected,(params,actual,expected)
                assert milliseconds<12000,(params,milliseconds)
                cases.append(dict(channel=channel,category=category,result=actual,client_ms=milliseconds))
        report=dict(passed=True,verified_at=datetime.now(timezone.utc).isoformat(),
            application_timeout_ms=12000,query_change='Creation cohort filtered before aggregation; joins unique paid/cancelled events through existing indexes',
            diagnostic_old_plan_analyzed=False,fixed_server_execution_ms=new_plan[0]['Execution Time'],
            maximum_client_ms=max(case['client_ms'] for case in cases),
            cases=cases,no_fact_data_mutations=True,no_indexes_changed=True)
    (output/'verification.json').write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf-8')
    print(json.dumps(report,ensure_ascii=False))
    return report


if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--db-url',default='postgresql://lcd301:lcd301_local@127.0.0.1:55432/lcd301')
    parser.add_argument('--output',required=True,type=Path)
    args=parser.parse_args()
    verify(args.db_url,args.output)
