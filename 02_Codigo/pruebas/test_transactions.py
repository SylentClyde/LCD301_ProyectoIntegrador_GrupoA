"""Fallo real entre registro y hechos, rollback, reintento y duplicado en DB aislada."""
import argparse
import copy
import hashlib
import json
import sys
import uuid
from datetime import datetime,timezone
from pathlib import Path
from urllib.parse import urlparse,urlunparse
import psycopg
from psycopg import sql
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'procesamiento'))
from database import connect,initialize,publish


def test_transactions(db_url,output):
    parsed=urlparse(db_url)
    name='lcd301_tx_'+uuid.uuid4().hex[:10]
    test_url=urlunparse(parsed._replace(path='/'+name))
    admin=urlunparse(parsed._replace(path='/postgres'))
    with psycopg.connect(admin,autocommit=True) as conn:
        conn.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(name)))
    initialize(test_url)
    line=dict(order_line_id='L1',product_id='PRD001',quantity=2,unit_price_cents=5000,discount_cents=0,refund_cents=None)
    created=dict(event_id='TX-C',event_type='created',order_id='TX-ORDER',customer_id='TX-CUSTOMER',event_time='2026-04-30T23:59:00Z',channel='web',currency='BOB',total_cents=10000,original_payment_id=None,items=[line])
    paid=copy.deepcopy(created)
    paid.update(event_id='TX-P',event_type='paid',event_time='2026-05-01T00:01:00Z')
    refund=copy.deepcopy(paid)
    refund.update(event_id='TX-R',event_type='refunded',event_time='2026-05-02T00:00:00Z',total_cents=5000,original_payment_id='TX-P')
    refund['items'][0].update(quantity=1,refund_cents=5000)
    with connect(test_url) as conn:
        conn.execute("INSERT INTO dim_product VALUES('PRD001','Control','hogar',true,1,5000,now())")
        conn.execute("INSERT INTO pipeline_runs(run_id,batch_id,started_at,status) VALUES('tx-run','tx',now(),'running')")
        for e in (created,paid,refund):
            payload_hash=hashlib.sha256(json.dumps(e,sort_keys=True).encode()).hexdigest()
            conn.execute('INSERT INTO stage_event(run_id,event_id,event_type,order_id,customer_id,event_time,channel,currency,total_cents,original_payment_id,payload_hash,items_json,ingested_at,source_file) VALUES('+','.join(['%s']*14)+')',
                ['tx-run',*[e[k] for k in ('event_id','event_type','order_id','customer_id','event_time','channel','currency','total_cents','original_payment_id')],payload_hash,json.dumps(e['items']),datetime.now(timezone.utc).isoformat(),'test_transactions'])
            for item in e['items']:
                conn.execute('INSERT INTO stage_line(run_id,event_id,order_line_id,product_id,quantity,unit_price_cents,discount_cents,refund_cents,product_name,category,catalog_version) VALUES('+','.join(['%s']*11)+')',
                    ['tx-run',e['event_id'],*[item[k] for k in ('order_line_id','product_id','quantity','unit_price_cents','discount_cents','refund_cents')],'Control','hogar',1])
    try:
        publish(test_url,'tx-run','tx',failure_injection=True)
        raise AssertionError('La inyección de fallo no ocurrió')
    except RuntimeError as exc:
        assert 'Prueba controlada' in str(exc)
    with connect(test_url) as conn:
        empty=conn.execute('SELECT (SELECT count(*) FROM event_registry) registry,(SELECT count(*) FROM fact_event) facts,(SELECT count(*) FROM fact_event_line) lines').fetchone()
        assert empty==dict(registry=0,facts=0,lines=0),empty
    retry=publish(test_url,'tx-run','tx')
    assert retry['published']==3 and retry['rejected']==0,retry
    repeat=publish(test_url,'tx-run','tx')
    assert repeat['published']==0 and repeat['duplicate']==3,repeat
    with connect(test_url) as conn:
        total=conn.execute("SELECT sum(CASE WHEN event_type='paid' THEN total_cents WHEN event_type='refunded' THEN -total_cents ELSE 0 END) net_cents,count(*) events FROM fact_event").fetchone()
        assert total==dict(net_cents=5000,events=3),total
        partitions=conn.execute('SELECT tableoid::regclass::text partition,count(*) rows FROM fact_event GROUP BY tableoid ORDER BY 1').fetchall()
        assert len(partitions)==2,partitions
    report=dict(passed=True,test_database=name,rollback=empty,retry=retry,repeat=repeat,financial=total,partitions=partitions,verified_at=datetime.now(timezone.utc).isoformat())
    output.parent.mkdir(parents=True,exist_ok=True)
    output.write_text(json.dumps(report,ensure_ascii=False,indent=2,default=str),encoding='utf-8')
    print(json.dumps(report,ensure_ascii=False,default=str))
    return report


if __name__=='__main__':
    p=argparse.ArgumentParser()
    p.add_argument('--db-url',default='postgresql://lcd301:lcd301_local@127.0.0.1:55432/lcd301')
    p.add_argument('--output',required=True,type=Path)
    a=p.parse_args()
    test_transactions(a.db_url,a.output)
