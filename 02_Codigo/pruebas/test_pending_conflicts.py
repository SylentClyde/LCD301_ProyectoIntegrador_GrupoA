"""Un ID pendiente conserva su contenido original y se publica al llegar su antecedente."""
import argparse
import copy
import hashlib
import json
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse, urlunparse
import psycopg
from psycopg import sql
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'procesamiento'))
from database import connect, initialize, publish


def stage(db_url, run_id, event, ingested_at):
    with connect(db_url) as conn:
        conn.execute('INSERT INTO pipeline_runs(run_id,batch_id,started_at,status) VALUES(%s,%s,now(),%s)',
                     (run_id,'pending-test','running'))
        payload_hash=hashlib.sha256(json.dumps(event,sort_keys=True).encode()).hexdigest()
        conn.execute('INSERT INTO stage_event(run_id,event_id,event_type,order_id,customer_id,event_time,channel,currency,total_cents,original_payment_id,payload_hash,items_json,ingested_at,source_file) VALUES('+','.join(['%s']*14)+')',
            [run_id,*[event[k] for k in ('event_id','event_type','order_id','customer_id','event_time','channel','currency','total_cents','original_payment_id')],payload_hash,json.dumps(event['items']),ingested_at,'test_pending_conflicts'])
        for item in event['items']:
            conn.execute('INSERT INTO stage_line(run_id,event_id,order_line_id,product_id,quantity,unit_price_cents,discount_cents,refund_cents,product_name,category,catalog_version) VALUES('+','.join(['%s']*11)+')',
                [run_id,event['event_id'],*[item[k] for k in ('order_line_id','product_id','quantity','unit_price_cents','discount_cents','refund_cents')],'Control','hogar',1])


def test_pending(db_url, output):
    parsed=urlparse(db_url)
    name='lcd301_pending_'+uuid.uuid4().hex[:10]
    test_url=urlunparse(parsed._replace(path='/'+name))
    with psycopg.connect(urlunparse(parsed._replace(path='/postgres')),autocommit=True) as conn:
        conn.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(name)))
    initialize(test_url)
    with connect(test_url) as conn:
        conn.execute("INSERT INTO dim_product VALUES('PRD001','Control','hogar',true,1,5000,now())")
    item=dict(order_line_id='L1',product_id='PRD001',quantity=2,unit_price_cents=5000,discount_cents=0,refund_cents=None)
    paid=dict(event_id='PENDING-P',event_type='paid',order_id='PENDING-O',customer_id='CUSTOMER-ORIGINAL',
        event_time='2026-05-01T00:01:00Z',channel='web',currency='BOB',total_cents=10000,
        original_payment_id=None,items=[item])
    original_ingested='2026-10-09T20:00:00+00:00'
    stage(test_url,'pending-first',paid,original_ingested)
    first=publish(test_url,'pending-first','pending-test')
    assert first==dict(published=0,rejected=0,duplicate=0,pending=1),first
    with connect(test_url) as conn:
        original=conn.execute('SELECT * FROM pending_event').fetchone()
    changed=copy.deepcopy(paid)
    changed['customer_id']='CUSTOMER-CHANGED'
    stage(test_url,'pending-conflict',changed,'2026-10-09T21:00:00+00:00')
    conflict=publish(test_url,'pending-conflict','pending-test')
    assert conflict==dict(published=0,rejected=1,duplicate=0,pending=1),conflict
    with connect(test_url) as conn:
        preserved=conn.execute('SELECT * FROM pending_event').fetchone()
        assert preserved['payload_hash']==original['payload_hash']
        assert preserved['payload']['customer_id']=='CUSTOMER-ORIGINAL'
        assert preserved['payload']['ingested_at']==original_ingested
        assert conn.execute('SELECT count(*) AS count FROM fact_event').fetchone()['count']==0
        assert conn.execute('SELECT reason FROM quality_reject').fetchone()['reason']=='pending_event_id_conflicting_payload'
    created=copy.deepcopy(paid)
    created.update(event_id='PENDING-C',event_type='created',event_time='2026-04-30T23:59:00Z')
    stage(test_url,'pending-resolve',created,'2026-10-09T22:00:00+00:00')
    resolved=publish(test_url,'pending-resolve','pending-test')
    assert resolved==dict(published=2,rejected=0,duplicate=0,pending=0),resolved
    with connect(test_url) as conn:
        financial=conn.execute("SELECT count(*) events,sum(CASE WHEN event_type='paid' THEN total_cents ELSE 0 END) net_cents FROM fact_event").fetchone()
        assert financial==dict(events=2,net_cents=10000),financial
        assert conn.execute('SELECT count(*) count FROM pending_event').fetchone()['count']==0
        registered=conn.execute("SELECT customer_id,ingested_at FROM event_registry WHERE event_id='PENDING-P'").fetchone()
        assert registered['customer_id']=='CUSTOMER-ORIGINAL'
        assert registered['ingested_at'].isoformat()==original_ingested
    report=dict(passed=True,test_database=name,first=first,conflict=conflict,resolved=resolved,
        first_version_preserved=True,first_ingestion_timestamp_preserved=True,financial=financial,
        verified_at=datetime.now(timezone.utc).isoformat())
    output.parent.mkdir(parents=True,exist_ok=True)
    output.write_text(json.dumps(report,ensure_ascii=False,indent=2,default=str),encoding='utf-8')
    print(json.dumps(report,ensure_ascii=False,default=str))
    return report


if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--db-url',default='postgresql://lcd301:lcd301_local@127.0.0.1:55432/lcd301')
    parser.add_argument('--output',required=True,type=Path)
    args=parser.parse_args()
    test_pending(args.db_url,args.output)
