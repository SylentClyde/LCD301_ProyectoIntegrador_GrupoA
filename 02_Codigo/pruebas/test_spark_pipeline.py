"""Integración Spark-JDBC-PostgreSQL con entradas pequeñas y base aislada."""
import argparse
import copy
import json
import os
import subprocess
import sys
import uuid
from datetime import datetime,timezone
from pathlib import Path
from urllib.parse import urlparse,urlunparse
import psycopg
from psycopg import sql
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'procesamiento'))
from database import connect


def test_spark(db_url,jdbc_jar,output):
    fixture=Path(__file__).resolve().parent/'spark_fixture'
    events=fixture/'data'/'landing'/'events'
    catalog=fixture/'data'/'landing'/'catalog'
    manifests=fixture/'data'/'manifests'
    for directory in (events,catalog,manifests):
        directory.mkdir(parents=True,exist_ok=True)
    line=dict(order_line_id='L1',product_id='PRD001',quantity=2,unit_price_cents=5000,discount_cents=0,refund_cents=None)
    created=dict(event_id='SC',event_type='created',order_id='SO',customer_id='SU',event_time='2026-04-30T23:59:00Z',channel=' WEB ',currency='bob',total_cents=10000,original_payment_id=None,items=[line],ingested_at=datetime.now(timezone.utc).isoformat())
    paid=copy.deepcopy(created)
    paid.update(event_id='SP',event_type='paid',event_time='2026-05-01T00:01:00Z')
    refund=copy.deepcopy(paid)
    refund.update(event_id='SR',event_type='refunded',event_time='2026-05-02T00:00:00Z',total_cents=5000,original_payment_id='SP')
    refund['items'][0].update(quantity=1,refund_cents=5000)
    unknown=copy.deepcopy(created)
    unknown.update(event_id='UNKNOWN',order_id='UNKNOWN')
    unknown['items'][0]['product_id']='PRD999'
    bad=copy.deepcopy(created)
    bad.update(event_id='BAD',order_id='BAD',total_cents=0)
    bad['items'][0]['quantity']=0
    # Orden físico inverso + duplicado idéntico; la lógica debe ordenar y deduplicar.
    records=[refund,unknown,paid,created,paid,bad]
    (events/'fixture.jsonl').write_text('\n'.join(json.dumps(r) for r in records)+'\n',encoding='utf-8')
    (catalog/'catalog.csv').write_text('product_id,name,category,active,catalog_version,list_price_cents\nPRD001,Control,HOGAR,true,1,6000\n',encoding='utf-8')
    (manifests/'fixture.json').write_text(json.dumps(dict(status='closed')),encoding='utf-8')
    parsed=urlparse(db_url)
    database='lcd301_spark_'+uuid.uuid4().hex[:10]
    url=urlunparse(parsed._replace(path='/'+database))
    with psycopg.connect(urlunparse(parsed._replace(path='/postgres')),autocommit=True) as conn:
        conn.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(database)))
    command=[sys.executable,str(Path(__file__).resolve().parents[1]/'procesamiento'/'job.py'),
             '--project-root',str(fixture),'--db-url',url,'--jdbc-jar',str(jdbc_jar),'--batch-id','fixture']
    subprocess.run(command,env=os.environ.copy(),check=True)
    with connect(url) as conn:
        run=conn.execute('SELECT * FROM pipeline_runs ORDER BY started_at DESC LIMIT 1').fetchone()
        assert run['published_count']==3 and run['rejected_count']==2 and run['duplicate_count']==1,run
        financial=conn.execute("SELECT sum(CASE WHEN event_type='paid' THEN total_cents WHEN event_type='refunded' THEN -total_cents ELSE 0 END) net_cents,count(*) events FROM fact_event").fetchone()
        assert financial==dict(net_cents=5000,events=3),financial
        metadata=conn.execute("SELECT channel,category,product_name,unit_price_cents FROM fact_event_line WHERE event_type='paid'").fetchone()
        assert metadata==dict(channel='web',category='hogar',product_name='Control',unit_price_cents=5000),metadata
        rejected=conn.execute('SELECT reason FROM quality_reject ORDER BY reason').fetchall()
        assert {r['reason'] for r in rejected}=={'unknown_product','invalid_line_quantity_or_amount'},rejected
    report=dict(passed=True,database=database,run=run,financial=financial,normalized_snapshot=metadata,rejections=rejected)
    output.parent.mkdir(parents=True,exist_ok=True)
    output.write_text(json.dumps(report,indent=2,ensure_ascii=False,default=str),encoding='utf-8')
    print(json.dumps(report,ensure_ascii=False,default=str))
    return report


if __name__=='__main__':
    p=argparse.ArgumentParser()
    p.add_argument('--db-url',default='postgresql://lcd301:lcd301_local@127.0.0.1:55432/lcd301')
    p.add_argument('--jdbc-jar',required=True,type=Path)
    p.add_argument('--output',required=True,type=Path)
    a=p.parse_args()
    test_spark(a.db_url,a.jdbc_jar,a.output)
