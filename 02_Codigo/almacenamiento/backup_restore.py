"""Backup custom, retención de siete archivos y restauración verificada aislada."""
import argparse
import hashlib
import json
import os
import subprocess
import sys
from contextlib import nullcontext
from datetime import datetime,timezone
from pathlib import Path
from urllib.parse import urlparse,unquote,urlunparse
import psycopg
from psycopg import sql


def snapshot(db_url,connection=None):
    result={}
    with (nullcontext(connection) if connection else psycopg.connect(db_url,options='-c timezone=UTC')) as conn:
        for table,key in [('event_registry','event_id'),('fact_event','event_id'),
                          ('fact_event_line','event_id,order_line_id'),('dim_product','product_id'),
                          ('dim_customer','customer_id'),('pipeline_runs','run_id'),('pending_event','event_id'),
                          ('quality_reject','reject_id')]:
            rows=conn.execute(sql.SQL('SELECT to_jsonb(t)::text FROM ecommerce.{} t ORDER BY '+key).format(sql.Identifier(table))).fetchall()
            digest=hashlib.sha256('\n'.join(r[0] for r in rows).encode('utf-8')).hexdigest()
            result[table]=dict(count=len(rows),sha256=digest)
        row=conn.execute("SELECT sum(CASE WHEN event_type='paid' THEN total_cents WHEN event_type='refunded' THEN -total_cents ELSE 0 END),count(*) FILTER(WHERE event_type='paid') FROM ecommerce.fact_event").fetchone()
        result['financial']=dict(net_cents=row[0],paid_orders=row[1])
    return result


def backup_restore(db_url,pg_bin,backup_dir):
    backup_dir=backup_dir.resolve()
    backup_dir.mkdir(parents=True,exist_ok=True)
    parsed=urlparse(db_url)
    stamp=datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')
    backup=backup_dir/f'lcd301_{stamp}.dump'
    restore_database='lcd301_restore_'+stamp.lower().replace('t','_').replace('z','')
    env=os.environ.copy()
    env['PGPASSWORD']=unquote(parsed.password or '')
    common=['-h',parsed.hostname or '127.0.0.1','-p',str(parsed.port or 5432),'-U',unquote(parsed.username or '')]
    exe_suffix='.exe' if os.name=='nt' else ''
    with psycopg.connect(db_url,options='-c timezone=UTC') as source:
        source.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
        snapshot_id=source.execute('SELECT pg_export_snapshot()').fetchone()[0]
        before=snapshot(db_url,source)
        subprocess.run([str(pg_bin/('pg_dump'+exe_suffix)),*common,'-d',parsed.path.lstrip('/'),'-Fc','--snapshot',snapshot_id,'-f',str(backup)],env=env,check=True,capture_output=True,text=True)
    admin_url=urlunparse(parsed._replace(path='/postgres'))
    with psycopg.connect(admin_url,autocommit=True) as conn:
        conn.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(restore_database)))
    subprocess.run([str(pg_bin/('pg_restore'+exe_suffix)),*common,'-d',restore_database,'--exit-on-error',str(backup)],env=env,check=True,capture_output=True,text=True)
    restored_url=urlunparse(parsed._replace(path='/'+restore_database))
    after=snapshot(restored_url)
    if before!=after:
        raise AssertionError('Backup restaurado difiere del original')
    # Solo archivos creados por este script, dentro del directorio resuelto autorizado.
    for old in sorted(backup_dir.glob('lcd301_*.dump'),key=lambda p:p.stat().st_mtime,reverse=True)[7:]:
        if old.resolve().parent!=backup_dir:
            raise RuntimeError('Ruta de retención fuera del directorio de backups')
        old.unlink()
    report=dict(verified=True,backup=str(backup),backup_bytes=backup.stat().st_size,
                backup_sha256=hashlib.sha256(backup.read_bytes()).hexdigest(),
                consistent_snapshot=snapshot_id,
                restore_database=restore_database,verified_at=datetime.now(timezone.utc).isoformat(),
                original=before,restored=after,retention_copies=7)
    serialized=json.dumps(report,ensure_ascii=False,indent=2,default=str)
    (backup_dir/'restore_verification.json').write_text(serialized,encoding='utf-8')
    (backup_dir/f'restore_verification_{stamp}.json').write_text(serialized,encoding='utf-8')
    print(json.dumps(report,ensure_ascii=False,default=str))
    return report


if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--db-url',default=os.environ.get('DATABASE_URL','postgresql://lcd301:lcd301_local@127.0.0.1:55432/lcd301'))
    parser.add_argument('--pg-bin',required=True,type=Path)
    parser.add_argument('--backup-dir',type=Path,default=Path(__file__).resolve().parents[2]/'data'/'backups')
    args=parser.parse_args()
    backup_restore(args.db_url,args.pg_bin,args.backup_dir)
