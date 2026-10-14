"""Primera ejecución + cinco repeticiones; plan y tiempo servidor/cliente reales."""
import argparse
import json
import platform
import statistics
import time
from datetime import datetime,timezone
from pathlib import Path
from database import connect


def benchmark(db_url, output, start, end, as_of, background_note=None):
    output.mkdir(parents=True,exist_ok=True)
    params = dict(start=start,end=end,as_of=as_of)
    measurements = []
    with connect(db_url) as conn:
        conn.execute('ANALYZE ecommerce.fact_event')
        conn.execute('ANALYZE ecommerce.fact_event_line')
        environment = dict(version=conn.execute('SELECT version() AS version').fetchone()['version'],
                           platform=platform.platform(),processor=platform.processor(),parameters=params,
                           measured_at=datetime.now(timezone.utc).isoformat(),
                           background_activity=background_note or 'Carga de fondo no controlada; sin aislamiento de CPU/E/S.',
                           row_counts=conn.execute('SELECT (SELECT count(*) FROM ecommerce.fact_event) event_rows,(SELECT count(*) FROM ecommerce.fact_event_line) line_rows').fetchone(),
                           partitions=conn.execute("SELECT p.relname AS parent,c.relname AS partition FROM pg_inherits i JOIN pg_class c ON c.oid=i.inhrelid JOIN pg_class p ON p.oid=i.inhparent JOIN pg_namespace n ON n.oid=p.relnamespace WHERE n.nspname='ecommerce' AND p.relname IN ('fact_event','fact_event_line') ORDER BY 1,2").fetchall(),
                           indexes=conn.execute("SELECT tablename,indexname,indexdef FROM pg_indexes WHERE schemaname='ecommerce' AND tablename IN ('fact_event','fact_event_line','event_registry') ORDER BY 1,2").fetchall(),
                           cache_note='Primera medición no garantiza caché fría. Repeticiones consecutivas suelen aprovechar caché. Sin comparación antes/después.')
        for path in sorted((Path(__file__).resolve().parents[1]/'sql').glob('Q*.sql')):
            sql = path.read_text(encoding='utf-8')
            times = []
            for repeat in range(6):
                begin = time.perf_counter()
                result = conn.execute(sql,params).fetchall()
                elapsed = (time.perf_counter()-begin)*1000
                times.append(elapsed)
                if repeat==0:
                    (output/f'{path.stem}_result.json').write_text(json.dumps(result,ensure_ascii=False,indent=2,default=str),encoding='utf-8')
            plan = conn.execute('EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) '+sql,params).fetchone()['QUERY PLAN']
            (output/f'{path.stem}_plan.json').write_text(json.dumps(plan,indent=2,default=str),encoding='utf-8')
            measurements.append(dict(query=path.stem,result_rows=len(result),first_client_ms=times[0],
                repetitions_ms=times[1:],median_client_ms=statistics.median(times[1:]),
                min_client_ms=min(times[1:]),max_client_ms=max(times[1:]),
                server_execution_ms=plan[0]['Execution Time'],server_planning_ms=plan[0]['Planning Time']))
    summary=dict(environment=environment,queries=measurements)
    (output/'benchmark_summary.json').write_text(json.dumps(summary,ensure_ascii=False,indent=2,default=str),encoding='utf-8')
    print(json.dumps(summary,ensure_ascii=False,default=str))
    return summary


if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--db-url',default='postgresql://lcd301:lcd301_local@127.0.0.1:55432/lcd301')
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--start',default='2026-04-01T00:00:00Z')
    parser.add_argument('--end',default='2026-10-01T00:00:00Z')
    parser.add_argument('--as-of',default='2026-10-01T00:00:00Z')
    parser.add_argument('--background-note',help='Contexto de carga concurrente observado durante la medición.')
    args=parser.parse_args()
    benchmark(args.db_url,args.output,args.start,args.end,args.as_of,args.background_note)
