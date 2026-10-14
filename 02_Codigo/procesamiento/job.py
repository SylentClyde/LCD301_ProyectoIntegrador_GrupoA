"""Spark batch real: validación, unión CSV, deduplicación, agregación y JDBC."""
import argparse
import json
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse, unquote
from database import connect, initialize, publish
from psycopg.types.json import Jsonb
from pyspark.sql import SparkSession, Window, functions as F, types as T

ITEM = T.StructType([
    T.StructField('order_line_id', T.StringType()),T.StructField('product_id', T.StringType()),
    T.StructField('quantity', T.IntegerType()),T.StructField('unit_price_cents', T.LongType()),
    T.StructField('discount_cents', T.LongType()),T.StructField('refund_cents', T.LongType())])
EVENT = T.StructType([T.StructField(k,T.StringType()) for k in
    ('event_id','event_type','order_id','customer_id','event_time','channel','currency','original_payment_id')]
    + [T.StructField('total_cents',T.LongType()),T.StructField('items',T.ArrayType(ITEM)),
       T.StructField('ingested_at',T.StringType()),T.StructField('batch_id',T.StringType()),
       T.StructField('_corrupt_record',T.StringType())])
CATALOG = T.StructType([T.StructField('product_id',T.StringType()),T.StructField('name',T.StringType()),
    T.StructField('category',T.StringType()),T.StructField('active',T.StringType()),
    T.StructField('catalog_version',T.IntegerType()),T.StructField('list_price_cents',T.LongType())])


def dump_rows(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix+'.tmp')
    temporary.write_text(json.dumps(rows, ensure_ascii=False, indent=2, default=str),encoding='utf-8')
    temporary.replace(path)


def args_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('--project-root',type=Path,default=Path(__file__).resolve().parents[2])
    parser.add_argument('--db-url',default=os.environ.get('DATABASE_URL','postgresql://lcd301:lcd301_local@127.0.0.1:55432/lcd301'))
    parser.add_argument('--jdbc-jar',type=Path,required=True)
    parser.add_argument('--events-dir',type=Path)
    parser.add_argument('--catalog-dir',type=Path)
    parser.add_argument('--batch-id',default='base')
    parser.add_argument('--allow-unsealed',action='store_true',help='Solo para pruebas; producción requiere manifiesto cerrado.')
    parser.add_argument('--high-value-cents',type=int,default=500000)
    parser.add_argument('--master',default='local[2]')
    parser.add_argument('--report',type=Path,help='Guardar resumen real del intento en una ruta adicional.')
    return parser


def run(args):
    root = args.project_root.resolve()
    data = Path(os.environ.get('DATA_ROOT',str(root/'data'))).resolve()
    manifest = data/'manifests'/f'{args.batch_id}.json'
    if not args.allow_unsealed:
        if not manifest.exists():
            raise RuntimeError(f'Falta manifiesto cerrado: {manifest}')
        state = json.loads(manifest.read_text(encoding='utf-8-sig'))
        if state.get('status') not in ('closed','complete','sealed') and not state.get('closed'):
            raise RuntimeError('El manifiesto no declara el lote cerrado')
    events_dir = (args.events_dir or data/'landing'/'events'/args.batch_id).resolve()
    catalog_dir = (args.catalog_dir or data/'landing'/'catalog').resolve()
    if not args.jdbc_jar.exists():
        raise FileNotFoundError(args.jdbc_jar)
    initialize(args.db_url)
    started = datetime.now(timezone.utc)
    run_id = uuid.uuid4().hex
    out = data/'processed'/run_id
    out.mkdir(parents=True,exist_ok=True)
    with connect(args.db_url) as conn:
        conn.execute('INSERT INTO pipeline_runs(run_id,batch_id,started_at,status) VALUES(%s,%s,%s,%s)',
                     (run_id,args.batch_id,started,'running'))
    spark = None
    try:
        spark = (SparkSession.builder.master(args.master).appName('LCD301-GrupoA-ecommerce')
                 .config('spark.driver.extraClassPath',str(args.jdbc_jar.resolve()))
                 .config('spark.executor.extraClassPath',str(args.jdbc_jar.resolve()))
                 .config('spark.sql.session.timeZone','UTC')
                 .config('spark.sql.shuffle.partitions','4')
                 .config('spark.sql.ansi.enabled','false')
                 .config('spark.ui.enabled','false').getOrCreate())
        spark.sparkContext.setLogLevel('ERROR')
        event_files = sorted(p for p in events_dir.rglob('*') if p.is_file() and p.suffix.lower() in ('.json','.jsonl'))
        catalog_files = sorted(p for p in catalog_dir.rglob('*.csv') if p.is_file())
        if not event_files or not catalog_files:
            raise RuntimeError('El lote requiere archivos de eventos y catálogo')
        # Lista explícita evita el listado Hadoop NativeIO de directorios en Windows.
        # Los archivos se leen y transforman íntegramente con los lectores Spark.
        raw = (spark.read.schema(EVENT).option('mode','PERMISSIVE').json([str(p).replace('\\','/') for p in event_files])
               .withColumn('source_file',F.input_file_name()).cache())
        input_count = raw.count()
        if not input_count:
            raise RuntimeError('No hay eventos en landing')
        events = raw
        for col in ('event_id','event_type','order_id','customer_id','original_payment_id'):
            events = events.withColumn(col,F.trim(F.col(col)))
        events = (events.withColumn('event_type',F.lower('event_type'))
                  .withColumn('channel',F.translate(F.lower(F.trim('channel')),'ó','o'))
                  .withColumn('currency',F.upper(F.trim('currency')))
                  .withColumn('parsed_time',F.to_timestamp('event_time'))
                  .withColumn('items',F.sort_array(F.transform('items',lambda i:F.struct(
                      F.trim(i['order_line_id']).alias('order_line_id'),F.trim(i['product_id']).alias('product_id'),
                      i['quantity'].alias('quantity'),i['unit_price_cents'].alias('unit_price_cents'),
                      i['discount_cents'].alias('discount_cents'),i['refund_cents'].alias('refund_cents'))))))
        # Metadato real de NiFi; mtime del archivo es fallback explícito en laboratorio.
        file_times = []
        for path in event_files:
            file_times.append((str(path).replace('\\','/'),datetime.fromtimestamp(path.stat().st_mtime,timezone.utc).isoformat()))
        # Metadatos del driver como relación SQL local; sin un RDD Python auxiliar.
        values = ','.join("('"+key.replace("'","''")+"','"+stamp+"')" for key,stamp in file_times)
        file_time = spark.sql('SELECT * FROM VALUES '+values+' AS file_times(file_key,file_ingested_at)')
        events = events.withColumn('file_key',F.regexp_replace(F.regexp_replace('source_file',r'^file:/+','' if os.name=='nt' else '/'),'%20',' '))
        events = events.join(F.broadcast(file_time),'file_key','left')
        events = events.withColumn('ingested_at',F.coalesce('ingested_at','file_ingested_at',F.lit(started.isoformat())))
        events = events.withColumn('computed_total',F.aggregate('items',F.lit(0).cast('long'),
            lambda total,item: total + F.when(F.col('event_type')=='refunded',item['refund_cents'])
            .otherwise(item['quantity']*item['unit_price_cents']-item['discount_cents'])))
        missing = F.lit(False)
        for name in ('event_id','event_type','order_id','customer_id','event_time','channel','currency'):
            missing = missing | F.col(name).isNull() | (F.trim(name)=='')
        bad_items = F.coalesce(F.exists('items',lambda i:
            i['order_line_id'].isNull() | (F.trim(i['order_line_id'])=='') |
            i['product_id'].isNull() | (F.trim(i['product_id'])=='') |
            i['quantity'].isNull() | (i['quantity']<=0) |
            i['unit_price_cents'].isNull() | (i['unit_price_cents']<0) |
            i['discount_cents'].isNull() | (i['discount_cents']<0) |
            (i['discount_cents']>i['quantity']*i['unit_price_cents']) |
            ((F.col('event_type')=='refunded') & (i['refund_cents'].isNull() | (i['refund_cents']<=0)))),F.lit(False))
        reason = (F.when(F.col('_corrupt_record').isNotNull(),'malformed_json_or_types')
            .when(missing,'missing_required_field')
            .when(~F.col('event_type').isin('created','paid','cancelled','refunded'),'invalid_event_type')
            .when(~F.col('channel').isin('web','movil','tienda'),'invalid_channel')
            .when(F.col('currency')!='BOB','invalid_currency')
            .when(F.col('parsed_time').isNull() | ~F.col('event_time').rlike(r'(Z|[+-]\d{2}:\d{2})$'),'invalid_timestamp_or_timezone')
            .when(F.to_timestamp('ingested_at').isNull(),'invalid_ingestion_timestamp')
            .when(F.col('total_cents').isNull() | (F.col('total_cents')<0),'invalid_total')
            .when((F.col('event_type')!='cancelled') & (F.col('items').isNull() | (F.size('items')==0)),'missing_items')
            .when(bad_items,'invalid_line_quantity_or_amount')
            .when(F.size(F.array_distinct(F.transform('items',lambda i:i['order_line_id'])))!=F.size('items'),'duplicate_order_line')
            .when((F.col('event_type')!='cancelled') & (F.col('computed_total')!=F.col('total_cents')),'line_total_mismatch')
            .when((F.col('event_type')=='cancelled') & (F.col('total_cents')!=0),'cancel_must_have_zero_amount')
            .when((F.col('event_type')=='refunded') & (F.col('original_payment_id').isNull() | (F.col('original_payment_id')=='')),'missing_payment_reference'))
        checked = events.withColumn('reason',reason).cache()
        bad = checked.filter(F.col('reason').isNotNull())
        candidates = checked.filter(F.col('reason').isNull()).withColumn('event_time',F.date_format('parsed_time',"yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'"))
        hash_cols = ['event_id','event_type','order_id','customer_id','event_time','channel','currency','total_cents','original_payment_id','items']
        candidates = candidates.withColumn('payload_hash',F.sha2(F.to_json(F.struct(*hash_cols)),256)).cache()
        conflicts = candidates.groupBy('event_id').agg(F.countDistinct('payload_hash').alias('variants')).filter('variants>1').select('event_id')
        conflicting = candidates.join(conflicts,'event_id','inner').withColumn('reason',F.lit('event_id_conflicting_payload'))
        unique = candidates.join(conflicts,'event_id','left_anti').dropDuplicates(['event_id']).cache()
        candidate_count, unique_count = candidates.count(),unique.count()
        conflict_count = conflicting.count()
        duplicates = candidate_count-unique_count-conflict_count
        catalog = (spark.read.schema(CATALOG).option('header','true').csv([str(p).replace('\\','/') for p in catalog_files])
                   .withColumn('product_id',F.trim('product_id')).withColumn('name',F.trim('name'))
                   .withColumn('category',F.lower(F.trim('category'))).withColumn('active',F.lower(F.trim('active'))))
        catalog_rule = (F.col('product_id').isNotNull() & (F.col('product_id')!='') & F.col('name').isNotNull() & (F.col('name')!='') & F.col('category').isNotNull() & (F.col('category')!='')
            & F.col('active').isin('true','false','1','0') & (F.col('catalog_version')>0) & (F.col('list_price_cents')>=0))
        catalog_valid = catalog.filter(catalog_rule).dropDuplicates()
        catalog_bad = catalog.filter(~F.coalesce(catalog_rule,F.lit(False)))
        dump_rows(out/'catalog_rejects.json',[r.asDict() for r in catalog_bad.collect()])
        catalog_conflicts = catalog_valid.groupBy('product_id','catalog_version').count().filter('count>1').collect()
        if catalog_conflicts:
            dump_rows(out/'catalog_conflicts.json',[r.asDict() for r in catalog_conflicts])
            raise RuntimeError('Catálogo tiene distintas definiciones de un SKU en la misma versión; corregir antes de publicar')
        latest = (catalog_valid.withColumn('rank',F.row_number().over(Window.partitionBy('product_id').orderBy(F.desc('catalog_version'),F.asc('name'))))
            .filter('rank=1').drop('rank').withColumn('active',F.col('active').isin('true','1')).cache())
        catalog_rows = [r.asDict() for r in latest.collect()]
        if not catalog_rows:
            raise RuntimeError('Catálogo válido vacío')
        with connect(args.db_url) as conn,conn.cursor() as cur:
            cur.executemany('INSERT INTO dim_product(product_id,name,category,active,catalog_version,list_price_cents) VALUES(%s,%s,%s,%s,%s,%s) ON CONFLICT(product_id) DO UPDATE SET name=excluded.name,category=excluded.category,active=excluded.active,catalog_version=excluded.catalog_version,list_price_cents=excluded.list_price_cents,updated_at=now() WHERE dim_product.catalog_version<=excluded.catalog_version',
                [tuple(r[k] for k in ('product_id','name','category','active','catalog_version','list_price_cents')) for r in catalog_rows])
        lines = unique.select('event_id',F.explode('items').alias('item')).select('event_id','item.*')
        enriched = lines.join(F.broadcast(latest.select('product_id',F.col('name').alias('product_name'),'category','catalog_version')),'product_id','left')
        unknown = enriched.filter(F.col('product_name').isNull()).select('event_id').distinct()
        unknown_events = unique.join(unknown,'event_id','inner').withColumn('reason',F.lit('unknown_product'))
        accepted = unique.join(unknown,'event_id','left_anti').cache()
        enriched = enriched.join(accepted.select('event_id'),'event_id','inner').cache()
        rejects = []
        for frame in (bad,conflicting,unknown_events):
            for row in frame.collect():
                payload = row.asDict(recursive=True)
                rejects.append(dict(event_id=payload.get('event_id'),reason=payload.pop('reason'),payload=payload))
        dump_rows(data/'quality'/'processing'/f'{run_id}.json',rejects)
        with connect(args.db_url) as conn,conn.cursor() as cur:
            cur.executemany('INSERT INTO quality_reject(run_id,event_id,reason,payload,stage) VALUES(%s,%s,%s,%s,%s)',
                [(run_id,r['event_id'],r['reason'],Jsonb(json.loads(json.dumps(r['payload'],default=str))),'spark') for r in rejects])
            cur.execute('UPDATE pipeline_runs SET input_count=%s,accepted_count=%s,rejected_count=%s,duplicate_count=%s WHERE run_id=%s',
                (input_count,accepted.count(),len(rejects),duplicates,run_id))
        parsed = urlparse(args.db_url)
        jdbc_url = f'jdbc:postgresql://{parsed.hostname}:{parsed.port or 5432}/{parsed.path.lstrip("/")}?currentSchema=ecommerce'
        jdbc_options = dict(url=jdbc_url,user=unquote(parsed.username or ''),password=unquote(parsed.password or ''),driver='org.postgresql.Driver',batchsize='2000')
        stage_events = accepted.select(F.lit(run_id).alias('run_id'),'event_id','event_type','order_id','customer_id','event_time','channel','currency','total_cents','original_payment_id','payload_hash',F.to_json('items').alias('items_json'),'ingested_at','source_file')
        stage_events.write.format('jdbc').options(**jdbc_options,dbtable='ecommerce.stage_event').mode('append').save()
        enriched.select(F.lit(run_id).alias('run_id'),'event_id','order_line_id','product_id','quantity','unit_price_cents','discount_cents','refund_cents','product_name','category','catalog_version').write.format('jdbc').options(**jdbc_options,dbtable='ecommerce.stage_line').mode('append').save()
        # Las agregaciones Spark describen candidatos. Los KPIs publicados se consultan en SQL tras validar el ciclo.
        value = F.when(F.col('event_type')=='paid',F.col('total_cents')).when(F.col('event_type')=='refunded',-F.col('total_cents')).otherwise(F.lit(0))
        daily = accepted.withColumn('day',F.to_date('parsed_time')).withColumn('signed_cents',value).groupBy('day','channel').agg(F.sum('signed_cents').alias('candidate_net_cents'),F.count('*').alias('events'),F.avg('total_cents').alias('mean_event_cents'))
        dump_rows(out/'daily_candidates.json',[r.asDict() for r in daily.orderBy('day','channel').collect()])
        by_category = enriched.join(accepted.select('event_id','event_type'),'event_id').withColumn('candidate_cents',F.when(F.col('event_type')=='paid',F.col('quantity')*F.col('unit_price_cents')-F.col('discount_cents')).when(F.col('event_type')=='refunded',-F.col('refund_cents')).otherwise(F.lit(0))).groupBy('category').agg(F.sum('candidate_cents').alias('candidate_net_cents'),F.count('*').alias('lines'))
        dump_rows(out/'category_candidates.json',[r.asDict() for r in by_category.collect()])
        high = accepted.filter((F.col('event_type')=='paid') & (F.col('total_cents')>=args.high_value_cents))
        dump_rows(out/'high_value_alerts.json',[r.asDict(recursive=True) for r in high.select('event_id','order_id','customer_id','channel','total_cents','event_time').collect()])
        counts = publish(args.db_url,run_id,args.batch_id)
        with connect(args.db_url) as conn:
            latency = conn.execute('SELECT percentile_cont(0.5) WITHIN GROUP(ORDER BY extract(epoch from persisted_at-ingested_at)*1000) median,percentile_cont(0.95) WITHIN GROUP(ORDER BY extract(epoch from persisted_at-ingested_at)*1000) p95 FROM event_registry WHERE run_id=%s',(run_id,)).fetchone()
            duration = (datetime.now(timezone.utc)-started).total_seconds()
            conn.execute("UPDATE pipeline_runs SET status='success',finished_at=now(),duration_seconds=%s,throughput_per_second=input_count/%s,latency_median_ms=%s,latency_p95_ms=%s WHERE run_id=%s",(duration,duration,latency['median'],latency['p95'],run_id))
            metrics = conn.execute('SELECT * FROM pipeline_runs WHERE run_id=%s',(run_id,)).fetchone()
        dump_rows(out/'run_summary.json',metrics)
        if args.report:
            dump_rows(args.report,metrics)
        print(json.dumps(metrics,default=str,ensure_ascii=False))
        return metrics
    except Exception as exc:
        with connect(args.db_url) as conn:
            conn.execute("UPDATE pipeline_runs SET status='failed',finished_at=now(),duration_seconds=extract(epoch from now()-started_at),error_message=%s WHERE run_id=%s",(str(exc)[:3000],run_id))
        raise
    finally:
        if spark is not None:
            spark.stop()


if __name__=='__main__':
    run(args_parser().parse_args())
