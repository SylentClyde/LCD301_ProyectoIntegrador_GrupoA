"""NiFi entrega un lote cerrado; no publica un manifiesto mientras existan colas."""
import argparse,datetime,hashlib,json,os,shutil,time,urllib.request
from pathlib import Path
import psycopg
from psycopg.types.json import Jsonb
from nifi_client import NiFiClient

def utc():return datetime.datetime.now(datetime.timezone.utc).isoformat()
def hashed(root,file):
    b=file.read_bytes()
    return {'path':file.relative_to(root).as_posix(),'bytes':len(b),'sha256':hashlib.sha256(b).hexdigest()}
def patch(c,id,props):
    e=c.call('GET','/processors/'+id);desc=e['component']['config']['descriptors']
    values={next((k for k,d in desc.items() if k==n or d.get('displayName')==n),n):v for n,v in props.items()}
    return c.call('PUT','/processors/'+id,{'revision':e['revision'],'component':{'id':id,'config':{'properties':values}}})
def snapshot(c,group,batch,db):
    e=c.call('GET',f'/flow/process-groups/{group}/status')['processGroupStatus'];s=e['aggregateSnapshot']
    flow=c.call('GET',f'/flow/process-groups/{group}')['processGroupFlow']['flow']
    invalid=sum(bool(p['component'].get('validationErrors')) for p in flow['processors'])
    bulletins=[b['bulletin'] for p in flow['processors'] for b in p.get('bulletins',[]) if b.get('bulletin',{}).get('level')=='ERROR']
    values={'queue_flowfiles':s['flowFilesQueued'],'queue_bytes':s['bytesQueued'],'active_threads':s['activeThreadCount'],'invalid_processors':invalid,'bulletin_errors':len(bulletins)}
    record={'sampled_at':utc(),'group_id':group,'batch_id':batch,'metrics':values,'snapshot':s,'bulletins':bulletins}
    if db:
        with psycopg.connect(db,connect_timeout=5) as conn:
            with conn.cursor() as cur:
                cur.executemany('INSERT INTO ecommerce.system_metric(sampled_at,source,metric,value,labels) VALUES (%s,%s,%s,%s,%s)',[(record['sampled_at'],'nifi',k,v,Jsonb({'group_id':group,'batch_id':batch})) for k,v in values.items()])
    return record
def run(root,batch,api,db,timeout=1800):
    root=Path(root).resolve();out=root/f'data/landing/events/{batch}';out.mkdir(parents=True,exist_ok=True)
    manifest=root/f'data/manifests/{batch}.json';manifest.parent.mkdir(parents=True,exist_ok=True)
    if manifest.exists():
        prior=json.loads(manifest.read_text(encoding='utf-8'))
        if prior.get('status')=='closed':
            for f in prior['files']:
                if hashed(root,root/f['path'])['sha256']!=f['sha256']:raise RuntimeError('El lote cerrado fue modificado')
            print(json.dumps({'status':'already_closed','manifest':str(manifest)}),flush=True);return prior
    if list(out.glob('*.jsonl')):raise RuntimeError('Existen archivos sin manifiesto; revisar el lote antes de volver a dispararlo')
    ids=json.loads((root/'03_Visualizaciones/evidencias/nifi_ids.json').read_text(encoding='utf-8'));c=NiFiClient();group=ids['group_id'];p=ids['processors']
    s=c.call('GET',f'/flow/process-groups/{group}/status')['processGroupStatus']['aggregateSnapshot']
    if s['flowFilesQueued']:raise RuntimeError('El grupo contiene datos pendientes; no se puede cambiar de lote')
    endpoint=api.rstrip('/')+('/events/delta?cursor=0&limit=50000' if batch=='delta' else '/events?cursor=0&limit=50000')
    patch(c,p['api'],{'HTTP URL':endpoint});patch(c,p['api_meta'],{'batch_id':batch});patch(c,p['catalog_meta'],{'batch_id':batch})
    if batch=='base':
        inbox=root/'data/inbox/catalog';inbox.mkdir(parents=True,exist_ok=True)
        for name in ['catalogo_productos.csv','catalogo_productos_invalido.csv']:shutil.copyfile(root/'data/source'/name,inbox/name)
    raw_before={f.name for f in (root/'data/raw/api').glob('*')};quality_before={f.name for f in (root/'data/quality/ingesta/metadatos').glob('*.json')}
    started=utc();begin=time.monotonic();records=[];stopped_trigger=False;empty_samples=0
    c.call('PUT',f'/flow/process-groups/{group}',{'id':group,'state':'RUNNING','disconnectedNodeAcknowledged':False})
    try:
        while time.monotonic()-begin<timeout:
            cycle_start=time.monotonic()
            if (root/'data/manifests/ABORTAR').exists():raise RuntimeError('Lote detenido por el operador, sin publicar sello')
            r=snapshot(c,group,batch,db);records.append(r)
            (root/f'03_Visualizaciones/evidencias/nifi_{batch}_progreso.json').write_text(json.dumps(records,ensure_ascii=False),encoding='utf-8')
            stats=r['snapshot'];raw_new=[f for f in (root/'data/raw/api').glob('*') if f.name not in raw_before]
            print(json.dumps({'batch':batch,'seconds':round(time.monotonic()-begin),'queue':stats['flowFilesQueued'],'threads':stats['activeThreadCount'],'landing_files':len(list(out.glob('*.jsonl'))),'raw_pages':len(raw_new),'errors':r['metrics']['bulletin_errors']}),flush=True)
            if not stopped_trigger and (raw_new or stats['flowFilesTransferred']):
                c.state(p['trigger'],'STOPPED');stopped_trigger=True
            if stopped_trigger and not raw_new and stats['flowFilesQueued']==0 and stats['activeThreadCount']==0 and len(records)>=3:
                raise RuntimeError('La petición REST falló; revisar la cuarentena HTTP y reintentar el lote abierto')
            ready=stopped_trigger and raw_new and list(out.glob('*.jsonl')) and stats['flowFilesQueued']==0 and stats['activeThreadCount']==0
            empty_samples=empty_samples+1 if ready else 0
            if empty_samples>=2:break
            time.sleep(max(0,15-(time.monotonic()-cycle_start)))
        else:raise TimeoutError('NiFi no pudo cerrar el lote dentro del tiempo máximo')
    finally:
        c.call('PUT',f'/flow/process-groups/{group}',{'id':group,'state':'STOPPED','disconnectedNodeAcknowledged':False})
    final=snapshot(c,group,batch,db);records.append(final)
    if final['snapshot']['flowFilesQueued'] or final['snapshot']['activeThreadCount']:raise RuntimeError('NiFi mantiene datos o tareas activas; el lote queda abierto')
    files=[hashed(root,f) for f in sorted(out.glob('*.jsonl'))]
    count=sum(sum(bool(line.strip()) for line in f.open(encoding='utf-8')) for f in out.glob('*.jsonl'))
    quality=[f for f in (root/'data/quality/ingesta/metadatos').glob('*.json') if f.name not in quality_before]
    raw_pages=[json.loads(f.read_text(encoding='utf-8')) for f in raw_new]
    if len(raw_pages)!=1 or raw_pages[0].get('next_cursor') is not None:
        raise RuntimeError('La fuente supera una página de50000 o devolvió páginas inesperadas; no se publica sello parcial')
    api_input_count=len(raw_pages[0]['events'])
    json_rejections=sum(json.loads(f.read_text(encoding='utf-8')).get('source')=='rest_api' for f in quality)
    if api_input_count!=count+json_rejections:
        raise RuntimeError('No se reconcilian entradas REST con landing y cuarentena; revisar antes de sellar')
    report={'batch_id':batch,'status':'closed','started_at':started,'closed_at':utc(),'source':'rest_api','group_id':group,'input_event_count':api_input_count,'event_count':count,'files':files,'catalog_files':[hashed(root,f) for f in sorted((root/'data/landing/catalog').glob('*.csv'))],'ingestion_rejections':len(quality),'rest_schema_rejections':json_rejections,'diagnostics':[hashed(root,f) for f in sorted(quality)],'zero_queues':True}
    # Escribir al final y reemplazar atómicamente: Spark nunca lee un sello parcial.
    temp=manifest.with_suffix('.json.tmp');temp.write_text(json.dumps(report,indent=2,ensure_ascii=False),encoding='utf-8');temp.replace(manifest)
    (root/f'03_Visualizaciones/evidencias/nifi_{batch}.json').write_text(json.dumps({'manifest':report,'samples':records},indent=2,ensure_ascii=False),encoding='utf-8')
    (root/'02_Codigo/ingesta/flujo_nifi_2.12.0.json').write_bytes(c.call('GET',f'/process-groups/{group}/download',raw=True))
    print(json.dumps(report,ensure_ascii=False),flush=True);return report

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--project-root',type=Path,required=True);p.add_argument('--batch-id',choices=['base','delta'],required=True);p.add_argument('--api-url',default='http://127.0.0.1:18080');p.add_argument('--db-url',default=os.getenv('DATABASE_URL'));p.add_argument('--timeout',type=int,default=1800);a=p.parse_args();run(a.project_root,a.batch_id,a.api_url,a.db_url,a.timeout)
