"""Reconciliación independiente de entrada REST, salida NiFi y cuarentena."""
import argparse,collections,hashlib,json
from pathlib import Path

def verify(root,batch):
    root=Path(root).resolve();manifest=root/f'data/manifests/{batch}.json';m=json.loads(manifest.read_text(encoding='utf-8'))
    assert m['status']=='closed' and m['zero_queues']
    source=root/'data/source'/('events_initial.json' if batch=='base' else 'events_delta.json')
    source_events=json.loads(source.read_text(encoding='utf-8'));source_ids=collections.Counter(e.get('event_id') for e in source_events)
    landing=[]
    for f in m['files']:
        file=root/f['path'];assert hashlib.sha256(file.read_bytes()).hexdigest()==f['sha256']
        for line in file.read_text(encoding='utf-8').splitlines():
            if line.strip():
                e=json.loads(line);assert e['source']=='rest_api' and e['batch_id']==batch
                assert e['ingested_at'].endswith('Z');landing.append(e)
    rejects=[];csv_rejects=[]
    for f in m['diagnostics']:
        file=root/f['path'];assert hashlib.sha256(file.read_bytes()).hexdigest()==f['sha256']
        d=json.loads(file.read_text(encoding='utf-8'));original=root/'data/quality/ingesta/originales'/file.name
        assert original.exists()
        if d['source']=='rest_api':
            assert d['json.validation.errors'] and d['quality.reason'];rejects.append(json.loads(original.read_text(encoding='utf-8')))
        elif d['source']=='catalog_csv':
            assert d['quality.csv_reason'];csv_rejects.append(d)
        else:raise AssertionError('Diagnóstico de fuente inesperada')
    actual=collections.Counter(e.get('event_id') for e in landing+rejects)
    assert source_ids==actual,'Se perdió o repitió una ocurrencia de la entrada'
    assert len(landing)==m['event_count']
    if batch=='base':assert len(rejects)==20 and len(csv_rejects)==1 and len(landing)==31045
    else:assert len(rejects)==0 and len(csv_rejects)==0 and len(landing)==7
    # Comparar los objetos de entrada completos, eliminando únicamente el enriquecimiento NiFi.
    def canonical(e):return json.dumps(e,sort_keys=True,separators=(',',':'),ensure_ascii=False)
    clean_landing=[{k:v for k,v in e.items() if k not in ['source','batch_id','ingested_at']} for e in landing]
    assert collections.Counter(map(canonical,source_events))==collections.Counter(map(canonical,clean_landing+rejects))
    result={'passed':True,'batch_id':batch,'input_events':len(source_events),'landing_events':len(landing),'schema_rejected_events':len(rejects),'catalog_rejections':len(csv_rejects),'all_occurrences_reconciled':True,'payloads_unchanged_except_metadata':True,'closed_manifest_hashes_verified':True}
    (root/f'03_Visualizaciones/evidencias/verificacion_ingesta_{batch}.json').write_text(json.dumps(result,indent=2),encoding='utf-8')
    print(json.dumps(result),flush=True);return result

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--project-root',type=Path,required=True);p.add_argument('--batch-id',choices=['base','delta'],required=True);a=p.parse_args();verify(a.project_root,a.batch_id)
