"""Actualiza el flujo existente con disparador y reintentos limitados sin recrearlo."""
import argparse,json
from pathlib import Path
from nifi_client import NiFiClient

def configure(root):
    root=Path(root).resolve();file=root/'03_Visualizaciones/evidencias/nifi_ids.json'
    ids=json.loads(file.read_text(encoding='utf-8'));c=NiFiClient();group=ids['group_id']
    def patch(key,name,props,auto=None,period='0 sec'):
        e=c.call('GET','/processors/'+ids['processors'][key]);desc=e['component']['config']['descriptors']
        config={'properties':{next((k for k,d in desc.items() if k==n or d.get('displayName')==n),n):v for n,v in props.items()},'schedulingPeriod':period,'penaltyDuration':'1 sec'}
        if auto is not None:config['autoTerminatedRelationships']=auto
        c.call('PUT','/processors/'+e['id'],{'revision':e['revision'],'component':{'id':e['id'],'name':name,'config':config}})
    types=c.call('GET','/flow/processor-types')['processorTypes']
    for key,name in [('trigger','GenerateFlowFile'),('retry','RetryFlowFile')]:
        if key not in ids['processors']:
            t=next(t for t in types if t['type'].split('.')[-1]==name)
            e=c.call('POST',f'/process-groups/{group}/processors',{'revision':{'version':0},'component':{'type':t['type'],'bundle':t['bundle'],'name':name,'position':{'x':-460,'y':0 if key=='trigger' else 380}}})
            ids['processors'][key]=e['id'];file.write_text(json.dumps(ids,indent=2))
    patch('trigger','00 Disparar un lote REST',{'Custom Text':'{}','Batch Size':'1','Mime Type':'application/json'},[],period='1 day')
    patch('api','01A Consultar API REST',{'Response Generation Required':'false'},['Original'])
    patch('retry','Reintentar HTTP hasta 3 veces',{'Maximum Retries':'3','Penalize Retries':'true','Retry Attribute':'lcd301.http_retries'},[])
    existing=c.call('GET',f'/process-groups/{group}/connections')['connections']
    for source,dest,rels in [('trigger','api',['success']),('api','retry',['Retry','Failure']),('api','error_tag',['No Retry']),('retry','api',['retry']),('retry','error_tag',['retries_exceeded','failure'])]:
        s,d=ids['processors'][source],ids['processors'][dest]
        if any(e['component']['source']['id']==s and e['component']['destination']['id']==d for e in existing):continue
        e=c.call('POST',f'/process-groups/{group}/connections',{'revision':{'version':0},'component':{'source':{'id':s,'groupId':group,'type':'PROCESSOR'},'destination':{'id':d,'groupId':group,'type':'PROCESSOR'},'selectedRelationships':rels,'backPressureObjectThreshold':1000,'backPressureDataSizeThreshold':'100 MB','flowFileExpiration':'0 sec'}})
        ids['connections'].append(e['id']);file.write_text(json.dumps(ids,indent=2))
    flow=c.call('GET',f'/flow/process-groups/{group}')['processGroupFlow']['flow']
    processors=flow['processors'];invalid=[{'id':e['id'],'errors':e['component'].get('validationErrors',[])} for e in processors if e['component'].get('validationErrors')]
    (root/'03_Visualizaciones/evidencias/nifi_configuracion.json').write_text(json.dumps([e['component'] for e in processors],indent=2,ensure_ascii=False),encoding='utf-8')
    (root/'02_Codigo/ingesta/flujo_nifi_2.12.0.json').write_bytes(c.call('GET',f'/process-groups/{group}/download',raw=True))
    print(json.dumps({'processors':len(processors),'invalid':invalid}),flush=True)
    if invalid:raise RuntimeError('NiFi contiene procesadores inválidos')

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--project-root',type=Path,required=True);a=p.parse_args();configure(a.project_root)
