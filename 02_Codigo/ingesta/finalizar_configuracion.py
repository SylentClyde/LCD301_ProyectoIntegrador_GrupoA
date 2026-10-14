"""Sincroniza las rutas de error restantes y exporta el flujo detenido de Grupo A."""
import argparse,json
from pathlib import Path
from nifi_client import NiFiClient
from ejecutar_lote import patch

def finish(root):
    root=Path(root).resolve();ids=json.loads((root/'03_Visualizaciones/evidencias/nifi_ids.json').read_text(encoding='utf-8'));c=NiFiClient();g=ids['group_id'];p=ids['processors']
    status=c.call('GET',f'/flow/process-groups/{g}/status')['processGroupStatus']['aggregateSnapshot']
    if status['flowFilesQueued'] or status['activeThreadCount']:raise RuntimeError('Solo se configura después de vaciar el flujo')
    flow=c.call('GET',f'/flow/process-groups/{g}')['processGroupFlow']['flow']
    if any(e['component']['state']=='RUNNING' for e in flow['processors']):raise RuntimeError('El grupo debe estar detenido')
    for key,auto in [('merge',['original']),('error_meta',[])]:
        e=c.call('GET','/processors/'+p[key]);c.call('PUT','/processors/'+e['id'],{'revision':e['revision'],'component':{'id':e['id'],'config':{'autoTerminatedRelationships':auto}}})
    existing=flow['connections']
    for src in ['merge','error_meta']:
        dest='error_tag' if src=='merge' else 'retain'
        if any(e['component']['source']['id']==p[src] and e['component']['destination']['id']==p[dest] and 'failure' in e['component']['selectedRelationships'] for e in existing):continue
        e=c.call('POST',f'/process-groups/{g}/connections',{'revision':{'version':0},'component':{'source':{'id':p[src],'groupId':g,'type':'PROCESSOR'},'destination':{'id':p[dest],'groupId':g,'type':'PROCESSOR'},'selectedRelationships':['failure'],'backPressureObjectThreshold':1000,'backPressureDataSizeThreshold':'100 MB','flowFileExpiration':'0 sec'}})
        ids['connections'].append(e['id']);(root/'03_Visualizaciones/evidencias/nifi_ids.json').write_text(json.dumps(ids,indent=2),encoding='utf-8')
    patch(c,p['error_meta'],{'Attributes List':'source,batch_id,quality.reason,quality.csv_reason,quality.stage,quality.original_filename,quality.at,json.validation.errors,lcd301.http_retries,invokehttp.status.code,invokehttp.status.message,invokehttp.request.url,invokehttp.java.exception.class,invokehttp.java.exception.message'})
    flow=c.call('GET',f'/flow/process-groups/{g}')['processGroupFlow']['flow'];processors=[e['component'] for e in flow['processors']]
    invalid=[{'name':e['name'],'errors':e.get('validationErrors')} for e in processors if e.get('validationErrors')]
    (root/'03_Visualizaciones/evidencias/nifi_configuracion.json').write_text(json.dumps(processors,indent=2,ensure_ascii=False),encoding='utf-8')
    (root/'02_Codigo/ingesta/flujo_nifi_2.12.0.json').write_bytes(c.call('GET',f'/process-groups/{g}/download',raw=True))
    report={'processors':len(processors),'connections':len(flow['connections']),'invalid':invalid,'all_stopped_or_disabled':True,'failure_routes_preserved':True}
    (root/'03_Visualizaciones/evidencias/nifi_configuracion_final.json').write_text(json.dumps(report,indent=2),encoding='utf-8');print(json.dumps(report),flush=True)
    if invalid:raise RuntimeError('NiFi inválido')

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--project-root',type=Path,required=True);a=p.parse_args();finish(a.project_root)
