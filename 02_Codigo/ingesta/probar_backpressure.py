"""Prueba real, aislada: 5 FlowFiles detienen al productor hasta liberar la cola."""
import argparse,json,time
from pathlib import Path
from nifi_client import NiFiClient

def test(root):
    root=Path(root).resolve();ev=root/'03_Visualizaciones/evidencias';c=NiFiClient()
    parent=c.call('GET','/flow/process-groups/root')['processGroupFlow']['id']
    e=c.call('POST',f'/process-groups/{parent}/process-groups',{'revision':{'version':0},'component':{'name':'LCD301 | Grupo A | Prueba aislada backpressure','position':{'x':3200,'y':2200}}});g=e['id']
    types=c.call('GET','/flow/processor-types')['processorTypes'];ids={}
    for key,typ,props,auto,x in [('producer','GenerateFlowFile',{'Custom Text':'LCD301_PRUEBA_SOLO_BACKPRESSURE','Batch Size':'1'},[],0),('consumer','LogAttribute',{'Log Level':'debug'},['success'],450)]:
        t=next(t for t in types if t['type'].split('.')[-1]==typ)
        e=c.call('POST',f'/process-groups/{g}/processors',{'revision':{'version':0},'component':{'name':key,'type':t['type'],'bundle':t['bundle'],'position':{'x':x,'y':0}}})
        ids[key]=e['id'];c.call('PUT','/processors/'+e['id'],{'revision':e['revision'],'component':{'id':e['id'],'config':{'properties':props,'schedulingPeriod':'10 ms','autoTerminatedRelationships':auto,'concurrentlySchedulableTaskCount':1}}})
    conn=c.call('POST',f'/process-groups/{g}/connections',{'revision':{'version':0},'component':{'source':{'id':ids['producer'],'groupId':g,'type':'PROCESSOR'},'destination':{'id':ids['consumer'],'groupId':g,'type':'PROCESSOR'},'selectedRelationships':['success'],'backPressureObjectThreshold':5,'backPressureDataSizeThreshold':'1 KB','flowFileExpiration':'0 sec'}})
    def snap():return c.call('GET',f'/flow/process-groups/{g}/status')['processGroupStatus']['aggregateSnapshot']
    c.state(ids['producer'],'RUNNING');time.sleep(3)
    a=snap();time.sleep(3);b=snap()
    c.state(ids['producer'],'STOPPED');c.state(ids['consumer'],'RUNNING');time.sleep(3);drained=snap()
    c.state(ids['consumer'],'STOPPED');c.state(ids['producer'],'DISABLED');c.state(ids['consumer'],'DISABLED')
    blocked=a['flowFilesQueued']>=5 and b['flowFilesQueued']==a['flowFilesQueued']
    final=snap();passed=blocked and final['flowFilesQueued']==0
    report={'passed':passed,'isolated_group_id':g,'connection_id':conn['id'],'threshold_flowfiles':5,'first':a,'second':b,'drained':final,'test_data_only':True,'final_processors_disabled':True}
    (ev/'nifi_backpressure.json').write_text(json.dumps(report,indent=2,ensure_ascii=False),encoding='utf-8')
    (root/'02_Codigo/ingesta/prueba_backpressure_nifi.json').write_bytes(c.call('GET',f'/process-groups/{g}/download',raw=True))
    print(json.dumps({'passed':passed,'first_queue':a['flowFilesQueued'],'second_queue':b['flowFilesQueued'],'final_queue':final['flowFilesQueued']}),flush=True)
    if not passed:raise AssertionError('No se comprobó bloqueo y posterior vaciado')

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--project-root',type=Path,required=True);a=p.parse_args();test(a.project_root)
