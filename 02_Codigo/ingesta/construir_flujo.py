"""Crea un flujo nuevo de NiFi mediante la API; exporta JSON nativo, sin secretos."""
import argparse, json, os, urllib.error
from pathlib import Path
from nifi_client import NiFiClient

def build(project,api_url):
    project=project.resolve()
    evidence=project/'03_Visualizaciones/evidencias'
    evidence.mkdir(parents=True,exist_ok=True)
    state=evidence/'nifi_ids.json'
    if state.exists():
        previous=json.loads(state.read_text(encoding='utf-8'))
        try:
            existing=NiFiClient().call('GET','/process-groups/'+previous['group_id'])['component']
            if existing.get('name')!='LCD301 | Grupo A | E-commerce nuevo':
                raise RuntimeError('El identificador guardado apunta a otro grupo; revisar antes de modificarlo')
            return previous
        except urllib.error.HTTPError as error:
            if error.code!=404:raise
            (evidence/'nifi_ids.previous.json').write_text(json.dumps(previous,indent=2),encoding='utf-8')
    for folder in ['data/inbox/catalog','data/landing/events','data/landing/catalog','data/raw/api','data/quality/ingesta/originales','data/quality/ingesta/metadatos','data/manifests']:
        (project/folder).mkdir(parents=True,exist_ok=True)
    client=NiFiClient()
    root=client.call('GET','/flow/process-groups/root')['processGroupFlow']['id']
    entity=client.call('POST',f'/process-groups/{root}/process-groups',{'revision':{'version':0},'component':{'name':'LCD301 | Grupo A | E-commerce nuevo','position':{'x':3200,'y':100}}})
    group=entity['id']
    ids={'group_id':group,'processors':{},'connections':[],'services':{}}
    def save(): state.write_text(json.dumps(ids,indent=2),encoding='utf-8')
    save()
    types=client.call('GET','/flow/processor-types')['processorTypes']
    stypes=client.call('GET','/flow/controller-service-types')['controllerServiceTypes']
    context=client.call('POST','/parameter-contexts',{'revision':{'version':0},'component':{'name':'LCD301 Grupo A - entorno','parameters':[{'parameter':{'name':'PROJECT_DATA','value':(project/'data').as_posix(),'sensitive':False}},{'parameter':{'name':'API_URL','value':api_url,'sensitive':False}},{'parameter':{'name':'BATCH_ID','value':'base','sensitive':False}},{'parameter':{'name':'CATALOG_SCRIPT','value':(project/'02_Codigo/ingesta/validar_catalogo.groovy').as_posix(),'sensitive':False}}]}})
    entity=client.call('GET','/process-groups/'+group)
    client.call('PUT','/process-groups/'+group,{'revision':entity['revision'],'component':{'id':group,'parameterContext':{'id':context['id']}}})
    ids['parameter_context']=context['id'];save()
    st=next(t for t in stypes if t['type'].endswith('.StandardJsonSchemaRegistry'))
    svc=client.call('POST',f'/process-groups/{group}/controller-services',{'revision':{'version':0},'component':{'type':st['type'],'bundle':st['bundle'],'name':'Contrato event.v1'}})
    schema=(project/'02_Codigo/fuentes/event.v1.schema.json').read_text(encoding='utf-8')
    svc=client.call('PUT','/controller-services/'+svc['id'],{'revision':svc['revision'],'component':{'id':svc['id'],'properties':{'event_v1':schema}}})
    ids['services']['schema']=svc['id'];save()
    client.call('PUT','/controller-services/'+svc['id']+'/run-status',{'revision':svc['revision'],'state':'ENABLED','disconnectedNodeAcknowledged':False})
    def processor(key,typ,name,x,y,values=None,auto=None,period='0 sec'):
        typ=next(t for t in types if t['type'].split('.')[-1]==typ)
        r=client.call('POST',f'/process-groups/{group}/processors',{'revision':{'version':0},'component':{'type':typ['type'],'bundle':typ['bundle'],'name':name,'position':{'x':x,'y':y}}})
        descriptors=r['component']['config'].get('descriptors',{})
        if typ['type'].split('.')[-1]=='InvokeHTTP':
            unknown=[name for name in (values or {}) if name not in descriptors and not any(d.get('displayName')==name for d in descriptors.values())]
            if unknown:raise ValueError('Propiedad HTTP desconocida: '+', '.join(unknown))
        props={next((n for n,d in descriptors.items() if n==k or d.get('displayName')==k),k):v for k,v in (values or {}).items()}
        actual={rel['name'].casefold():rel['name'] for rel in r['component'].get('relationships',[])}
        terminates=[actual.get(rel.casefold(),rel) for rel in (auto or [])]
        r=client.call('PUT','/processors/'+r['id'],{'revision':r['revision'],'component':{'id':r['id'],'config':{'properties':props,'autoTerminatedRelationships':terminates,'schedulingPeriod':period,'penaltyDuration':'1 sec','yieldDuration':'1 sec','concurrentlySchedulableTaskCount':1}}})
        ids['processors'][key]=r['id'];save()
        return {'id':r['id'],'groupId':group,'type':'PROCESSOR'}
    def connect(src,dst,rel,threshold=1000,size='100 MB'):
        if src['type']=='PROCESSOR':
            p=client.call('GET','/processors/'+src['id'])['component']
            actual={r['name'].casefold():r['name'] for r in p.get('relationships',[])}
            rel=[actual.get(r.casefold(),r) for r in rel]
        r=client.call('POST',f'/process-groups/{group}/connections',{'revision':{'version':0},'component':{'source':src,'destination':dst,'selectedRelationships':rel,'backPressureObjectThreshold':threshold,'backPressureDataSizeThreshold':size,'flowFileExpiration':'0 sec'}})
        ids['connections'].append(r['id']);save()
        return r['id']
    def label(text,x,y,w=900,h=70):
        client.call('POST',f'/process-groups/{group}/labels',{'revision':{'version':0},'component':{'label':text,'position':{'x':x,'y':y},'width':w,'height':h,'style':{'font-size':'18px','background-color':'#eaf4f5','color':'#142e43'}}})
    label('GRUPO A | Dos fuentes independientes: eventos REST + catalogo CSV\nNiFi valida y entrega lotes JSONL a Spark; PostgreSQL alimenta los dashboards.',0,-110,1500,90)
    trigger=processor('trigger','GenerateFlowFile','00 Disparar un lote REST',-460,0,{'Custom Text':'{}','Batch Size':'1','Mime Type':'application/json'},period='1 day')
    fetch=processor('api','InvokeHTTP','01A Consultar API REST',0,0,{'HTTP Method':'GET','HTTP URL':'#{API_URL}/events?cursor=0&limit=50000','Connection Timeout':'10 sec','Socket Read Timeout':'60 sec','Response Generation Required':'false'},['original'])
    retry=processor('retry','RetryFlowFile','Reintentar HTTP hasta 3 veces',0,380,{'Maximum Retries':'3','Penalize Retries':'true','Retry Attribute':'lcd301.http_retries'})
    split=processor('split','SplitJson','02 Separar eventos de pagina',460,0,{'JsonPath Expression':'$.events[*]'},['original'])
    meta=processor('api_meta','UpdateAttribute','03 Registrar fuente y lote',920,0,{'source':'rest_api','batch_id':'#{BATCH_ID}','ingested_at':'${now():format("yyyy-MM-dd\'T\'HH:mm:ss.SSSXXX", "UTC")}'})
    validate=processor('validate','ValidateJson','04 Validar contrato de evento',1380,0,{'Schema Access Strategy':'SCHEMA_NAME_PROPERTY','JSON Schema Registry':svc['id'],'Schema Name':'event_v1'})
    enrich=processor('metadata','JoltTransformJSON','05 Anadir tiempo de ingesta',1840,0,{'Jolt Transform':'jolt-transform-chain','Pretty Print':'false','Jolt Specification':json.dumps([{'operation':'default','spec':{'ingested_at':'${ingested_at}','source':'${source}','batch_id':'${batch_id}'}}])})
    merge=processor('merge','MergeContent','06 Formar lote JSONL',2300,0,{'Merge Strategy':'Bin-Packing Algorithm','Merge Format':'Binary Concatenation','Minimum Number of Entries':'2000','Maximum Number of Entries':'10000','Max Bin Age':'5 sec','Delimiter Strategy':'Text','Demarcator':'\n'},['original'])
    fname=processor('filename','UpdateAttribute','07 Nombrar lote cerrado',2760,0,{'filename':'${uuid}.jsonl'})
    write=processor('landing','PutFile','08 Publicar eventos para Spark',3220,0,{'Directory':'#{PROJECT_DATA}/landing/events/${batch_id}','Conflict Resolution Strategy':'fail','Create Missing Directories':'true'},['success'])
    raw=processor('raw_api','PutFile','Conservar pagina original REST',460,380,{'Directory':'#{PROJECT_DATA}/raw/api','Conflict Resolution Strategy':'replace','Create Missing Directories':'true'},['success'])
    csv=processor('catalog','GetFile','01B Recibir catalogo CSV',0,760,{'Input Directory':'#{PROJECT_DATA}/inbox/catalog','File Filter':'.*\\.csv','Keep Source File':'false','Batch Size':'10'},period='1 sec')
    cmeta=processor('catalog_meta','UpdateAttribute','Registrar fuente CSV',460,760,{'source':'catalog_csv','batch_id':'#{BATCH_ID}'})
    cvalidate=processor('catalog_validate','ExecuteScript','Validar CSV y claves de producto',920,760,{'Script Engine':'Groovy','Script File':'#{CATALOG_SCRIPT}'})
    cwrite=processor('catalog_landing','PutFile','Publicar catalogo para Spark',1380,760,{'Directory':'#{PROJECT_DATA}/landing/catalog','Conflict Resolution Strategy':'replace','Create Missing Directories':'true'},['success'])
    error=processor('error_tag','UpdateAttribute','Clasificar rechazo de ingesta',1840,760,{'quality.reason':"${'json.validation.errors':replaceNull('Revisar quality.csv_reason y Provenance para el detalle tecnico')}",'quality.stage':'ingesta','quality.original_filename':'${filename}','quality.at':'${now():format("yyyy-MM-dd\'T\'HH:mm:ss.SSSXXX", "UTC")}','filename':'${uuid}.json'})
    ewrite=processor('error_original','PutFile','Conservar original rechazado',2300,760,{'Directory':'#{PROJECT_DATA}/quality/ingesta/originales','Conflict Resolution Strategy':'fail','Create Missing Directories':'true'})
    ediag=processor('error_meta','AttributesToJSON','Crear diagnostico del rechazo',2760,760,{'Destination':'flowfile-content','Attributes List':'source,batch_id,quality.reason,quality.csv_reason,quality.stage,quality.original_filename,quality.at,json.validation.errors,lcd301.http_retries,invokehttp.status.code,invokehttp.status.message,invokehttp.request.url,invokehttp.java.exception.class,invokehttp.java.exception.message','Include Core Attributes':'true','Pretty Print':'true'})
    ediagwrite=processor('error_diag','PutFile','Guardar motivo de rechazo',3220,760,{'Directory':'#{PROJECT_DATA}/quality/ingesta/metadatos','Conflict Resolution Strategy':'fail','Create Missing Directories':'true'},['success'])
    retain=processor('retain','LogAttribute','RETENCION - fallo de disco',2300,1150,{'Log Level':'warn','Log Prefix':'LCD301_REVISION_MANUAL'},['success'])
    connect(trigger,fetch,['success']);connect(fetch,retry,['retry','failure']);connect(fetch,error,['no retry']);connect(retry,fetch,['retry']);connect(retry,error,['retries_exceeded','failure'])
    connect(fetch,split,['response']);connect(fetch,raw,['response'])
    connect(split,meta,['split']);connect(split,error,['failure'])
    connect(meta,validate,['success']);connect(validate,enrich,['valid']);connect(validate,error,['invalid','failure'])
    connect(enrich,merge,['success']);connect(enrich,error,['failure']);connect(merge,fname,['merged']);connect(merge,error,['failure']);connect(fname,write,['success']);connect(write,error,['failure']);connect(raw,error,['failure'])
    connect(csv,cmeta,['success']);connect(cmeta,cvalidate,['success']);connect(cvalidate,cwrite,['success']);connect(cvalidate,error,['failure']);connect(cwrite,error,['failure'])
    connect(error,ewrite,['success']);connect(ewrite,ediag,['success']);connect(ewrite,retain,['failure']);connect(ediag,ediagwrite,['success']);connect(ediag,retain,['failure']);connect(ediagwrite,retain,['failure'])
    client.state(retain['id'],'DISABLED')
    label('Backpressure: 1000 FlowFiles o 100 MB por cola; no se descartan eventos por expiracion.\nLos rechazos conservan contenido y motivo. Importes en centavos BOB.',0,1180,1500,100)
    export=client.call('GET',f'/process-groups/{group}/download',raw=True)
    flowfile=project/'02_Codigo/ingesta/flujo_nifi_2.12.0.json'
    flowfile.write_bytes(export)
    invalid=[]
    details=[]
    for key,id in ids['processors'].items():
        p=client.call('GET','/processors/'+id)['component']
        details.append({'key':key,'name':p['name'],'id':id,'validationStatus':p.get('validationStatus'),'validationErrors':p.get('validationErrors',[]),'config':p['config']})
        if p.get('validationErrors'): invalid.append({'key':key,'errors':p['validationErrors']})
    (evidence/'nifi_configuracion.json').write_text(json.dumps(details,indent=2,ensure_ascii=False),encoding='utf-8')
    print(json.dumps({'group_id':group,'processors':len(ids['processors']),'connections':len(ids['connections']),'invalid':invalid},ensure_ascii=False))
    return ids

if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--project-root',type=Path,required=True)
    parser.add_argument('--api-url',default='http://127.0.0.1:18080')
    args=parser.parse_args()
    build(args.project_root,args.api_url)
