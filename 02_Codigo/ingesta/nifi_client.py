"""Cliente de laboratorio; nunca exporta credenciales ni tokens."""
import json, os, re, ssl, urllib.request, urllib.parse
from pathlib import Path

class NiFiClient:
    def __init__(self, url=None):
        self.url=(url or os.environ.get('NIFI_URL','https://localhost:8443/nifi-api')).rstrip('/')
        self.context=ssl.create_default_context()
        if os.environ.get('NIFI_VERIFY_TLS','false').lower()=='false':
            allowed={'localhost','127.0.0.1'}
            if os.environ.get('NIFI_ALLOW_INSECURE_HOST'):
                allowed.add(os.environ['NIFI_ALLOW_INSECURE_HOST'])
            if urllib.parse.urlparse(self.url).hostname not in allowed:
                raise ValueError('TLS sin verificación requiere localhost o NIFI_ALLOW_INSECURE_HOST explícito para el laboratorio')
            self.context=ssl._create_unverified_context()
        username,password=os.getenv('NIFI_USERNAME'),os.getenv('NIFI_PASSWORD')
        if not (username and password):
            runtime=Path(os.environ['NIFI_RUNTIME'])
            log=(runtime/'logs/nifi-app.log').read_text(encoding='utf-8')
            username=re.findall(r'Generated Username \[(.*?)\]',log)[-1]
            password=re.findall(r'Generated Password \[(.*?)\]',log)[-1]
        token=urllib.request.urlopen(urllib.request.Request(self.url+'/access/token',data=urllib.parse.urlencode({'username':username,'password':password}).encode()),context=self.context,timeout=30).read().decode()
        self.headers={'Authorization':'Bearer '+token,'Content-Type':'application/json'}
    def call(self,method,path,data=None,raw=False):
        request=urllib.request.Request(self.url+path,data=None if data is None else json.dumps(data).encode(),headers=self.headers,method=method)
        with urllib.request.urlopen(request,context=self.context,timeout=90) as response: body=response.read()
        return body if raw else json.loads(body) if body else None
    def state(self,id,state):
        entity=self.call('GET','/processors/'+id)
        return self.call('PUT','/processors/'+id+'/run-status',{'revision':entity['revision'],'state':state,'disconnectedNodeAcknowledged':False})
