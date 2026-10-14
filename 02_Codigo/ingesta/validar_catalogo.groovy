import java.nio.charset.StandardCharsets
import org.apache.nifi.processor.io.InputStreamCallback
def ff=session.get()
if(!ff) return
def text=''
session.read(ff,{stream -> text=stream.getText('UTF-8')} as InputStreamCallback)
def reason=null
try {
    def rows=text.readLines().findAll{it.trim()}
    if(rows.size()<2) throw new Exception('CSV sin registros')
    def expected=['product_id','name','category','active','catalog_version','list_price_cents']
    if(rows[0].replace('\uFEFF','').split(',').toList()!=expected) throw new Exception('Cabecera de catálogo no coincide con contrato')
    def ids=[] as Set
    rows.drop(1).eachWithIndex { row,i ->
        def fields=row.split(',',-1)
        if(fields.size()!=6) throw new Exception('Fila '+(i+2)+': número de columnas inválido; el contrato no admite comas en textos')
        if(!fields[0].matches('PRD[0-9]{3}') || !ids.add(fields[0])) throw new Exception('Fila '+(i+2)+': SKU inválido o repetido')
        if(!fields[1].trim() || !fields[2].trim() || !fields[4].trim()) throw new Exception('Fila '+(i+2)+': texto requerido vacío')
        if(!(fields[3].toLowerCase() in ['true','false'])) throw new Exception('Fila '+(i+2)+': active debe ser true/false')
        if(!fields[5].matches('[0-9]+') || fields[5].toLong()<=0) throw new Exception('Fila '+(i+2)+': precio inválido')
    }
    ff=session.putAttribute(ff,'catalog.records',Integer.toString(rows.size()-1))
} catch(Exception e) { reason=e.message }
if(reason) {
    ff=session.putAttribute(ff,'quality.csv_reason',reason)
    session.transfer(ff,REL_FAILURE)
} else { session.transfer(ff,REL_SUCCESS) }
