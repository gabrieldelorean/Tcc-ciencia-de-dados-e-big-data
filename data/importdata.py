from elasticsearch_dsl import Document, Date, Integer, Text,Keyword,GeoPoint
from elasticsearch_dsl.connections import connections
from elasticsearch.helpers import bulk
import csv
import pandas as pd
connections.create_connection(hosts=["elastic:changeme@127.0.0.1:9200"])

class WordCloud(Document):
    id = Integer()
    classificacao_acidente = Keyword() 
    dia_semana = Keyword()  
    causa_acidente = Keyword()
    tipo_acidente = Keyword()
    condicao_metereologica = Keyword() 
    
    class Index:
        name = "data"
        doc_type = "csv"   # If you need it to be called so
    
#connections.get_connection().indices.delete(index='data')
WordCloud.init()


#remoção de colunas insignificantes para o projeto  e de tuplas dulicadas
with open('datatran2020.csv', encoding="utf8") as f:
    df = pd.read_csv(f,sep=';')
    df.drop(["latitude","longitude","regional","delegacia","uop","tipo_pista","tracado_via","uso_solo","feridos","veiculos","sentido_via","km","ilesos","ignorados"], axis=1, inplace=True)
    df.to_csv("datatran_ready.csv",index=False,sep="|",date_format='%dd%mm%yyyy')
  

with open('datatran_ready.csv', encoding="utf8") as f:
    reader = csv.DictReader(f,delimiter='|')      
    bulk(
        connections.get_connection(),
        (WordCloud(**row).to_dict(True) for row in reader)
    )
    
 