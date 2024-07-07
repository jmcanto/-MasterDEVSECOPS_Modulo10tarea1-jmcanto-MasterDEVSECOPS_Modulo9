from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests.auth import HTTPBasicAuth

# Ruta al certificado CA
ca_cert_path = '/etc/elasticsearch/certs/http_ca.crt'

# Conexión a Elasticsearch usando HTTPS y autenticación básica
es = Elasticsearch(
    ['https://localhost:9200'],
    http_auth=HTTPBasicAuth('usuario-elastic', 'clave-elastic'),
    use_ssl=True,
    verify_certs=True,
    ca_certs=ca_cert_path,
    connection_class=RequestsHttpConnection
)

# Verificar el estado del cluster
try:
    health = es.cluster.health()
    print(health)
except Exception as e:
    print(f"Error conetándose a Elasticsearch: {e}")
    
    
