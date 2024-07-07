import csv
from elasticsearch import Elasticsearch, RequestsHttpConnection, helpers
from requests.auth import HTTPBasicAuth

# Ruta al fichero CSV
csv_file_path = './ChicageCrimeData.csv'

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

# Leer el contenido CSV
with open(csv_file_path, newline='') as csvfile:
    csv_reader = csv.DictReader(csvfile)

    # Preparar datos para la API bulk
    actions = [
        {
            "_index": "tweets-000001",
            "_source": row
        }
        for row in csv_reader
    ]

    # Indexar datos usando la API bulk
    try:
        helpers.bulk(es, actions)
        print("Datos indexados.")
    except Exception as e:
        print(f"Error indexando datos:: {e}")
