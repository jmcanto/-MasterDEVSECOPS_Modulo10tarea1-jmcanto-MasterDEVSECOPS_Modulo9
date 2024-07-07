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

# Leer el contenido CSV y cargar los datos en lotes
def bulk_load_data(file_path, index_name, batch_size=1000):  # Aumentar el tamaño del lote aquí
    with open(file_path, newline='') as csvfile:
        csv_reader = csv.DictReader(csvfile)
        actions = []
        for row in csv_reader:
            action = {
                "_index": index_name,
                "_source": row
            }
            actions.append(action)
            # Cuando el lote alcanza el tamaño definido, se carga en Elasticsearch
            if len(actions) >= batch_size:
                helpers.bulk(es, actions)
                actions = []  # Reiniciar la lista de acciones
        # Cargar cualquier remanente de acciones que no alcanzaron el tamaño del lote
        if actions:
            helpers.bulk(es, actions)

# Nombre del índice donde deseas almacenar los datos
index_name = 'tweets-000001'

# Ejecutar la carga de datos
try:
    bulk_load_data(csv_file_path, index_name)
    print("Datos indexados.")
except Exception as e:
    print(f"Error indexando los datos: {e}")
