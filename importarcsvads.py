import csv
from elasticsearch import Elasticsearch, RequestsHttpConnection, helpers
from requests.auth import HTTPBasicAuth
from datetime import datetime
from dateutil import parser

# Ruta al fichero CSV
csv_file_path = './ChicageCrimeData.csv'

# Ruta al certificado CA
ca_cert_path = '/etc/elasticsearch/certs/http_ca.crt'

# Conexión a Elasticsearch usando HTTPS y autenticación básica
es = Elasticsearch(
    ['https://localhost:9200'],
    http_auth=HTTPBasicAuth('elastic', 'vtnGs_P2Y-9qXU06hN3l'),
    use_ssl=True,
    verify_certs=True,
    ca_certs=ca_cert_path,
    connection_class=RequestsHttpConnection
)

# Función para convertir fechas al formato ISO 8601
def convert_to_iso(date_str):
    return parser.parse(date_str).isoformat()

# Leer el contenido CSV y cargar los datos en lotes
def bulk_load_data(file_path, data_stream_name, batch_size=500):
    with open(file_path, newline='') as csvfile:
        csv_reader = csv.DictReader(csvfile)
        actions = []
        for row in csv_reader:
            action = {
                "_op_type": "create",
                "_index": data_stream_name,
                "_source": {
                    "@timestamp": datetime.now().isoformat(),
                    "ID": int(row["ID"]),
                    "Case Number": row["Case Number"],
                    "Date": datetime.strptime(row["Date"], '%m/%d/%Y %I:%M:%S %p'),
                    "Block": row["Block"],
                    "IUCR": row["IUCR"],
                    "Primary Type": row["Primary Type"],
                    "Description": row["Description"],
                    "Location Description": row["Location Description"],
                    "Arrest": row["Arrest"].lower() == "true",
                    "Domestic": row["Domestic"].lower() == "true",
                    "Beat": row["Beat"],
                    "District": row["District"],
                    "Ward": row["Ward"],
                    "Community Area": row["Community Area"],
                    "FBI Code": row["FBI Code"],
                    "X Coordinate": float(row["X Coordinate"]) if row["X Coordinate"] else None,
                    "Y Coordinate": float(row["Y Coordinate"]) if row["Y Coordinate"] else None,
                    "Year": int(row["Year"]),
                    "Updated On": datetime.strptime(row["Updated On"], '%m/%d/%Y %I:%M:%S %p'),
                    "Latitude": float(row["Latitude"]) if row["Latitude"] else None,
                    "Longitude": float(row["Longitude"]) if row["Longitude"] else None,
                    "Location": row["Location"]
                    #"Location": {
                    #    "lat": float(row["Latitude"]) if row["Latitude"] else None,
                    #    "lon": float(row["Longitude"]) if row["Longitude"] else None
                    #}
                }
            }
            actions.append(action)
            # Cuando el lote alcanza el tamaño definido, se carga en Elasticsearch
            if len(actions) >= batch_size:
                helpers.bulk(es, actions)
                actions = []  # Reiniciar la lista de acciones
        # Cargar cualquier remanente de acciones que no alcanzaron el tamaño del lote
        if actions:
            helpers.bulk(es, actions)

# Nombre del data stream donde deseas almacenar los datos
data_stream_name = 'tweets-main'#000001'

# Ejecutar la carga de datos
try:
    bulk_load_data(csv_file_path, data_stream_name)
    print("Datos indexados correctamente.")
except Exception as e:
    print(f"Error indexando los datos: {e}")
