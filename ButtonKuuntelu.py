import json
from opcua import Client, ua
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

# InfluxDB yhteystiedot
token = "E9pLhUMQQqpfNJQuHx8scPF7tjLaIGUbQHvIigrq92OBJ7AGjRVT6hy4NqN9UVcfWTfI1G0CjDlz_kddJ4E52w=="
org = "Burpellet"
bucket = "NewDataCollection"
url = "http://10.10.10.10:8086"

# OPC UA -asiakasasetukset
opc_ua_url = "opc.tcp://10.10.10.1:4840"

# Lue DataNodet.json tiedosto
with open('ButtonNodet.json', 'r') as file:
    data = json.load(file)

# OPC UA -klientin määritys
client = Client(opc_ua_url)
client.connect()

# InfluxDB -klientin määritys
influx_client = InfluxDBClient(url=url, token=token)
write_api = influx_client.write_api(write_options=SYNCHRONOUS)

# Aiempi arvojen tallennus
previous_values = {}

def write_to_influxdb(measurement_name, alue, node_name, erp_code, tags, serjies_no, hys, value):
    if value is True:
        value = 1
    elif value is False:
        value = 0

    point = Point("Button") \
        .tag("Name", measurement_name) \
        .tag("Alue", alue) \
        .tag("NodeName", node_name) \
        .tag("ErpCode", erp_code) \
        .tag("Tags", tags) \
        .tag("Serjies no", serjies_no) \
        .tag("Hys", hys) \
        .field("value", value)

    write_api.write(bucket=bucket, org=org, record=point)

def parse_data_node(node_data):
    parts = node_data.split('|')
    return {
        'measurement_name': parts[0],
        'node_id_value': parts[1],
        'alue': parts[2],
        'node_name': parts[3],
        'erp_code': parts[4],
        'tags': parts[5],
        'serjies_no': parts[6],
        'hys': parts[7]
    }

def check_value_change(node_info):
    global previous_values
    node = client.get_node(node_info['node_id_value'])
    current_value = node.get_value()
    
    previous_value = previous_values.get(node_info['node_id_value'])
    
    if previous_value is None or current_value != previous_value:
        write_to_influxdb(node_info['measurement_name'], node_info['alue'], node_info['node_name'],
                          node_info['erp_code'], node_info['tags'], node_info['serjies_no'], node_info['hys'], current_value)
        previous_values[node_info['node_id_value']] = current_value
        print(f"Value changed for {node_info['node_name']}")

        # Kirjoita arvo takaisin falseksi ilman tilaa ja aikaleimaa
        try:
            node.set_value(ua.DataValue(False))
            print(f"Set value to False for {node_info['node_name']}")
        except ua.UaStatusCodeError as e:
            print(f"Error setting value for {node_info['node_name']}: {e}")

try:
    parsed_data_nodes = [parse_data_node(node_data) for node_data in data['ButtonNodet']]
    while True:
        for node_info in parsed_data_nodes:
            check_value_change(node_info)
except KeyboardInterrupt:
    client.disconnect()
    influx_client.close()
