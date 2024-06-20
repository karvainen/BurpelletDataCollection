import json
import time
import threading
import timeit
from concurrent.futures import ThreadPoolExecutor, as_completed
from opcua import Client, ua
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

# InfluxDB yhteystiedot
token = "E9pLhUMQQqpfNJQuHx8scPF7tjLaIGUbQHvIigrq92OBJ7AGjRVT6hy4NqN9UVcfWTfI1G0CjDlz_kddJ4E52w=="
org = "Burpellet"
bucket = "DataCollection"
url = "http://10.10.10.10:8086"

# OPC UA -asiakasasetukset
opc_ua_url = "opc.tcp://10.10.10.1:4840"

# Lue DataNodet.json tiedosto
with open('DataNodet.json', 'r') as file:
    data = json.load(file)

def create_opc_ua_client():
    while True:
        try:
            client = Client(opc_ua_url)
            client.connect()
            print("Connected to OPC UA")
            return client
        except Exception as e:
            print(f"Failed to connect to OPC UA: {e}")
            time.sleep(5)  # Yritä uudelleen 5 sekunnin kuluttua

# OPC UA -klientin määritys
client = create_opc_ua_client()

def create_influxdb_client():
    while True:
        try:
            influx_client = InfluxDBClient(url=url, token=token)
            write_api = influx_client.write_api(write_options=SYNCHRONOUS)
            print("Connected to InfluxDB")
            return influx_client, write_api
        except Exception as e:
            print(f"Failed to connect to InfluxDB: {e}")
            time.sleep(5)  # Yritä uudelleen 5 sekunnin kuluttua

# InfluxDB -klientin määritys
influx_client, write_api = create_influxdb_client()

# Aiempi arvojen tallennus
previous_values = {}

def write_to_influxdb(measurement_name, alue, node_name, erp_code, tags, serjies_no, hys, value, country, type):
    global influx_client, write_api
    point = Point("Data") \
        .tag("Part", alue) \
        .tag("ERP", erp_code) \
        .tag("Tag", tags) \
        .tag("SerialNumper", serjies_no) \
        .tag("Country", country) \
        .tag("Type", type) \
        .field(measurement_name, value)
    
    while True:
        try:
            write_api.write(bucket=bucket, org=org, record=point)
            break
        except Exception as e:
            print(f"Failed to write to InfluxDB: {e}")
            influx_client, write_api = create_influxdb_client()

def parse_data_node(node_data):
    parts = node_data.split('|')
    return {
        'measurement_name': parts[0],
        'node_id_value': parts[1],
        'alue': parts[2],
        'node_name': parts[3],  # Lisätty node_name
        'erp_code': parts[4],
        'tags': parts[5],
        'serjies_no': parts[6],
        'hys': parts[7],
        'country': parts[9],  # Lisätty country
        'Type': parts[10]
    }

def check_value_change(node_info):
    global previous_values, client
    try:
        start_read = timeit.default_timer()
        node = client.get_node(node_info['node_id_value'])
        current_value = node.get_value()
        elapsed_read = timeit.default_timer() - start_read
    except Exception as e:
        print(f"Error reading value from OPC UA: {e}")
        try:
            client.disconnect()
        except:
            pass
        client = create_opc_ua_client()
        return
    
    previous_value = previous_values.get(node_info['node_id_value'])
    
    if previous_value is None or current_value != previous_value:
        start_write = timeit.default_timer()
        write_to_influxdb(node_info['measurement_name'], node_info['alue'], node_info['node_name'],
                          node_info['erp_code'], node_info['tags'], node_info['serjies_no'], node_info['hys'], current_value, node_info['country'], node_info['Type'])
        elapsed_write = timeit.default_timer() - start_write
        previous_values[node_info['node_id_value']] = current_value
        print(f"Value changed for {node_info['node_name']} (Read: {elapsed_read:.4f}s, Write: {elapsed_write:.4f}s)")

def process_node(node_info):
    start_time = timeit.default_timer()
    check_value_change(node_info)
    elapsed_time = timeit.default_timer() - start_time
    print(f"Processing node {node_info['node_name']} took {elapsed_time:.4f} seconds")

try:
    parsed_data_nodes = [parse_data_node(node_data) for node_data in data['DataNodet']]
    with ThreadPoolExecutor(max_workers=10) as executor:
        while True:
            futures = [executor.submit(process_node, node_info) for node_info in parsed_data_nodes]
            for future in as_completed(futures):
                future.result()
            time.sleep(1)  # Odota 1 sekunti ennen seuraavaa kyselyä
except KeyboardInterrupt:
    try:
        client.disconnect()
    except:
        pass
    try:
        influx_client.close()
    except:
        pass
    print("Disconnected from OPC UA and InfluxDB clients")
