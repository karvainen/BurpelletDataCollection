import json
import time
from opcua import Client, ua
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
import config

global TestInfo
TestInfo = "Normal"





# Lue DataNodet.json tiedosto
with open('DataNodet.json', 'r') as file:
    data = json.load(file)

def create_opc_ua_client():
    while True:
        try:
            client = Client(config.plc_ip)
            client.connect()
            print("Connected to OPC UA")
            return client
        except Exception as e:
            print(f"No connect to PLC Check connection !!! {e}")
            time.sleep(5)  # Yritä uudelleen 5 sekunnin kuluttua

# OPC UA -klientin määritys
client = create_opc_ua_client()

def create_influxdb_client():
    while True:
        try:
            influx_client = InfluxDBClient(url=config.influx_ip, token=config.token)
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
    global TestInfo
    if measurement_name != "TestInfo":

        global influx_client, write_api
        point = Point("Data") \
            .tag("TestInfo", TestInfo) \
            .tag("Part", alue) \
            .tag("ERP", erp_code) \
            .tag("Tag", tags) \
            .tag("SerialNumber", serjies_no) \
            .tag("Country", country) \
            .tag("Type", type) \
            .field(measurement_name, value)
        
        while True:
            try:
                write_api.write(bucket=config.bucket, org=config.org, record=point)
                break
            except Exception as e:
                print(f"Failed to write to InfluxDB: {e}")
                influx_client, write_api = create_influxdb_client()
    else:
        TestInfo = value


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
        node = client.get_node(node_info['node_id_value'])
        current_value = node.get_value()
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
        write_to_influxdb(node_info['measurement_name'], node_info['alue'], node_info['node_name'],
                          node_info['erp_code'], node_info['tags'], node_info['serjies_no'], node_info['hys'], current_value, node_info['country'], node_info['Type'])
        previous_values[node_info['node_id_value']] = current_value
        print(f"Value changed for {node_info['node_name']}")

try:
    parsed_data_nodes = [parse_data_node(node_data) for node_data in data['DataNodet']]
    while True:
        for node_info in parsed_data_nodes:
            check_value_change(node_info)
        time.sleep(0)  # Odota 1 sekunti ennen seuraavaa kyselyä
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
