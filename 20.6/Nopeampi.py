from opcua import Client
from influxdb_client import InfluxDBClient, Point, WriteOptions
from influxdb_client.client.write_api import SYNCHRONOUS
import logging
import time
from concurrent.futures import ThreadPoolExecutor

opcua_url = "opc.tcp://10.10.10.1:4840"

device_type_dict = {
    0: "Not valid",
    1: "Speed_n1_rpm",
    2: "Speed_%",
    3: "Speed_n2_rpm",
    4: "Current",
    5: "Torque_%_nom",
    6: "Torque_n2_Nm",
    7: "Power",
    8: "Voltage",
    10: "Temperature",
    11: "Position",
    12: "Pressure",
    13: "Flow",
    14: "Moisture",
    20: "Volume",
    21: "Weight",
    30: "Digital In",
    31: "Digital Out",
    32: "Button",
    33: "Warning",
    34: "Fault",
    35: "DCS Digital Input",
    36: "DCS Digital Output",
    37: "Counter",
    40: "Parameter"
}

def HaeTyyppi(enum_value):
    return device_type_dict.get(enum_value, "Unknown enum value")

# Asetetaan lokitus
logging.basicConfig(level=logging.INFO)

def read_opcua_node(client, node_id, retry_attempts=3, retry_delay=5):
    for attempt in range(retry_attempts):
        try:
            value = client.get_node(node_id).get_value()
            return value
        except Exception as e:
            logging.error(f"Error reading node {node_id} on attempt {attempt + 1}/{retry_attempts}: {e}")
            time.sleep(retry_delay)
    return None

def write_to_influxdb(write_api, name, value, tag, tag2):
    try:
        point = Point(tag2).tag("tag", tag).field(name, value)
        write_api.write(bucket=bucket, org=org, record=point)
        logging.info(f"Data written to InfluxDB: {name}={value}, tag={tag}")
    except Exception as e:
        logging.error(f"Error writing to InfluxDB: {e}")

def process_parameters(client, write_api, kpl, node):
    for i in range(kpl):
        Mittauspaikka = node
        node_name_id = f'ns=3;s="{Mittauspaikka}"."stHMI"."stParameter"[{i}]."sName"'
        node_value_id = f'ns=3;s="{Mittauspaikka}"."stHMI"."stParameter"[{i}]."fValue"'
        
        Nimi = read_opcua_node(client, node_name_id)
        Arvo = read_opcua_node(client, node_value_id)
        
        if Nimi is not None and Arvo is not None:
            write_to_influxdb(write_api, Nimi, Arvo, Mittauspaikka, "Parameter")
        else:
            logging.error(f"Failed to read data for node {i}")

def process_buttons(client, write_api, kpl, node):
    for i in range(kpl):
        Mittauspaikka = node
        tag = "Buttons"
        node_name_id = f'ns=3;s="{Mittauspaikka}"."stHMI"."stButtons"[{i}]."sName"'
        node_value_id = f'ns=3;s="{Mittauspaikka}"."stHMI"."stButtons"[{i}]."bStateMemory"'
        
        Nimi = read_opcua_node(client, node_name_id)
        Arvo = read_opcua_node(client, node_value_id)
        Arvo = int(Arvo == True)
        
        if Nimi is not None and Arvo is not None:
            write_to_influxdb(write_api, Nimi, Arvo, Mittauspaikka, "Button")
        else:
            logging.error(f"Failed to read data for node {i}")

def process_data(client, write_api, kpl, node):
    for i in range(kpl):
        Mittauspaikka = node
        tag = "Data"
        node_name_id = f'ns=3;s="{Mittauspaikka}"."stHMI"."stData"[{i}]."eValueType"'
        node_value_id = f'ns=3;s="{Mittauspaikka}"."stHMI"."stData"[{i}]."fValue"'
        
        Nimi = read_opcua_node(client, node_name_id)
        Arvo = read_opcua_node(client, node_value_id)
        
        if Nimi is not None:
            Nimi = HaeTyyppi(int(Nimi))
        
        if Nimi is not None and Arvo is not None:
            write_to_influxdb(write_api, Nimi, Arvo, Mittauspaikka, "Data")
        else:
            logging.error(f"Failed to read data for node {i}")

if __name__ == "__main__":
    token = "E9pLhUMQQqpfNJQuHx8scPF7tjLaIGUbQHvIigrq92OBJ7AGjRVT6hy4NqN9UVcfWTfI1G0CjDlz_kddJ4E52w=="
    org = "Burpellet"
    bucket = "DataCollection"
    url = "http://10.10.10.10:8086"

    client = InfluxDBClient(url=url, token=token, org=org)
    write_api = client.write_api(write_options=SYNCHRONOUS)
    opcua_client = Client(opcua_url)

    with opcua_client, ThreadPoolExecutor(max_workers=10) as executor:
        opcua_client.connect()
        for j in range(100):
            nodes = [
                ("fbAnaIn_Grease PR311 L2", 6, 4, 1),
                ("fbAnaIn_Pressing nip PR311", 6, 4, 1),
                ("fbDirectDrive_Hyd_Motor", 0, 6, 1),
                ("fbDirectDrive_HydCooler", 0, 6, 1),
                ("fbDirectDrive_HydHeating", 0, 6, 1),
                ("fbValve_301_Press", 0, 6, 1),
                ("fbValve_301_Lift", 0, 6, 1),
                ("fbValve_311_Press", 0, 6, 1),
                ("fbValve_311_Lift", 0, 6, 1),
                ("fbValve_Lube_Pump", 0, 6, 1),
                ("fbValve_Lube_Line_1", 0, 6, 1),
                ("fbValve_Lube_Line_2", 0, 6, 1),
                ("fbValve_Lube_A", 0, 6, 1),
                ("fbValve_Lube_B", 0, 6, 1),
                ("fbValve_PR301 floating piston", 0, 6, 1),
                ("fbValve_PR301 floating safety", 0, 6, 1),
                ("fbValve_PR311 floating piston", 0, 6, 1),
                ("fbValve_PR311 floating shaft safety", 0, 6, 1),
                ("fbDistributionscrew_SCR200", 8, 3, 9),
                ("fbLowerPressRoll_PR301", 8, 3, 9),
                ("fbUpperPressRoll_PR301", 8, 3, 9),
                ("fbAuxiliarydrive_PR301", 8, 3, 9),
                ("fbAirBlower_PR301", 8, 3, 9),
                ("fbEffluentfilterscraper_SCA302", 8, 3, 9),
                ("fbDischargeScrew_SCR303", 8, 3, 9),
                ("fbFeedScrew_SCR310", 8, 3, 9),
                ("fbLowerPressRoll_PR311", 8, 3, 9),
                ("fbUpperPressRoll_PR311", 8, 3, 9),
                ("fbAuxiliarydrive_PR311", 8, 3, 9),
                ("fbAirBlower_PR311", 8, 3, 9),
                ("fbEffluentfilterscraper_SCA312", 8, 3, 9),
                ("fbDischargeScrew_SCR313", 8, 3, 9),
                ("fbSpillageliftingscraperconveyor_SCA501", 8, 3, 9),
                ("fbSpillagescraperconveyor_SCA500", 8, 3, 9),
                ("fbAnaIn_Oil temperature", 6, 4, 1),
                ("fbAnaIn_Oil level", 6, 4, 1),
                ("fbAnaIn_Pressure line PR311", 6, 4, 1),
                ("fbAnaIn_Pressure line PR301", 6, 4, 1),
                ("fbAnaIn_Grease PR301 L1", 6, 4, 1),
                ("fbAnaIn_Grease PR301 L2", 6, 4, 1)
            ]
            
            for node in nodes:
                Luettava, param_count, button_count, data_count = node
                if param_count > 0:
                    executor.submit(process_parameters, opcua_client, write_api, param_count, Luettava)
                if button_count > 0:
                    executor.submit(process_buttons, opcua_client, write_api, button_count, Luettava)
                if data_count > 0:
                    executor.submit(process_data, opcua_client, write_api, data_count, Luettava)
                    
        opcua_client.disconnect()
    client.close()
