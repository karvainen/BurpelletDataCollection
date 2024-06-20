from opcua import Client
from influxdb_client import InfluxDBClient, Point, WriteOptions
from influxdb_client.client.write_api import SYNCHRONOUS
import logging
import time
import logging
logging.basicConfig(level=logging.ERROR)
opcua_url = "opc.tcp://10.10.10.1:4840"

print("Datankeräys käynnissä älä koske!")

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
#logging.basicConfig(level=logging.INFO)

def read_opcua_node(opcua_url, node_id, retry_attempts=3, retry_delay=5):
    for attempt in range(retry_attempts):
        client = Client(opcua_url)
        try:
            client.connect()
            value = client.get_node(node_id).get_value()
            client.disconnect()
            return value
        except Exception as e:
            logging.error(f"Error reading node {node_id} on attempt {attempt + 1}/{retry_attempts}: {e}")
            time.sleep(retry_delay)
        finally:
            try:
                client.disconnect()
            except Exception:
                pass
    return None

def write_to_influxdb(name, value, tag, tag2):
    # InfluxDB yhteystiedot
    token = "E9pLhUMQQqpfNJQuHx8scPF7tjLaIGUbQHvIigrq92OBJ7AGjRVT6hy4NqN9UVcfWTfI1G0CjDlz_kddJ4E52w=="
    org = "Burpellet"
    bucket = "DataCollection"
    url = "http://10.10.10.10:8086"  # Korvaa oikealla URL-osoitteella

    

    client = InfluxDBClient(url=url, token=token, org=org)
    write_api = client.write_api(write_options=SYNCHRONOUS)
    
    try:
        point = Point(tag2).tag("tag", tag).field(name, value)
        write_api.write(bucket=bucket, org=org, record=point)
        #logging.info(f"Data written to InfluxDB: {name}={value}, tag={tag}")
    except Exception as e:
        logging.error(f"Error writing to InfluxDB: {e}")
    finally:
        client.close()


def Parametrit(kpl, node):
    for i in range(kpl):
        
        Mittauspaikka = node
        
        
        node_name_id = f'ns=3;s="{Mittauspaikka}"."stHMI"."stParameter"[{i}]."sName"'
        node_value_id = f'ns=3;s="{Mittauspaikka}"."stHMI"."stParameter"[{i}]."fValue"'
    
        Nimi = read_opcua_node(opcua_url, node_name_id)
        Arvo = read_opcua_node(opcua_url, node_value_id)
        
        if Nimi is not None and Arvo is not None:
            write_to_influxdb(Nimi, Arvo, Mittauspaikka, "Parameter")
        else:
            logging.error(f"Failed to read data for node {i}")


def Napit(kpl, node):
    for i in range(kpl):

        Mittauspaikka = node
        tag = "Buttons"

        node_name_id = f'ns=3;s="{Mittauspaikka}"."stHMI"."stButtons"[{i}]."sName"'
        node_value_id = f'ns=3;s="{Mittauspaikka}"."stHMI"."stButtons"[{i}]."bStateMemory"'
    
        Nimi = read_opcua_node(opcua_url, node_name_id)
        Arvo = read_opcua_node(opcua_url, node_value_id)

        if Arvo == True:
            Arvo = int(1)
        else:
            Arvo = int(0)

        if Nimi is not None and Arvo is not None:
            write_to_influxdb(Nimi, Arvo, Mittauspaikka, "Button")
        else:
            logging.error(f"Failed to read data for node {i}")


def StData(kpl, node):
    for i in range(kpl):
        Mittauspaikka = node
        tag = "Data"

        
        node_name_id = f'ns=3;s="{Mittauspaikka}"."stHMI"."stData"[{i}]."eValueType"'
        node_value_id = f'ns=3;s="{Mittauspaikka}"."stHMI"."stData"[{i}]."fValue"'
    
        Nimi = read_opcua_node(opcua_url, node_name_id)
        Arvo = read_opcua_node(opcua_url, node_value_id)
        if Nimi is not None:
            Nimi = HaeTyyppi(int(Nimi))
        else:
            print(f"Warning: Nimi is None for Luettava: {Luettava}")


        if Nimi is not None and Arvo is not None:
            write_to_influxdb(Nimi, Arvo, Mittauspaikka, "Data")
        else:
            logging.error(f"Failed to read data for node {i}")






for j in range(100):
    Luettava = "fbAnaIn_Grease PR311 L2"
    Parametrit(6, Luettava)
    Napit(4, Luettava)
    StData(1, Luettava)

    Luettava = "fbAnaIn_Pressing nip PR311"
    Parametrit(6, Luettava)
    Napit(4, Luettava)
    StData(1, Luettava)

    Luettava = "fbDirectDrive_Hyd_Motor"
    Napit(6, Luettava)
    StData(1, Luettava)

    Luettava = "fbDirectDrive_HydCooler"
    Napit(6, Luettava)
    StData(1, Luettava)

    Luettava = "fbDirectDrive_HydHeating"
    Napit(6, Luettava)
    StData(1, Luettava)

    Luettava = "fbValve_301_Press"
    Napit(6, Luettava)
    StData(1, Luettava)

    Luettava = "fbValve_301_Lift"
    Napit(6, Luettava)
    StData(1, Luettava)

    Luettava = "fbValve_311_Press"
    Napit(6, Luettava)
    StData(1, Luettava)

    Luettava = "fbValve_311_Lift"
    Napit(6, Luettava)
    StData(1, Luettava)

    Luettava = "fbValve_Lube_Pump"
    Napit(6, Luettava)
    StData(1, Luettava)

    Luettava = "fbValve_Lube_Line_1"
    Napit(6, Luettava)
    StData(1, Luettava)

    Luettava = "fbValve_Lube_Line_2"
    Napit(6, Luettava)
    StData(1, Luettava)

    Luettava = "fbValve_Lube_A"
    Napit(6, Luettava)
    StData(1, Luettava)

    Luettava = "fbValve_Lube_B"
    Napit(6, Luettava)
    StData(1, Luettava)

    Luettava = "fbValve_PR301 floating piston"
    Napit(6, Luettava)
    StData(1, Luettava)

    Luettava = "fbValve_PR301 floating safety"
    Napit(6, Luettava)
    StData(1, Luettava)

    Luettava = "fbValve_PR311 floating piston"
    Napit(6, Luettava)
    StData(1, Luettava)

    Luettava = "fbValve_PR311 floating shaft safety"
    Napit(6, Luettava)
    StData(1, Luettava)

    Luettava = "fbDistributionscrew_SCR200"
    Parametrit(8, Luettava)
    Napit(3, Luettava)
    StData(9, Luettava)

    Luettava = "fbLowerPressRoll_PR301"
    Parametrit(8, Luettava)
    Napit(3, Luettava)
    StData(9, Luettava)

    Luettava = "fbUpperPressRoll_PR301"
    Parametrit(8, Luettava)
    Napit(3, Luettava)
    StData(9, Luettava)

    Luettava = "fbAuxiliarydrive_PR301"
    Parametrit(8, Luettava)
    Napit(3, Luettava)
    StData(9, Luettava)

    Luettava = "fbAirBlower_PR301"
    Parametrit(8, Luettava)
    Napit(3, Luettava)
    StData(9, Luettava)

    Luettava = "fbEffluentfilterscraper_SCA302"
    Parametrit(8, Luettava)
    Napit(3, Luettava)
    StData(9, Luettava)

    Luettava = "fbDischargeScrew_SCR303"
    Parametrit(8, Luettava)
    Napit(3, Luettava)
    StData(9, Luettava)

    Luettava = "fbFeedScrew_SCR310"
    Parametrit(8, Luettava)
    Napit(3, Luettava)
    StData(9, Luettava)

    Luettava = "fbLowerPressRoll_PR311"
    Parametrit(8, Luettava)
    Napit(3, Luettava)
    StData(9, Luettava)

    Luettava = "fbUpperPressRoll_PR311"
    Parametrit(8, Luettava)
    Napit(3, Luettava)
    StData(9, Luettava)

    Luettava = "fbAuxiliarydrive_PR311"
    Parametrit(8, Luettava)
    Napit(3, Luettava)
    StData(9, Luettava)

    Luettava = "fbAirBlower_PR311"
    Parametrit(8, Luettava)
    Napit(3, Luettava)
    StData(9, Luettava)

    Luettava = "fbEffluentfilterscraper_SCA312"
    Parametrit(8, Luettava)
    Napit(3, Luettava)
    StData(9, Luettava)

    Luettava = "fbDischargeScrew_SCR313"
    Parametrit(8, Luettava)
    Napit(3, Luettava)
    StData(9, Luettava)

    Luettava = "fbSpillageliftingscraperconveyor_SCA501"
    Parametrit(8, Luettava)
    Napit(3, Luettava)
    StData(9, Luettava)

    Luettava = "fbSpillagescraperconveyor_SCA500"
    Parametrit(8, Luettava)
    Napit(3, Luettava)
    StData(9, Luettava)

    Luettava = "fbAnaIn_Oil temperature"
    Parametrit(6, Luettava)
    Napit(4, Luettava)
    StData(1, Luettava)

    Luettava = "fbAnaIn_Oil level"
    Parametrit(6, Luettava)
    Napit(4, Luettava)
    StData(1, Luettava)

    Luettava = "fbAnaIn_Pressure line PR311"
    Parametrit(6, Luettava)
    Napit(4, Luettava)
    StData(1, Luettava)

    Luettava = "fbAnaIn_Pressure line PR301"
    Parametrit(6, Luettava)
    Napit(4, Luettava)
    StData(1, Luettava)

    Luettava = "fbAnaIn_Grease PR301 L1"
    Parametrit(6, Luettava)
    Napit(4, Luettava)
    StData(1, Luettava)

    Luettava = "fbAnaIn_Grease PR301 L2"
    Parametrit(6, Luettava)
    Napit(4, Luettava)
    StData(1, Luettava)









"""
for j in range(100):
    
    ##### Luetaan fbAnaIn_Grease PR311 L2 parametrit #####
    for i in range(6):
        
        Mittauspaikka = "fbAnaIn_Grease PR311 L2"
        tag = "Parameter"
        
        node_name_id = f'ns=3;s="{Mittauspaikka}"."stHMI"."stParameter"[{i}]."sName"'
        node_value_id = f'ns=3;s="{Mittauspaikka}"."stHMI"."stParameter"[{i}]."fValue"'
    
        Nimi = read_opcua_node(opcua_url, node_name_id)
        Arvo = read_opcua_node(opcua_url, node_value_id)
        
        if Nimi is not None and Arvo is not None:
            write_to_influxdb(Nimi, Arvo, Mittauspaikka, tag)
        else:
            logging.error(f"Failed to read data for node {i}")
    ##### Luetaan fbAnaIn_Grease PR311 L2 parametrit #####
    

    
    ##### Luetaan fbAnaIn_Grease PR311 L2 napit #####
    for i in range(4):

        Mittauspaikka = "fbAnaIn_Grease PR311 L2"
        tag = "Buttons"

        node_name_id = f'ns=3;s="{Mittauspaikka}"."stHMI"."stButtons"[{i}]."sName"'
        node_value_id = f'ns=3;s="{Mittauspaikka}"."stHMI"."stButtons"[{i}]."bStateMemory"'
    
        Nimi = read_opcua_node(opcua_url, node_name_id)
        Arvo = read_opcua_node(opcua_url, node_value_id)

        if Arvo == True:
            Arvo = int(1)
        else:
            Arvo = int(0)

        if Nimi is not None and Arvo is not None:
            write_to_influxdb(Nimi, Arvo, Mittauspaikka, tag)
        else:
            logging.error(f"Failed to read data for node {i}")
    ##### Luetaan fbAnaIn_Grease PR311 L2 napit #####
    

    
        ##### Luetaan fbAnaIn_Grease PR311 L2 Data #####
    for i in range(1):
        Mittauspaikka = "fbAnaIn_Grease PR311 L2"
        tag = "Data"

        
        node_name_id = f'ns=3;s="{Mittauspaikka}"."stHMI"."stData"[{i}]."eValueType"'
        node_value_id = f'ns=3;s="{Mittauspaikka}"."stHMI"."stData"[{i}]."fValue"'
    
        Nimi = read_opcua_node(opcua_url, node_name_id)
        Arvo = read_opcua_node(opcua_url, node_value_id)

        Nimi = HaeTyyppi(int(Nimi))


        if Nimi is not None and Arvo is not None:
            write_to_influxdb(Nimi, Arvo, Mittauspaikka, tag)
        else:
            logging.error(f"Failed to read data for node {i}")
    ##### Luetaan fbAnaIn_Grease PR311 L2 Data #####
"""