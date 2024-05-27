import asyncio
from asyncua import Client, ua
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
import json

# Ladataan JSON-tiedosto
try:
    with open('Nodet.json', 'r') as file:
        data = json.load(file)
    if "DataNodet" not in data:
        raise KeyError("JSON-tiedostosta puuttuu 'DataNodet'-avain")
except FileNotFoundError:
    raise FileNotFoundError("JSON-tiedostoa 'Nodet.json' ei löydy")
except json.JSONDecodeError:
    raise ValueError("JSON-tiedosto 'Nodet.json' on virheellinen")

# InfluxDB yhteystiedot
token = "E9pLhUMQQqpfNJQuHx8scPF7tjLaIGUbQHvIigrq92OBJ7AGjRVT6hy4NqN9UVcfWTfI1G0CjDlz_kddJ4E52w=="
org = "Burpellet"
bucket = "NewDataCollection"
url = "http://10.10.10.10:8086"  # Korvaa oikealla URL-osoitteella

# OPC UA -asiakasasetukset
opc_ua_url = "opc.tcp://10.10.10.1:4840"

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

class DataChangeHandler:
    def __init__(self, write_api, measurement_name, tag_values):
        self.write_api = write_api
        self.measurement_name = measurement_name
        self.tag_values = tag_values

    def datachange_notification(self, node, val, data):
        # Tarkista, onko measurement_name enum-arvo, ja hae nimi tarvittaessa
        try:
            mittauksen_nimi = HaeTyyppi(int(self.measurement_name))
        except ValueError:
            mittauksen_nimi = self.measurement_name

        # Luo InfluxDB-piste
        point = Point(self.measurement_name).field("value", val)
        
        # Lisää tagit pisteeseen
        tags = ["Node", "ErpCode", "sTag", "MachineSN", "Mittauksen nimi", "dLogIntervalHysteresis", "Contry", "MachineType", "TestInfo", "ProjectNo", "tag11", "tag12"]
        for i, tag in enumerate(tags[:len(self.tag_values)]):  # Varmistaa, että ei yritetä käyttää tagia, jota ei ole määritetty
            if tag == "Mittauksen nimi":
                point.tag(tag, mittauksen_nimi)
            else:
                point.tag(tag, self.tag_values[i])

        # Kirjoita data InfluxDB:hen
        self.write_api.write(bucket=bucket, org=org, record=point)
        print(f"Written to InfluxDB: {self.measurement_name} value={val}, Mittauksen nimi={mittauksen_nimi}")

async def main():
    # InfluxDB client
    influx_client = InfluxDBClient(url=url, token=token, org=org)
    write_api = influx_client.write_api(write_options=SYNCHRONOUS)
    
    # OPC UA client
    async with Client(url=opc_ua_url) as client:
        # Käy läpi kaikki node tiedot JSON-tiedostosta
        for item in data["DataNodet"]:
            try:
                # Erottimena '|'
                parts = item.split('|')
                value_node_id = parts[1]
                measurement_name = parts[2]
                tag_values = parts[3:]

                # Luo nodelle subscription
                node = client.get_node(value_node_id)
                handler = DataChangeHandler(write_api, measurement_name, tag_values)
                subscription = await client.create_subscription(500, handler)
                await subscription.subscribe_data_change(node)
                print(f"Subscribed to node: {value_node_id} for measurement: {measurement_name}")

            except Exception as e:
                print(f"Error subscribing to node: {item}, Error: {e}")

        # Pidä yhteys auki
        while True:
            await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(main())
