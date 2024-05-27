import asyncio
from asyncua import Client, ua
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
import json

# Ladataan JSON-tiedosto
with open('Nodet.json', 'r') as file:
    data = json.load(file)

# InfluxDB yhteystiedot
token = "E9pLhUMQQqpfNJQuHx8scPF7tjLaIGUbQHvIigrq92OBJ7AGjRVT6hy4NqN9UVcfWTfI1G0CjDlz_kddJ4E52w=="
org = "Burpellet"
bucket = "NewDataCollection"
url = "http://10.10.10.10:8086"  # Korvaa oikealla URL-osoitteella

# OPC UA -asiakasasetukset
opc_ua_url = "opc.tcp://10.10.10.1:4840"

class DataChangeHandler:
    def __init__(self, write_api, measurement_name, tag_values):
        self.write_api = write_api
        self.measurement_name = measurement_name
        self.tag_values = tag_values

    def datachange_notification(self, node, val, data):
        # Luo InfluxDB-piste
        point = Point(self.measurement_name).field("value", val)
        
        # Lisää tagit pisteeseen
        tags = ["tag1", "tag2", "tag3", "tag4", "tag5", "tag6", "tag7", "tag8", "tag9", "tag10", "tag11", "tag12"]
        for i, tag in enumerate(tags):
            if i < len(self.tag_values):
                point.tag(tag, self.tag_values[i])

        # Kirjoita data InfluxDB:hen
        self.write_api.write(bucket=bucket, org=org, record=point)
        print(f"Written to InfluxDB: {self.measurement_name} value={val}")

async def main():
    # InfluxDB client
    influx_client = InfluxDBClient(url=url, token=token, org=org)
    write_api = influx_client.write_api(write_options=SYNCHRONOUS)
    
    # OPC UA client
    async with Client(url=opc_ua_url) as client:
        subscription_interval = 500  # Muuta tätä arvoa tarvittaessa
        nodes_per_subscription = 5  # Määrä nodeja per tilaus
        
        # Käy läpi kaikki node tiedot JSON-tiedostosta
        node_groups = [data["DataNodet"][i:i + nodes_per_subscription] for i in range(0, len(data["DataNodet"]), nodes_per_subscription)]
        
        for group in node_groups:
            try:
                handler = DataChangeHandler(write_api, "MeasurementName", [])
                subscription = await client.create_subscription(subscription_interval, handler)
                for item in group:
                    # Erottimena '|'
                    parts = item.split('|')
                    value_node_id = parts[1]
                    measurement_name = parts[2]
                    tag_values = parts[3:]

                    # Luo nodelle subscription
                    node = client.get_node(value_node_id)
                    await subscription.subscribe_data_change(node)
                    print(f"Subscribed to node: {value_node_id}")
            except Exception as e:
                print(f"Error in subscription group: {e}")

        # Pidä yhteys auki
        while True:
            await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(main())
