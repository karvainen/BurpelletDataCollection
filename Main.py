import asyncio
from asyncua import Client, Node, ua
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

async def main():
    # InfluxDB client
    influx_client = InfluxDBClient(url=url, token=token, org=org)
    write_api = influx_client.write_api(write_options=SYNCHRONOUS)
    
    # OPC UA client
    async with Client(url=opc_ua_url) as client:
        # Käy läpi kaikki node tiedot JSON-tiedostosta
        for item in data["DataNodet"]:
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

        # Pidä yhteys auki
        while True:
            await asyncio.sleep(1)

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
            point.tag(tag, self.tag_values[i])

        # Kirjoita data InfluxDB:hen
        self.write_api.write(bucket=bucket, org=org, record=point)
        print(f"Written to InfluxDB: {self.measurement_name} value={val}")

if __name__ == "__main__":
    asyncio.run(main())
