from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS
import pandas as pd
import matplotlib.pyplot as plt

# InfluxDB:n yhteystiedot
token = "E9pLhUMQQqpfNJQuHx8scPF7tjLaIGUbQHvIigrq92OBJ7AGjRVT6hy4NqN9UVcfWTfI1G0CjDlz_kddJ4E52w=="
org = "Burpellet"
bucket = "DataCollection"
url = "http://10.10.10.10:8086"

# Luo asiakas
client = InfluxDBClient(url=url, token=token, org=org)

# Aikaväli ja kysely
start_time = "2024-06-13T10:50:00Z"
stop_time = "2024-06-13T11:10:00Z"
query = f'''
from(bucket: "{bucket}")
  |> range(start: {start_time}, stop: {stop_time})
  |> filter(fn: (r) => r["_measurement"] == "Data")
  |> filter(fn: (r) => r["SerialNumber"] == "202" or r["SerialNumper"] == "202")
  |> filter(fn: (r) => r["_field"] == "Power")
  |> group(columns: ["Part"])
  |> yield(name: "grouped_data")
'''

# Hae data
result = client.query_api().query(org=org, query=query)

# Muunna data DataFrameksi
values = []
for table in result:
    for record in table.records:
        values.append((record.get_time(), record.get_value(), record.values["Part"]))

df = pd.DataFrame(values, columns=["time", "Power", "Part"])

# Laske jokaiselle laitteelle keskiarvo, minimi, maksimi ja kokonaiskulutus
grouped = df.groupby("Part")
results = []

for name, group in grouped:
    average_power = group["Power"].mean()
    min_power = group["Power"].min()
    max_power = group["Power"].max()
    total_consumption = group["Power"].sum() * (10 / 60)  # Oletetaan, että data on 10 minuutin ajalta ja Power on kW

    results.append({
        "Part": name,
        "Keskiarvo (kW)": average_power,
        "Minimi (kW)": min_power,
        "Maksimi (kW)": max_power,
        "Kokonaiskulutus (kWh)": total_consumption
    })

# Tulosta tulokset
result_df = pd.DataFrame(results)
print(result_df)

# Piirretään graafit
fig, axs = plt.subplots(2, 2, figsize=(15, 10))

# Keskiarvo
axs[0, 0].bar(result_df["Part"], result_df["Keskiarvo (kW)"], color='b')
axs[0, 0].set_title('Average (kW)')
axs[0, 0].set_xlabel('Part')
axs[0, 0].set_ylabel('kW')
axs[0, 0].tick_params(axis='x', rotation=45)

# Minimi
axs[0, 1].bar(result_df["Part"], result_df["Minimi (kW)"], color='g')
axs[0, 1].set_title('Min (kW)')
axs[0, 1].set_xlabel('Part')
axs[0, 1].set_ylabel('kW')
axs[0, 1].tick_params(axis='x', rotation=45)

# Maksimi
axs[1, 0].bar(result_df["Part"], result_df["Maksimi (kW)"], color='r')
axs[1, 0].set_title('Max (kW)')
axs[1, 0].set_xlabel('Part')
axs[1, 0].set_ylabel('kW')
axs[1, 0].tick_params(axis='x', rotation=45)

# Kokonaiskulutus
axs[1, 1].bar(result_df["Part"], result_df["Kokonaiskulutus (kWh)"], color='c')
axs[1, 1].set_title('Total consumption (kWh)')
axs[1, 1].set_xlabel('Part')
axs[1, 1].set_ylabel('kWh')
axs[1, 1].tick_params(axis='x', rotation=45)

plt.tight_layout()
plt.show()

# Suljetaan asiakas
client.close()

