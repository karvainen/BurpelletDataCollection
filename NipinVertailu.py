from influxdb_client import InfluxDBClient
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px

# Asetukset
token = "E9pLhUMQQqpfNJQuHx8scPF7tjLaIGUbQHvIigrq92OBJ7AGjRVT6hy4NqN9UVcfWTfI1G0CjDlz_kddJ4E52w=="
org = "Burpellet"
bucket = "DataCollection"
url = "http://10.10.10.10:8086"

client = InfluxDBClient(url=url, token=token, org=org)

# Kyselyt kahden koneen nipin korkeudesta
query = f'''
from(bucket: "{bucket}") 
|> range(start: -12h) 
|> filter(fn: (r) => r["_measurement"] == "Data") 
|> filter(fn: (r) => r["_field"] == "Position")
|> filter(fn: (r) => r["SerialNumper"] == "203" or r["SerialNumper"] == "202")
|> pivot(rowKey:["_time"], columnKey: ["SerialNumper"], valueColumn: "_value")
'''

# Suoritetaan kysely
result = client.query_api().query_data_frame(org=org, query=query)

# Yhdistetään kaikki DataFrame-objektit yhdeksi DataFrameksi
if result:
    df = pd.concat(result)
else:
    raise ValueError("Kysely ei palauttanut dataa.")

# Tulostetaan tuloksen ensimmäiset rivit
print(df.head())

# Tarkistetaan sarakenimet
print(df.columns)

# Valitaan vain relevantit sarakkeet
if '203' in df.columns and '202' in df.columns:
    data = df[["_time", "203", "202"]].rename(columns={"203": "nipin_korkeus_kone203", "202": "nipin_korkeus_kone202"})
else:
    raise KeyError("Expected columns '203' and '202' not found in the result")

# Asetetaan aikaindeksi
data.set_index("_time", inplace=True)

# Visualisointi Matplotlibilla
plt.figure(figsize=(10, 5))
plt.plot(data.index, data['nipin_korkeus_kone203'], label='Kone 203')
plt.plot(data.index, data['nipin_korkeus_kone202'], label='Kone 202')
plt.xlabel('Aika')
plt.ylabel('Nipin korkeus')
plt.title('Nipin korkeus kahdessa koneessa')
plt.legend()
plt.show()

# Visualisointi Seabornilla
data_long = data.reset_index().melt(id_vars=["_time"], value_vars=["nipin_korkeus_kone203", "nipin_korkeus_kone202"], 
                                    var_name="Kone", value_name="Nipin korkeus")

plt.figure(figsize=(10, 5))
sns.violinplot(x="Kone", y="Nipin korkeus", data=data_long)
plt.title('Nipin korkeuden jakautuminen')
plt.show()

# Visualisointi Plotlylla
fig = px.line(data_long, x='_time', y='Nipin korkeus', color='Kone', title='Nipin korkeus kahdessa koneessa')
fig.show()

# Keskiarvon ja varianssin vertailu
mean_203 = data['nipin_korkeus_kone203'].mean()
mean_202 = data['nipin_korkeus_kone202'].mean()
var_203 = data['nipin_korkeus_kone203'].var()
var_202 = data['nipin_korkeus_kone202'].var()

print(f"Kone 203 - Keskiarvo: {mean_203}, Varianssi: {var_203}")
print(f"Kone 202 - Keskiarvo: {mean_202}, Varianssi: {var_202}")

# Korrelaatioanalyysi
correlation = data['nipin_korkeus_kone203'].corr(data['nipin_korkeus_kone202'])
print(f"Korrelaatio kahden koneen välillä: {correlation}")
