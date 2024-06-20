import pandas as pd
from datetime import datetime, timedelta
from influxdb_client import InfluxDBClient

# InfluxDB asetukset
token = "E9pLhUMQQqpfNJQuHx8scPF7tjLaIGUbQHvIigrq92OBJ7AGjRVT6hy4NqN9UVcfWTfI1G0CjDlz_kddJ4E52w=="
org = "Burpellet"
bucket = "DataCollection"
url = "http://10.10.10.10:8086"

client = InfluxDBClient(url=url, token=token, org=org)

def get_runtime_hours():
    query = f'''
    from(bucket: "{bucket}")
      |> range(start: -1w)
      |> filter(fn: (r) => r._measurement == "Data" and r._field == "Current" and (r.Part == "Feed Screw" or r.Part == "Lower Press Roll" or r.Part == "Upper Press Roll"))
    '''
    query_api = client.query_api()
    result = query_api.query(org=org, query=query)
    
    data = []
    for table in result:
        for record in table.records:
            data.append((record.get_time(), record.get_measurement(), record.get_field(), record.get_value(), record.values["Part"]))
    
    df = pd.DataFrame(data, columns=['_time', '_measurement', '_field', '_value', 'Part'])
    
    # Suodatetaan viimeisimmän viikon data
    end_time = df['_time'].max()
    start_time = end_time - timedelta(days=7)
    recent_week_df = df[(df['_time'] >= start_time) & (df['_time'] <= end_time)]
    
    # Suodatetaan data halutun ehdon perusteella
    filtered_df = recent_week_df[(recent_week_df['_measurement'] == 'Data') & 
                                 (recent_week_df['_field'] == 'Current') & 
                                 (recent_week_df['Part'].isin(['Feed Screw', 'Lower Press Roll', 'Upper Press Roll'])) & 
                                 (recent_week_df['_value'] > 1)]
    
    # Lasketaan ajoaika
    if not filtered_df.empty:
        # Lasketaan aikaväli, jolloin kaikki ehdot täyttyvät
        filtered_df['_time'] = pd.to_datetime(filtered_df['_time'])
        runtime_minutes = filtered_df['_time'].diff().dropna().apply(lambda x: x.total_seconds() / 60).sum()
        runtime_hours = runtime_minutes / 60
    else:
        runtime_hours = 0
    
    return runtime_hours

# Esimerkki funktion käytöstä
if __name__ == '__main__':
    runtime_hours = get_runtime_hours()/5
    print(f'Koneen ajoaika viimeisimmän viikon aikana, päivässä Keskimäärin: {round(runtime_hours),1} tuntia')
