from influxdb_client import InfluxDBClient
import pandas as pd
import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.express as px
import matplotlib.pyplot as plt

# Asetukset
token = "E9pLhUMQQqpfNJQuHx8scPF7tjLaIGUbQHvIigrq92OBJ7AGjRVT6hy4NqN9UVcfWTfI1G0CjDlz_kddJ4E52w=="
org = "Burpellet"
bucket = "DataCollection"
url = "http://10.10.10.10:8086"

client = InfluxDBClient(url=url, token=token, org=org)

# Funktio kyselyille ja datan yhdistämiselle
def query_data(bucket, org, field, serial_numbers, measurement="Data"):
    query = f'''
    from(bucket: "{bucket}") 
    |> range(start: -2h) 
    |> filter(fn: (r) => r["_measurement"] == "{measurement}") 
    |> filter(fn: (r) => r["_field"] == "{field}")
    |> filter(fn: (r) => r["SerialNumper"] == "203" or r["SerialNumper"] == "202")
    |> pivot(rowKey:["_time"], columnKey: ["SerialNumper"], valueColumn: "_value")
    '''
    result = client.query_api().query_data_frame(org=org, query=query)
    if result:
        df = pd.concat(result)
        df.set_index("_time", inplace=True)
        # Poistetaan ylimääräiset sarakkeet
        df = df[['202', '203']]
        return df
    else:
        raise ValueError(f"Kysely ei palauttanut dataa kentälle {field}.")

# Kyselyt ja datan yhdistäminen
def load_and_combine_data():
    position_data = query_data(bucket, org, "Position", ["203", "202"])
    current_data = query_data(bucket, org, "Current", ["203", "202"])
    torque_data = query_data(bucket, org, "Torque_%_nom", ["203", "202"])

    # Poistetaan duplikaatti-indeksit
    position_data = position_data[~position_data.index.duplicated(keep='first')]
    current_data = current_data[~current_data.index.duplicated(keep='first')]
    torque_data = torque_data[~torque_data.index.duplicated(keep='first')]

    # Yhdistetään data yhteisten aikaleimojen avulla
    combined_data = position_data.add_suffix('_Position').join(current_data.add_suffix('_Current'), how='outer').join(torque_data.add_suffix('_Torque'), how='outer')

    return combined_data

combined_data = load_and_combine_data()

# Tulostetaan sarakenimet diagnostiikkaa varten
print("Combined Data Columns:", combined_data.columns)

# Diagnostiikkatulostus: tulostetaan muutama rivi yhdistetystä datasta
print("Combined Data Head:")
print(combined_data.head(20))

# Matplotlib-grafiikat diagnostiseen visualisointiin
plt.figure(figsize=(10, 5))
plt.plot(combined_data.index, combined_data['203_Current'], label='Kone 203 - Virta')
plt.plot(combined_data.index, combined_data['202_Current'], label='Kone 202 - Virta')
plt.xlabel('Aika')
plt.ylabel('Virta')
plt.title('Virta kahdessa koneessa')
plt.legend()
plt.show()

plt.figure(figsize=(10, 5))
plt.plot(combined_data.index, combined_data['203_Torque'], label='Kone 203 - Vääntö')
plt.plot(combined_data.index, combined_data['202_Torque'], label='Kone 202 - Vääntö')
plt.xlabel('Aika')
plt.ylabel('Vääntö')
plt.title('Vääntö kahdessa koneessa')
plt.legend()
plt.show()

# Dash-sovellus
app = dash.Dash(__name__)

app.layout = html.Div([
    html.H1("Koneiden mittausten vertailu"),
    dcc.Tabs([
        dcc.Tab(label='Nipin korkeus', children=[
            dcc.Graph(id='nipin-korkeus-graph')
        ]),
        dcc.Tab(label='Virta', children=[
            dcc.Graph(id='virta-graph')
        ]),
        dcc.Tab(label='Vääntö', children=[
            dcc.Graph(id='vaanto-graph')
        ])
    ])
])

@app.callback(
    Output('nipin-korkeus-graph', 'figure'),
    Input('nipin-korkeus-graph', 'id')
)
def update_nipin_korkeus_graph(_):
    data_long = combined_data.reset_index().melt(id_vars=["_time"], value_vars=["203_Position", "202_Position"], 
                                                 var_name="Kone", value_name="Nipin korkeus")
    fig = px.line(data_long, x='_time', y='Nipin korkeus', color='Kone', title='Nipin korkeus kahdessa koneessa')
    return fig

@app.callback(
    Output('virta-graph', 'figure'),
    Input('virta-graph', 'id')
)
def update_virta_graph(_):
    data_long_current = combined_data.reset_index().melt(id_vars=["_time"], value_vars=["203_Current", "202_Current"], 
                                                         var_name="Kone", value_name="Virta")
    fig = px.line(data_long_current, x='_time', y='Virta', color='Kone', title='Virta kahdessa koneessa')
    return fig

@app.callback(
    Output('vaanto-graph', 'figure'),
    Input('vaanto-graph', 'id')
)
def update_vaanto_graph(_):
    data_long_torque = combined_data.reset_index().melt(id_vars=["_time"], value_vars=["203_Torque", "202_Torque"], 
                                                        var_name="Kone", value_name="Vääntö")
    fig = px.line(data_long_torque, x='_time', y='Vääntö', color='Kone', title='Vääntö kahdessa koneessa')
    return fig

if __name__ == '__main__':
    app.run_server(debug=True)
