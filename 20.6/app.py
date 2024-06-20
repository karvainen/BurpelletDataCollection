from flask import Flask, render_template_string
from influxdb_client import InfluxDBClient
import plotly.graph_objs as go
import plotly.io as pio

app = Flask(__name__)

# Asetukset
token = "E9pLhUMQQqpfNJQuHx8scPF7tjLaIGUbQHvIigrq92OBJ7AGjRVT6hy4NqN9UVcfWTfI1G0CjDlz_kddJ4E52w=="
org = "Burpellet"
bucket = "DataCollection"
url = "http://10.10.10.10:8086"

client = InfluxDBClient(url=url, token=token)
query_api = client.query_api()

@app.route('/')
def index():
    query = '''
    from(bucket: "{}")
      |> range(start: -12h)
      |> filter(fn: (r) => r._measurement == "Data" and r._field == "Position")
      |> filter(fn: (r) => r.SerialNumber == "202" or r.SerialNumber == "203")
    '''.format(bucket)
    
    result = query_api.query(org=org, query=query)
    
    times_202 = []
    values_202 = []
    times_203 = []
    values_203 = []
    for table in result:
        for record in table.records:
            if record["SerialNumber"] == "202":
                times_202.append(record.get_time())
                values_202.append(record.get_value())
            elif record["SerialNumber"] == "203":
                times_203.append(record.get_time())
                values_203.append(record.get_value())
    
    # Plotly graafinen esitys
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=times_202, y=values_202, mode='lines', name='202 SerialNumber'))
    fig.add_trace(go.Scatter(x=times_203, y=values_203, mode='lines', name='203 SerialNumber'))
    fig.update_layout(title='Nipin korkeus', xaxis_title='Time', yaxis_title='Nipin korkeus')
    graph_html = pio.to_html(fig, full_html=False)
    
    return render_template_string('''
    <!DOCTYPE html>
    <html lang="fi">
    <head>
        <meta charset="UTF-8">
        <title>Nipin korkeus, virta ja momentti</title>
        <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
    </head>
    <body>
        <h1>Nipin koko ja virrat</h1>
        <div>{{ graph_html|safe }}</div>
    </body>
    </html>
    ''', graph_html=graph_html)

if __name__ == '__main__':
    app.run(debug=True)
