import json
from opcua import Client, ua

def lue_opc_ua_node(client, node_id):
    try:
        node = client.get_node(node_id)
        value = node.get_value()
        return value
    except ua.UaStatusCodeError as e:
        print(f"Error reading node {node_id}: {e}")
        return None

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

# Lataa JSON-tiedosto
with open('Nodet.json', 'r', encoding='utf-8') as file:
    data = json.load(file)

# Alusta listat
Nimet = []
Arvot = []
Osa = []
Node = []
ErpNo = []
Tagit = []
Sarjanumero = []
Hys = []
Intervalli = []
Maa = []
KoneenTyyppi = []
TestiInfo = []
Projektinumero = []

# Käy läpi tiedot ja jaa listat
for item in data['DataNodet']:
    parts = item.split('|')
    Nimet.append(parts[0])
    Arvot.append(parts[1])
    Osa.append(parts[2])
    Node.append(parts[3])
    ErpNo.append(parts[4])
    Tagit.append(parts[5])
    Sarjanumero.append(parts[6])
    Hys.append(parts[7])
    Intervalli.append(parts[8])
    Maa.append(parts[9])
    KoneenTyyppi.append(parts[10])
    TestiInfo.append(parts[11])
    Projektinumero.append(parts[12])

server_url = "opc.tcp://10.10.10.1:4840"  # Korvaa oikealla palvelimen URL:llä

# Luo asiakas ja yhdistä OPC UA -palvelimeen
client = Client(server_url)
client.connect()

try:
    for i in range(len(Nimet)):
        NodeNimi = lue_opc_ua_node(client, Nimet[i])
        NodeArvo = lue_opc_ua_node(client, Arvot[i])
        if NodeNimi is not None:
            NodeNimi = HaeTyyppi(NodeNimi)
        else:
            NodeNimi = "Unknown"

        if NodeArvo == False: 
            NodeArvo = 0
        elif NodeArvo == True:
            NodeArvo = 1

        if NodeArvo is None:
            NodeArvo = "Unknown"

        print(f"{NodeNimi} = {NodeArvo}")
finally:
    # Katkaise yhteys
    client.disconnect()

