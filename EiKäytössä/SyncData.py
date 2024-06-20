######################################################################################################################################
#####                                                                                                                            #####
##### Laboratorion koneista kerätään dataa ja tallennetaan se excel-tiedostoon.                                                  #####
#####                                                                                                                            #####
##### Ohjelma kysyy käyttäjältä kuinka kauan dataa kerätään minuutteina ja aloittaa keräyksen.                                   #####
##### Dataa kerätään valitun ajan verran ja tallennetaan se excel-tiedostoon.                                                    #####
#####                                                                                                                            #####
##### Ohjelma käyttää OPC UA -protokollaa ja yhdistää siihen laboratorion koneen.                                                #####
#####                                                                                                                            #####
##### Ohjelma käyttää kirjastoja:                                                                                                #####
##### - pandas                                                                                                                   #####
##### - opcua                                                                                                                    #####
##### - datetime                                                                                                                 #####
##### - time                                                                                                                     #####
##### - concurrent.futures                                                                                                       #####
#####                                                                                                                            #####
##### Ohjelman on kirjoittanut: Juha Pihlajamäki Saalasti FINLAND Oy                                                             #####
#####                                                                                                                            #####
######################################################################################################################################





from opcua import Client
from datetime import datetime
import time
import pandas as pd
import concurrent.futures
import json
import os

# Nykyinen skriptin hakemisto
current_directory = os.path.dirname(os.path.abspath(__file__))
file_path = os.path.join(current_directory, 'DataNodet.json')

# Lue tiedot DataNodet.json tiedostosta
try:
    with open(file_path, 'r') as file:
        data = json.load(file)

    # Alustetaan tyhjät listat
    Nimi = []
    Node = []
    NodeName = []
    Erp = []
    Tagi = []
    Sarjanumero = []
    Hystereesi = []
    Tosi = []
    Maa = []
    Malli = []
    Test = []
    ProjektiNumero = []

    # Muunnos listaksi
    for item in data["DataNodet"]:
        fields = item.split('|')
        Nimi.append(fields[0])
        Node.append(fields[1])
        NodeName.append(fields[2])
        Erp.append(fields[3])
        Tagi.append(fields[4])
        Sarjanumero.append(fields[5])
        Hystereesi.append(fields[6])
        Tosi.append(fields[7])
        Maa.append(fields[8])
        Malli.append(fields[9])
        Test.append(fields[10])
        ProjektiNumero.append(fields[11])

    # Tulostetaan tulokset
    for i in range(len(Nimi)):
        print("Nimi: ", Nimi[i])
        print("Node: ", Node[i])
        print("NodeName: ", NodeName[i])
        print("Erp: ", Erp[i])
        print("Tagi: ", Tagi[i])
        print("Sarjanumero: ", Sarjanumero[i])
        print("Hystereesi: ", Hystereesi[i])
        print("Tosi: ", Tosi[i])
        print("Maa: ", Maa[i])
        print("Malli: ", Malli[i])
        print("Test: ", Test[i])
        print("ProjektiNumero: ", ProjektiNumero[i])
        print("\n")

    

except FileNotFoundError:
    print(f"Tiedostoa {file_path} ei löytynyt.")















# OPC UA -asiakas
plc_url = "opc.tcp://10.10.10.1:4840"
client = Client(plc_url)
client.connect()  # Connect to the client

def read_node(address):
    node = client.get_node(address)
    value = node.get_value()
    return value





dataa = []
aikaleima = []

aika = 60 * int(input("Kuinka kauan dataa kerätään minuutteina: "))
print("Kerätään dataa", aika / 60, "minuuttia")
print("Aloitetaan keräys")
print("Kerätään dataa, tallennus loppuu automaattisesti kun tallennus on ohi")

for i in range(aika):
    current_second = datetime.now().second
    with concurrent.futures.ThreadPoolExecutor() as executor:
        future_to_address = {executor.submit(read_node, address): address for address in Node}
        values = [future.result() for future in concurrent.futures.as_completed(future_to_address)]

    while datetime.now().second == current_second:
        time.sleep(0)

    print("Kerätään dataa", i, "/", aika, "sekuntia")
    dataa.append(values)
    aikaleima.append(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

df = pd.DataFrame(dataa, columns=Nimi)
df['Aikaleima'] = aikaleima
df.to_excel("LabraData_" + datetime.now().strftime("%Y-%m-%d") + ".xlsx", index=False)

print("Tallennus valmis")
client.disconnect()  # Disconnect from the client