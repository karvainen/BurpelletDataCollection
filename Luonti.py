#################################################################################
##### FirstFile.py                                                          #####
#####                                                                       #####
##### Lukee ensimmäisellä käynnistyskerralla konekohtaiset tiedot           #####
##### Tekee niistä pythonille ST_ProjectData.py tiedoston                   #####
#####                                                                       #####
##### Ei vielä valmis.                                                      #####
#####                                                                       #####
##### @Juha Pihlajamäki Saalasti FINLAND Oy                                 #####
#################################################################################

from opcua import Client, ua
import config
from influxdb_client import InfluxDBClient, Point, WriteOptions
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
import asyncio
import json
def LueData(client, paikka, arvo):  
    try:
        # Muodosta node ID:t
        node_id_eValueType = f'ns=3;s="{paikka}."stHMI"."stData"[{arvo}]."eValueType"'
        node_id_fValue = f'ns=3;s="{paikka}."stHMI"."stData"{arvo}."fValue"'
        
        # Hae node ja sen arvot
        node_eValueType = client.get_node(node_id_eValueType)
        #value_eValueType = node_eValueType.get_value()
        
        node_fValue = client.get_node(node_id_fValue)
        #value_fValue = node_fValue.get_value()

        node_fValue = HaeTyyppi(node_fValue)
        print(f"Node: {node_fValue}")
        print(f"Value: {value_fValue}")

        return value_eValueType, value_fValue
    except ua.UaStatusCodeError as e:
        print(f"Error reading node {paikka}: {e}")
        return None, None
    
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

DataNodet = []
ButtonNodet = []
ParmetriNodet = []
DataKpl = []
ParametritKpl = []
NapitKpl = []
Paikka = []
Nimet = []
Erppi = []
Tagit = []
Sarjanumero = []
Hys = []
intervalli = []
Maa = []
KoneenTyyppi = []
TestiInfo = []
ProjektiNumero = []
###### Tässä ei ole kaikki, lisää puuttuvat ######
Nodet = []
Nodet.append("fbAnaIn_Grease PR311 L2")
Nodet.append("fbAnaIn_Pressing nip PR311")
Nodet.append("fbDirectDrive_Hyd_Motor")
Nodet.append("fbDirectDrive_HydCooler")
Nodet.append("fbDirectDrive_HydHeating")
Nodet.append("fbValve_301_Press")
Nodet.append("fbValve_301_Lift")
Nodet.append("fbValve_311_Press")
Nodet.append("fbValve_311_Lift")
Nodet.append("fbValve_Lube_Pump")
Nodet.append("fbValve_Lube_Line_1")
Nodet.append("fbValve_Lube_Line_2")
Nodet.append("fbValve_Lube_A")
Nodet.append("fbValve_Lube_B")
Nodet.append("fbValve_PR301 floating piston")
Nodet.append("fbValve_PR301 floating safety")
Nodet.append("fbValve_PR311 floating piston")
Nodet.append("fbValve_PR311 floating shaft safety")
Nodet.append("fbDistributionscrew_SCR200")
Nodet.append("fbLowerPressRoll_PR301")
Nodet.append("fbUpperPressRoll_PR301")
Nodet.append("fbAuxiliarydrive_PR301")
Nodet.append("fbAirBlower_PR301")
Nodet.append("fbEffluentfilterscraper_SCA302")
Nodet.append("fbDischargeScrew_SCR303")
Nodet.append("fbFeedScrew_SCR310")
Nodet.append("fbLowerPressRoll_PR311")
Nodet.append("fbUpperPressRoll_PR311")
Nodet.append("fbAuxiliarydrive_PR311")
Nodet.append("fbAirBlower_PR311")
Nodet.append("fbEffluentfilterscraper_SCA312")
Nodet.append("fbDischargeScrew_SCR313")
Nodet.append("fbSpillageliftingscraperconveyor_SCA501")
Nodet.append("fbSpillagescraperconveyor_SCA500")
Nodet.append("fbAnaIn_Oil temperature")
Nodet.append("fbAnaIn_Oil level")
Nodet.append("fbAnaIn_Pressure line PR311")
Nodet.append("fbAnaIn_Pressure line PR301")
Nodet.append("fbAnaIn_Grease PR301 L1")
Nodet.append("fbAnaIn_Grease PR301 L2")

# Luo OPC UA -asiakasobjekti ja määritä palvelimen URL
url = config.opcua_url
client = Client(url)

try:
    # Yritä yhdistää palvelimeen
    client.connect()
    print("Client connected")

    

    
    ############################      Käydään läpi kaikki mittauspaikat ja haetaan niiden tiedot     ############################
    for index, Mittauspaikka in enumerate(Nodet):
        sERP_Code = client.get_node(f'ns=3;s="{Mittauspaikka}"."stHMI"."stBaseData"."sERP_Code"').get_value()
        sName = client.get_node(f'ns=3;s="{Mittauspaikka}"."stHMI"."stBaseData"."sName"').get_value()
        sTag = client.get_node(f'ns=3;s="{Mittauspaikka}"."stHMI"."stBaseData"."sTag"').get_value()
        dMachineSerialNumber = client.get_node(f'ns=3;s="{Mittauspaikka}"."stHMI"."stBaseData"."dMachineSerialNumber"').get_value()
        dLogIntervalHysteresis = client.get_node(f'ns=3;s="{Mittauspaikka}"."stHMI"."stBaseData"."dLogIntervalHysteresis"').get_value()
        nNumberOfValues = client.get_node(f'ns=3;s="{Mittauspaikka}"."stHMI"."stBaseData"."nNumberOfValues"').get_value()
        nNumberOfButtons = client.get_node(f'ns=3;s="{Mittauspaikka}"."stHMI"."stBaseData"."nNumberOfButtons"').get_value()
        nNumberOfParameters = client.get_node(f'ns=3;s="{Mittauspaikka}"."stHMI"."stBaseData"."nNumberOfParameters"').get_value()
        bLogByIntervalWhenTRUE = client.get_node(f'ns=3;s="{Mittauspaikka}"."stHMI"."stBaseData"."bLogByIntervalWhenTRUE"').get_value()

        sCountry = client.get_node(f'ns=3;s="dbCommonData"."stHMI"."stProjectData"."sCountry"').get_value()
        sMachineType = client.get_node(f'ns=3;s="dbCommonData"."stHMI"."stProjectData"."sMachineType"').get_value()
        sTestingInfo = client.get_node(f'ns=3;s="dbCommonData"."stHMI"."stProjectData"."sTestingInfo"').get_value()
        dProjectNumber = client.get_node(f'ns=3;s="dbCommonData"."stHMI"."stProjectData"."dProjectNumber"').get_value()




        """sMachineType = client.get_node(f'ns=3;s="{Mittauspaikka}"."stHMI"."stProjectData"."sMachineType"').get_value()
        sTestingInfo = client.get_node(f'ns=3;s="{Mittauspaikka}"."stHMI"."stProjectData"."sTestingInfo"').get_value()
        dProjecNumber = client.get_node(f'ns=3;s="{Mittauspaikka}"."stHMI"."stProjectData"."dProjecNumber"').get_value()"""

    ############################      Käydään läpi kaikki mittauspaikat ja haetaan niiden tiedot     ############################
        





        #################################     Tarkistetaan, että kaikki arvot on olemassa     #################################
        if Mittauspaikka == None:
            Mittauspaikka = "Arvoa ei saatu luettua"
        
        if sERP_Code == None:
            sERP_Code = "Ei ERP koodia"

        if sName == None:
            sName = "Ei nimeä"

        if sTag == None:
            sTag = "Ei tagia"

        if dMachineSerialNumber == None:
            dMachineSerialNumber = "Ei sarjanumeroa"

        if dLogIntervalHysteresis == None:
            dLogIntervalHysteresis = "Ei hystereesi arvoa"

        if nNumberOfValues == None:
            nNumberOfValues = 0

        if nNumberOfButtons == None:
            nNumberOfButtons = 0

        if nNumberOfParameters == None:
            nNumberOfParameters = 0
        
        if bLogByIntervalWhenTRUE == None:
            bLogByIntervalWhenTRUE = "Ei arvoa"

        if sCountry == None:
            sCountry = "Ei maata"

        if sMachineType == None:
            sMachineType = "Ei konetyyppiä"

        if sTestingInfo == None:
            sTestingInfo = "Ei testitietoa"
        
        if dProjectNumber == None:
            dProjectNumber = 0


        #################################     Tarkistetaan, että kaikki arvot on olemassa     #################################






        
        #################################     Lisätään arvot listoihin                        #################################
        Paikka.append(Mittauspaikka)
        Nimet.append(sName)
        DataKpl.append(nNumberOfValues)
        NapitKpl.append(nNumberOfButtons)
        ParametritKpl.append(nNumberOfParameters)
        Erppi.append(sERP_Code)
        Tagit.append(sTag)
        Sarjanumero.append(dMachineSerialNumber)
        Hys.append(dLogIntervalHysteresis)
        intervalli.append(bLogByIntervalWhenTRUE)
        Maa.append(sCountry)
        KoneenTyyppi.append(sMachineType)
        TestiInfo.append(sTestingInfo)
        ProjektiNumero.append(dProjectNumber)

        #################################     Lisätään arvot listoihin                        #################################

    
    
    for i in range(len(Paikka)):
    
        
            # Jos nodella on dataa niin tehdään siihen valmiit nodet
        for j in range(DataKpl[i]):
            try:
                HaettuNimi = client.get_node(f'ns=3;s="{Paikka[i]}"."stHMI"."stData"[{j}]."eValueType"').get_value()
                HaettuNimi = HaeTyyppi(HaettuNimi)
                print(f"Haettu nimi: {HaettuNimi}")
                DataNodet.append(f'{HaettuNimi}|ns=3;s="{Paikka[i]}"."stHMI"."stData"[{j}]."fValue"|{Nimet[i]}|{Paikka[i]}|{Erppi[i]}|{Tagit[i]}|{Sarjanumero[i]}|{Hys[i]}|{intervalli[i]}|{Maa[i]}|{KoneenTyyppi[i]}|{TestiInfo[i]}|{ProjektiNumero[i]}')
            except Exception as e:
                print(f"Paikkaa {Paikka[i]} ei pystytty lukemaan, numero j = {j}, numero i = {i}")



        j = 0
        for j in range(NapitKpl[i]):
                    try:
                        HaettuNimi = client.get_node(f'ns=3;s="{Paikka[i]}"."stHMI"."stButtons"[{j}]."sName"').get_value()
                        ButtonNodet.append(f'{HaettuNimi}|ns=3;s="{Paikka[i]}"."stHMI"."stButtons"[{j}]."bStateMemory"|{Nimet[i]}|{Paikka[i]}|{Erppi[i]}|{Tagit[i]}|{Sarjanumero[i]}|{Hys[i]}|{intervalli[i]}|{Maa[i]}|{KoneenTyyppi[i]}|{TestiInfo[i]}|{ProjektiNumero[i]}')
                    except Exception as e:
                        print(f"Napit ei pystytty lukemaan")

      
        j = 0


        for j in range(ParametritKpl[i]):
            try:
                Vianhaku = (f'ns=3;s="{Paikka[i]}"."stHMI"."stParameter"[{j}]."sName"')
                HaettuNimi = client.get_node(f'ns=3;s="{Paikka[i]}"."stHMI"."stParameter"[{j}]."sName"').get_value()     
                print(f"Haettu nimi: {HaettuNimi}")
                ParmetriNodet.append(f'{HaettuNimi}|ns=3;s="{Paikka[i]}"."stHMI"."stParameter"[{j}]."fValue"|{Nimet[i]}|{Paikka[i]}|{Erppi[i]}|{Tagit[i]}|{Sarjanumero[i]}|{Hys[i]}|{intervalli[i]}|{Maa[i]}|{KoneenTyyppi[i]}|{TestiInfo[i]}|{ProjektiNumero[i]}')
            except Exception as e:
                print(f"Nodea {Vianhaku} ei pystytty lukemaan, numero j = {j}, numero i = {i}")
        j = 0
        




















except Exception as e:
    print("Error:", e)
finally:
    # Sulje yhteys palvelimeen
    client.disconnect()
    print("Client disconnected")


    

























#for i in range(len(DataNodet)):
    #print(f'Datanode {DataNodet[i]}')
    #print(f'ButtonNode {ButtonNodet[i]}')
    

"""for i in range(len(Paikka)):       
    print(f"Paikka {Paikka[i]}")
    print(f"Nimi {Nimet[i]}")
    print(f"DataKpl {DataKpl[i]}")
    print(f"NapitKpl {NapitKpl[i]}")
    print(f"ParametritKpl {ParametritKpl[i]}")
    print(f"ERP {Erppi[i]}")
    print(f"Tag {Tagit[i]}")
    print(f"Sarjanumero {Sarjanumero[i]}")
    print(f"Hysteresis {Hys[i]}")
    print(f"Intervalli {intervalli[i]}")
    print(f"Maa {Maa[i]}")
    print(f"Koneen tyyppi {KoneenTyyppi[i]}")
    print(f"Testi info {TestiInfo[i]}")
    print(f"Projekti numero {ProjektiNumero[i]}")
    print("\n" * 3)"""




data1 = {
    "DataNodet": DataNodet,
}

# Tallenna tiedot JSON-tiedostoon
with open('DataNodet.json', 'w') as f:
    json.dump(data1, f, indent=4)
    
data2 = {
    "ButtonNodet": ButtonNodet,
}

# Tallenna tiedot JSON-tiedostoon
with open('ButtonNodet.json', 'w') as f:
    json.dump(data2, f, indent=4)

data3 = {
    "ParmetriNodet": ParmetriNodet,
}

# Tallenna tiedot JSON-tiedostoon
with open('ParmetriNodet.json', 'w') as f:
    json.dump(data3, f, indent=4)
    


