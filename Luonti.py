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

from opcua import Client
import config
from influxdb_client import InfluxDBClient, Point, WriteOptions
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
import asyncio

opcua_url = "opc.tcp://10.10.10.1:4840"
DataKpl = []
ParametritKpl = []
NapitKpl = []
Paikka = []
Nimet = []
Nodet = []

Nodet.append("fbAnaIn_Grease PR311 L2")
Nodet.append("fbAnaIn_Pressing nip PR311")
Nodet.append("fbDirectDrive_Hyd_Motor")
Nodet.append("fbDirectDrive_HydCooler")
Nodet.append("fbDirectDrive_HydHeating")
Nodet.append("fbValve_301_Press")
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
    ############################      Käydään läpi kaikki mittauspaikat ja haetaan niiden tiedot     ############################
        





        #################################     Tarkistetaan, että kaikki arvot on olemassa     #################################
        if Mittauspaikka == None:
            Mittauspaikka = "Arvoa ei saatu luettua"
        
        if nNumberOfValues == None:
            nNumberOfValues = 0

        if nNumberOfButtons == None:
            nNumberOfButtons = 0

        if nNumberOfParameters == None:
            nNumberOfParameters = 0

        if sName == None:
            sName = "Arvoa ei saatu luettua"
        #################################     Tarkistetaan, että kaikki arvot on olemassa     #################################






        
        #################################     Lisätään arvot listoihin                        #################################
        Paikka.append(Mittauspaikka)
        Nimet.append(sName)
        DataKpl.append(nNumberOfValues)
        NapitKpl.append(nNumberOfButtons)
        ParametritKpl.append(nNumberOfParameters)
        #################################     Lisätään arvot listoihin                        #################################

except Exception as e:
    print("Error:", e)
finally:
    # Sulje yhteys palvelimeen
    client.disconnect()
    print("Client disconnected")








for i in range(len(Paikka)):       
    print(f"{Paikka[i]}")
    print(f"{Nimet[i]}")
    print(f"{DataKpl[i]}")
    print(f"{NapitKpl[i]}")
    print(f"{ParametritKpl[i]}")
    print("\n" * 3)