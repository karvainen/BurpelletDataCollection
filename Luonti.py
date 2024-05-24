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
import json
#import LueParametrit

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

except Exception as e:
    print("Error:", e)
finally:
    # Sulje yhteys palvelimeen
    client.disconnect()
    print("Client disconnected")


for i in range(len(Paikka)):
    
    #Jos nodella on dataa niin tehdään siihen valmiit nodet
    for j in range(DataKpl[i]):
        DataNodet.append(f'ns=3;s="{Paikka[i]}"."stHMI"."stData"[{i}]."eValueType"|ns=3;s="{Paikka[i]}"."stHMI"."stData"[{i}]."fValue"|{Paikka[i]}|{Nimet[i]}|{Erppi[i]}|{Tagit[i]}|{Sarjanumero[i]}|{Hys[i]}|{intervalli[i]}|{Maa[i]}|{KoneenTyyppi[i]}|{TestiInfo[i]}|{ProjektiNumero[i]}')
    
    for j in range(NapitKpl[i]):
        ButtonNodet.append(f'ns=3;s="{Paikka[i]}"."stHMI"."stButtons"[{i}]."sName"|ns=3;s="{Paikka[i]}"."stHMI"."stButtons"[{i}]."bStateMemory"|{Paikka[i]}|{Nimet[i]}|{Erppi[i]}|{Tagit[i]}|{Sarjanumero[i]}|{Hys[i]}|{intervalli[i]}|{Maa[i]}|{KoneenTyyppi[i]}|{TestiInfo[i]}|{ProjektiNumero[i]}')
        

    for j in range(ParametritKpl[i]):
        ParmetriNodet.append(f'ns=3;s="{Paikka[i]}"."stHMI"."stParameters"[{i}]."sName"|ns=3;s="{Paikka[i]}"."stHMI"."stParameters"[{i}]."fValue"|{Paikka[i]}|{Nimet[i]}|{Erppi[i]}|{Tagit[i]}|{Sarjanumero[i]}|{Hys[i]}|{intervalli[i]}|{Maa[i]}|{KoneenTyyppi[i]}|{TestiInfo[i]}|{ProjektiNumero[i]}')
    










for i in range(len(DataNodet)):
    print(f'Datanode {DataNodet[i]}')
    print(f'ButtonNode {ButtonNodet[i]}')
    

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
    

data = {
    "DataNodet": DataNodet,
    "ButtonNodet": ButtonNodet,
    "ParmetriNodet": ParmetriNodet
}

# Tallenna tiedot JSON-tiedostoon
with open('Nodet.json', 'w') as f:
    json.dump(data, f, indent=4)
    



