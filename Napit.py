import time
import importlib

while True:
    try:
        # Tuo ja suorita toinen skripti
        toinen_skripti = importlib.import_module('ButtonSave')
        toinen_skripti.paa_logiikka()
    except Exception as e:
        print(f"Error: {e}")
        time.sleep(5)  # Odota ennen uudelleenkäynnistystä
        continue
