from opcua import Client

def read_opcua_value(plc_url, node_id):
    try:
        # Luo yhteys PLC:hen
        client = Client(plc_url)
        client.connect()
        
        ParametrinNimi = f'ns=3;s="{node_id}"."stHMI"."stParameter"[{i}]."sName"'
        ParametrinArvo = f'ns=3;s="{node_id}"."stHMI"."stParameter"[{i}]."fValue"'
        
        
        
        # Hae noden arvo
        node = client.get_node(node_id)
        value = node.get_value()
        
        # Sulje yhteys
        client.disconnect()
        
        return value
    except Exception as e:
        print(f"Virhe OPC UA:n lukemisessa: {e}")
        return None