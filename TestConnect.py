import asyncio
from asyncua import Client

# OPC UA -asiakasasetukset
opc_ua_url = "opc.tcp://10.10.10.1:4840"

async def list_nodes():
    async with Client(url=opc_ua_url) as client:
        root = client.get_root_node()
        objects = await root.get_children()
        
        # Etsi 'ns=3;s=PLC' node ja listaa sen lapset
        plc_node = client.get_node("ns=3;s=PLC")
        plc_children = await plc_node.get_children()
        
        print(f"Children of node 'ns=3;s=PLC': {plc_children}")
        for child in plc_children:
            print("Child Node: ", child)
            grand_children = await child.get_children()
            for grand_child in grand_children:
                print("Grandchild Node: ", grand_child)

if __name__ == "__main__":
    asyncio.run(list_nodes())
