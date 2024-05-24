import asyncio
from asyncua import Client

opc_ua_url = "opc.tcp://10.10.10.1:4840"  # Korvaa oikealla osoitteella

async def test_connection():
    try:
        async with Client(url=opc_ua_url) as client:
            print("Yhteys onnistui!")
    except Exception as e:
        print(f"Yhteys epäonnistui: {e}")

if __name__ == "__main__":
    asyncio.run(test_connection())
