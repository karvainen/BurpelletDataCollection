from opcua import Client
from opcua import ua
import socket

def browse_nodes(node, depth, file, current_depth=0):
    if current_depth > depth:
        return

    children = node.get_children()
    for child in children:
        browse_name = child.get_browse_name()
        display_name = child.get_display_name()
        node_id = child.nodeid
        file.write(f"Node ID: {node_id}, Browse Name: {browse_name}, Display Name: {display_name}\n")

        # Recursive call to browse children
        browse_nodes(child, depth, file, current_depth + 1)

def main():
    # PLC:n osoite, käytä oletusporttia 4840
    plc_url = "opc.tcp://10.10.10.1:4840"
    

    # OPC UA -asiakas
    client = Client(plc_url)
    try:
        client.connect()
        print("Yhteys muodostettu")

        # Alkunode
        root = client.get_root_node()

        # Aseta syvyys
        syvyys = 3  # Korvaa halutulla syvyydellä

        # Luo tai avaa tiedosto kirjoitusta varten
        with open("nodet.txt", "w", encoding="utf-8") as file:
            # Lue kaikki nodet syvyyteen asti ja tallenna tiedostoon
            browse_nodes(root, syvyys, file)

    except ConnectionRefusedError:
        print("Yhteyttä ei voi muodostaa, koska kohdekone ei salli sitä. Tarkista IP-osoite ja portti.")
    except socket.error as e:
        print(f"Socket error: {e}")
    except Exception as e:
        print(f"Yleinen virhe: {e}")
    finally:
        try:
            client.disconnect()
            print("Yhteys katkaistu")
        except Exception as e:
            print(f"Virhe yhteyden katkaisussa: {e}")

if __name__ == "__main__":
    main()
