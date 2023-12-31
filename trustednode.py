#!/usr/bin/env python3

import socket
import json
import ssl
import sys

def send_query_to_proxy(query):
    """
    Send a SQL query to the proxy server and receive the response.

    Args:
        query (str): The SQL query to be sent to the proxy server.

    Returns:
        dict: The response from the proxy server or an error message if the request fails.
    """
    try:
        # Proxy hostname and port
        host= 'ip-172-31-95-113.ec2.internal'
        port = 5000

        # Create an SSL context for the client
        context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
        print("ssl context created")

        # Load the proxy's self-signed certificate
        context.load_verify_locations('proxy.crt')
        print("certificate loaded")

        # Create a socket object for outgoing requests to the proxy server
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        print("client socket created")

        print("wrapping socket with ssl")
        # Wrap the socket with SSL
        secure_socket = context.wrap_socket(client_socket, server_hostname=host)
        print("secure socket created")

        # Establish a secure connection with the proxy server
        print("connecting to proxy")
        secure_socket.connect((host, port))
        print("secure connection established")

        # Send the query to the proxy server
        print("sending query to proxy")
        secure_socket.send(query.encode('utf-8'))
        print("query sent to proxy")

        # Receive and process the response
        response = secure_socket.recv(4096).decode('utf-8')
        result = json.loads(response)

        print("Query Result:", result)

    except Exception as e:
        print("Error:", e)
        result = {'error': str(e)}

    finally:
        # Close the secure connection with the proxy server
        secure_socket.close()

    return result



def start_trusted_node_server():
    """
    Start the Trusted Node Server to handle incoming connections.

    This server listens for connections from a Gatekeeper, verifies the pre-shared key, 
    and forwards received SQL queries to the proxy server. It then sends the query results back to the Gatekeeper.
    
    The server uses SSL for secure communication.
    """
    try:

      # Create a socket object
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        # Bind to '0.0.0.0', port 5000
        server_socket.bind(('0.0.0.0', 5000))

        # SSL Context creation and configuration
        context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        context.load_cert_chain(certfile='tn.crt', keyfile='tn.key')
        context.check_hostname = False
        
        # Wrap the socket with SSL
        secure_socket = context.wrap_socket(server_socket, server_side=True)

        # Listen to requests and queue up to 5 requests
        secure_socket.listen(5)
        print("Trusted node listening on port 5000")

        while True:
            try:
                # Accept a connection with Gatekeeper
                gk_socket, addr = secure_socket.accept()
                print(f"Got a secure connection from {addr}")

                # Verify Gatekeeper's pre-shared key
                key = gk_socket.recv(1024).decode('utf-8')
                if key != 'pre_shared_key':
                    print("Invalid key. Connection closed.")
                    secure_socket.send('Invalid key. Connection closed.'.encode('utf-8'))
                    secure_socket.close()
                    continue
                print("Key verified.")
                # Receive query from Gatekeeper
                query = gk_socket.recv(1024).decode('utf-8')
                print("Query received:", query)

                # Forward query to proxy server
                result = send_query_to_proxy(query)

                # Sending the result back to the Gatekeeper
                gk_socket.send(json.dumps(result).encode('utf-8'))

                # Close connection with the Gatekeeper
                gk_socket.close()

            except Exception as e:
                print(f"Error handling connection: {e}")

    except Exception as e:
        print("Error:", e)
        sys.exit(1)




def main():
    """
    Main function to start the Trusted Node Server.
    """
    start_trusted_node_server()   

if __name__ == '__main__':
    main()

