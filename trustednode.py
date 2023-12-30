#!/usr/bin/env python3

import socket
import json
import ssl
import sys

def send_query_to_proxy(query):
    try:
        # Proxy hostname and port
        proxy_hostname = 'ip-172-31-90-19.ec2.internal'
        proxy_port = 5000

        # Create an SSL context for the client
        context = ssl.create_default_context()

        # Load the proxy's self-signed certificate
        context.load_verify_locations('proxy_cert.pem')

        # Create a socket object for outgoing requests to the proxy server
        proxy_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        # Wrap the socket with SSL
        secure_proxy_socket = context.wrap_socket(proxy_socket, server_hostname=proxy_hostname)

        # Establish a secure connection with the proxy server
        secure_proxy_socket.connect((proxy_hostname, proxy_port))

        # Send the query to the proxy server
        secure_proxy_socket.send(query.encode('utf-8'))

        # Receive and process the response
        response = secure_proxy_socket.recv(4096).decode('utf-8')
        result = json.loads(response)

        print("Query Result:", result)

    except Exception as e:
        print("Error:", e)
        result = {'error': str(e)}

    finally:
        # Close the secure connection with the proxy server
        secure_proxy_socket.close()

    return result



def start_trusted_node_server():
    try:
        # Create an SSL context
        context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        
        # Load server's certificate and private key
        context.load_cert_chain('trusted_node_cert.pem', 'trusted_node_key.key')

        # Create a socket object for incoming connections from Gatekeeper
        gatekeeper_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        gatekeeper_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        
        # Bind to '0.0.0.0', port 5000
        gatekeeper_socket.bind(('0.0.0.0', 5000))

         # Wrap the socket with SSL
        socket_ssl = context.wrap_socket(gatekeeper_socket, server_side=True)

        # Listen to Gatekeeper requests and queue up to 5 requests
        socket_ssl.listen(5)
        
    while True:
            # Accept a connection with Gatekeeper
            clientsocket, addr = socket_ssl.accept()
            
            # Wrap the client socket with SSL
            secure_socket = context.wrap_socket(clientsocket, server_side=True)
            print(f"Got a secure connection from {addr}")

            # Verify Gatekeeper's pre-shared key
            key = secure_socket.recv(1024).decode('utf-8')
            if key != 'pre_shared_key':
                print("Invalid key. Connection closed.")
                secure_socket.send('Invalid key. Connection closed.'.encode('utf-8'))
                secure_socket.close()
                continue

            query = secure_socket.recv(1024).decode('utf-8')

            # Send query to proxy server
            result = send_query_to_proxy(query)

            # Sending the result back to the Gatekeeper
            secure_socket.send(json.dumps(result).encode('utf-8'))

            # Close connection with the Gatekeeper
            secure_socket.close()

    except Exception as e:
        print("Error:", e)
        sys.exit(1)

def main():
    start_trusted_node_server()   

if __name__ == '__main__':
    main()
