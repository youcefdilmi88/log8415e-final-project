import socket
import json


def send_query_to_proxy(query):
    try:
        # Create a socket object for outgoing requests the proxy server
        proxy_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        # Proxy hostname and port
        proxy_hostname = 'ip-172-31-90-19.ec2.internal'
        proxy_port = 5000

        # Establish a connection with the proxy server
        proxy_socket.connect((proxy_hostname, proxy_port))

        # Send the query to the proxy server
        proxy_socket.send(query.encode('utf-8'))

        # Receive and process the response
        response = proxy_socket.recv(4096).decode('utf-8')
        result = json.loads(response)

        print("Query Result:", result)

    except Exception as e:
        print("Error:", e)
        result = {'error': str(e)}

    finally:
        # Close the connection with the Proxy server
        proxy_socket.close()

    return result


def start_trusted_node_server():
    try:

        # Create a socket object for incoming connections from Gatekeeper
        gatekeeper_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        gatekeeper_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        
        # Bind to '0.0.0.0' to listen on all network interfaces
        gatekeeper_hostname = '0.0.0.0'
        listening_port = 5000
        gatekeeper_socket.bind((gatekeeper_hostname, listening_port))

        # Get the actual port number assigned
        port = gatekeeper_socket.getsockname()[1]
        print(f"Trusted node server listening on port {port}")

        # Listen to Gatekeeper requests and queue up to 5 requests
        gatekeeper_socket.listen(5)

        while True:
            # Establish a connection with Gatekeeper
            clientsocket, addr = gatekeeper_socket.accept()
            print(f"Got a connection from {addr}")

            key = clientsocket.recv(1024).decode('utf-8')
            if key != 'your_pre_shared_key':
                print("Invalid key. Connection closed.")
                clientsocket.send('Invalid key. Connection closed.'.encode('utf-8'))
                clientsocket.close()
                continue

            query = clientsocket.recv(1024).decode('utf-8')

            # Send query to proxy server
            result = send_query_to_proxy(query)

            # Sending the result back to the Gatekeeper
            clientsocket.send(json.dumps(result).encode('utf-8'))

            # Close connection with the Gatekeeper
            clientsocket.close()

    except Exception as e:
        print("Error:", e)

def main():
    start_trusted_node_server()   

if __name__ == '__main__':
    main()
