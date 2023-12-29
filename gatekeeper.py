import socket
import json

def send_query(query):
    try:
        # Create a socket object for outgoing requests to the Trusted node
        trusted_node_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        # Trusted node hostname and port
        host = 'ip-172-31-90-19.ec2.internal'
        port = 5000

        # Establish a connection to the Trusted node
        trusted_node_socket.connect((host, port))

        # Authenticate the Gatekeeper to the Trusted node
        trusted_node_socket.send('pre_shared_key'.encode('utf-8'))

        # Send the query to the Trusted node
        trusted_node_socket.send(query.encode('utf-8'))

        # Receiving the response from the Trusted node and process the response
        response = trusted_node_socket.recv(4096).decode('utf-8')
        result = json.loads(response)
        print("Query Result:", result)

        # Close the connection with the Trusted node
        trusted_node_socket.close()

    except Exception as e:
        print("Error:", e)

def main():
    while True:
        query = input("Enter SQL query (or type 'exit' to quit): ")
        if query.lower() == 'exit':
            break
        send_query(query)

if __name__ == '__main__':
    main()
