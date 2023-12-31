#!/usr/bin/env python3

import socket
import json
import ssl



def send_query(query):
    """
    Send a SQL query to the Trusted Node and receive the response.

    This function creates a secure SSL connection to the Trusted Node, sends a pre-shared key for authentication, 
    and then receives the query response. This function also prints the response to query in the console.

    Args:
        query (str): The SQL query to be sent to the Trusted Node.

    Returns:
        dict: The response from the Trusted Node or an error message if the request fails.
    """
    try:
        # Trusted node hostname and port
        host = 'ip-172-31-90-19.ec2.internal'
        port = 5000

        # Create an SSL context
        context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)

        # Load the CA certificate
        context.load_verify_locations('tn.crt')

        # Create a socket object for outgoing requests to the Trusted node
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        # Wrap the socket with SSL
        secure_socket = context.wrap_socket(client_socket, server_hostname=host)

        # Establish a secure connection to the Trusted node
        secure_socket.connect((host, port))

        # Authenticate the Gatekeeper to the Trusted node
        secure_socket.send('pre_shared_key'.encode('utf-8'))

        # Send the query to the Trusted node
        secure_socket.send(query.encode('utf-8'))

        # Receiving the response from the Trusted node and process the response
        response = secure_socket.recv(4096).decode('utf-8')
        result = json.loads(response)

        print("Query Result:", result)

    except Exception as e:
        print("Error:", e)

    finally:
        # Close the secure connection with the proxy server
        secure_socket.close()



def main():
    """
    Main function to interactively send SQL queries to the Trusted Node.

    Continuously prompts the user for SQL queries and sends them to the Trusted Node using the 'send_query' function. 
    The loop can be exited by typing 'exit'.
    """
    while True:
        query = input("Enter SQL query (or type 'exit' to quit): ")
        if query.lower() == 'exit':
            break
        send_query(query)

if __name__ == '__main__':
    main()
