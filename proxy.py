#!/usr/bin/env python3

import socket
import random
import sys
import json
import pymysql
import argparse
import subprocess
import re
import datetime
import ssl

# SQL server configurations with details for worker and manager nodes.
sql_servers = {
    'Worker1': { 'host': 'ip-172-31-95-107.ec2.internal', 'user': 'root', 'password': 'hello', 'database': 'sakila'},
    'Worker2': { 'host': 'ip-172-31-93-149.ec2.internal', 'user': 'root', 'password': 'hello', 'database': 'sakila'},
    'Worker3': { 'host': 'ip-172-31-87-210.ec2.internal', 'user': 'root', 'password': 'hello', 'database': 'sakila'},
    'Manager': { 'host': 'ip-172-31-94-157.ec2.internal', 'user': 'root', 'password': 'hello', 'database': 'sakila'},
}

# Global variable to store server selection mode
server_selection_mode = "random"

def random_worker():
    """
    Choose a random worker server from the available SQL servers.    
    
    Returns:
        str: The name of the selected server.
    """
    return random.choice(list(sql_servers.keys())[:3])

def choose_server(query):
    """
    Choose a server based on the specified mode.
    
    Args:
        query (str): The SQL query to be executed.
    
    Returns:
        str: The name of the selected server.
        
    Raises:
        ValueError: If the server selection mode is unknown.
    """
    if server_selection_mode == "random":
        return random_worker()
    elif server_selection_mode == "directhit":
        return 'Manager'
    elif server_selection_mode == "customized":
        return choose_lowest_latency_server()
    elif server_selection_mode == "loadbalance":
        return choose_load_balance_server(query)
    else:
        raise ValueError("Unknown server selection mode")

def choose_lowest_latency_server():
    """
    Choose the server with the lowest latency based on ping response times.
    
    Returns:
        str: The name of the server with the lowest latency.
    """
    lowest_latency = float('inf')
    chosen_host = None
    for host in random_worker():  # Exclude host4
        try:
            ping_output = subprocess.check_output(['ping', '-c', '1', sql_servers[host]['host']])
            ping_output_str = ping_output.decode('utf-8')
            latency = parse_ping_response(ping_output_str)
            if latency < lowest_latency:
                lowest_latency = latency
                chosen_host = host
        except subprocess.CalledProcessError:
            continue
    return chosen_host if chosen_host else random_worker()

def parse_ping_response(ping_output):
    """
    Parse ping response to extract the latency.
    
    Args:
        ping_output (str): The output from the ping command.
        
    Returns:
        float: The latency extracted from the ping output, or None if no latency found.
    """
    time_pattern = r'time=(\d+\.\d+) ms'

    # Search for the pattern
    match = re.search(time_pattern, ping_output)

    # If a match is found, return the latency value
    if match:
        return float(match.group(1))
    else:
        return None
    pass

def choose_load_balance_server(query):
    """
    Choose the server for load balancing based on the query type.
    
    Args:
        query (str): The SQL query to be executed.
    
    Returns:
        str: The name of the server based on the load balancing strategy.
    """
    print("Choosing load balance server")
    if query.strip().lower().startswith("select"):
        return random_worker() # Random worker for SELECT queries
    else:
        return 'Manager' # Manager for other queries

def forward_query(query):
    """
    Forward the received SQL query to the chosen SQL server and return the result.
    
    Args:
        query (str): The SQL query to be executed.
    
    Returns:
        dict: The result of the executed query or error if any.
    """
    print(f"Received query: {query}")

    chosen_host = choose_server(query)
    config = sql_servers[chosen_host]
    print(f"Forwarding query to {chosen_host}")

    try:
        # Connect to the database
        print("Connecting to database")
        connection = pymysql.connect(host=config['host'],
                                     user=config['user'],
                                     password=config['password'],
                                     database=config['database'],
                                     cursorclass=pymysql.cursors.DictCursor)

        with connection:
            with connection.cursor() as cursor:
                # Execute the query
                print("Executing query")
                cursor.execute(query)
                rows = cursor.fetchall()

                result = []
                for row in rows:
                    processed_row = {key: (value.isoformat() if isinstance(value, datetime.datetime) else value) for key, value in row.items()}
                    result.append(processed_row)

            # Commit the changes if needed
            print("Committing changes if needed")
            connection.commit()
        return result

    except Exception as e:
        print(f"Error while forwarding query to {chosen_host}: {e}")
        return {'error': str(e)}

def start_proxy_server():
    """
    Start the proxy server to handle incoming SQL queries, forward them to the appropriate SQL server,
    and send the responses back to the client.
    
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
        context.load_cert_chain(certfile='proxy.crt', keyfile='proxy.key')
        context.check_hostname = False
        
        # Wrap the socket with SSL
        secure_socket = context.wrap_socket(server_socket, server_side=True)

        # Listen to requests and queue up to 5 requests
        secure_socket.listen(5)
        print("Proxy Server listening on port 5000")


        while True:
            try:
                # Accept a connection with the Trusted node
                tn_socket, addr = secure_socket.accept()
                print(f"Got a secure connection from {addr}")

                # Receive query from Trusted node
                query = tn_socket.recv(1024).decode('utf-8')

                # Forward query to SQL server
                result = forward_query(query)

                # Sending the result back to the Trusted node
                tn_socket.send(json.dumps(result).encode('utf-8'))

                # Close connection with the Gatekeeper
                tn_socket.close()

            except Exception as e:
                print(f"Error handling connection: {e}")

    except Exception as e:
        print("Error:", e)
        sys.exit(1)


if __name__ == '__main__':
    """
    Main entry point of the script.
    
    Parses command line arguments to set the server selection mode and starts the proxy server.
    The server selection mode determines how the proxy chooses which SQL server to forward requests to.
    
    Available modes are:
    - 'random': Chooses a random worker server.
    - 'directhit': Always chooses the manager server.
    - 'customized': Chooses the server with the lowest latency based on ping times.
    - 'loadbalance': Chooses a server based on load balancing strategy for query type.
    
    The proxy server listens for incoming connections on port 5000, securely forwarding SQL queries
    to the chosen SQL server and returning the results to the client.
    """
    parser = argparse.ArgumentParser(description='SQL Proxy Server')
    parser.add_argument('--mode', type=str, choices=['random', 'directhit', 'customized', 'loadbalance'], default='random',
                        help='Server selection mode: random, directhit, customized, loadbalance')
    args = parser.parse_args()
    server_selection_mode = args.mode

    start_proxy_server()
