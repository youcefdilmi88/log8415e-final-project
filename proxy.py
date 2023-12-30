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

# SQL server configurations
sql_servers = {
    'Worker1': { 'host': 'ip-172-31-95-107.ec2.internal', 'user': 'root', 'password': 'hello', 'database': 'sakila'},
    'Worker2': { 'host': 'ip-172-31-93-149.ec2.internal', 'user': 'root', 'password': 'hello', 'database': 'sakila'},
    'Worker3': { 'host': 'ip-172-31-87-210.ec2.internal', 'user': 'root', 'password': 'hello', 'database': 'sakila'},
    'Manager': { 'host': 'ip-172-31-94-157.ec2.internal', 'user': 'root', 'password': 'hello', 'database': 'sakila'},
}

# Global variable to store server selection mode
server_selection_mode = "random"

# Function to choose a random  worker server
def random_worker():
    return random.choice([server_info['host'] for server_info in sql_servers.values()][:-1])

# Function to choose a server based on the specified mode
def choose_server(query):
    if server_selection_mode == "random":
        return random_worker()
    elif server_selection_mode == "directhit":
        return sql_servers['Manager']['host']
    elif server_selection_mode == "customized":
        return choose_lowest_latency_server()
    elif server_selection_mode == "loadbalance":
        return choose_load_balance_server(query)
    else:
        raise ValueError("Unknown server selection mode")

# Function to choose the server with the lowest latency
def choose_lowest_latency_server():
    lowest_latency = float('inf')
    chosen_host = None
    for host in random_worker():  # Exclude host4
        try:
            ping_output = subprocess.check_output(['ping', '-c', '1', host])
            ping_output_str = ping_output.decode('utf-8')
            latency = parse_ping_response(ping_output_str)
            if latency < lowest_latency:
                lowest_latency = latency
                chosen_host = host
        except subprocess.CalledProcessError:
            continue
    return chosen_host if chosen_host else random_worker()

# Function to parse ping response and extract latency
def parse_ping_response(ping_output):
    time_pattern = r'time=(\d+\.\d+) ms'

    # Search for the pattern
    match = re.search(time_pattern, ping_output)

    # If a match is found, return the latency value
    if match:
        return float(match.group(1))
    else:
        return None
    pass

# Function to choose the server for load balancing
def choose_load_balance_server(query):

    if query.strip().lower().startswith("select"):
        return random_worker() # Random worker for SELECT queries
    else:
        return sql_servers['Manager']['host'] # Manager for other queries

# Function to forward query to SQL server
def forward_query(query):

    chosen_host = choose_server(query)
    config = sql_servers[chosen_host]
    print(f"Forwarding query to {chosen_host}")

    try:
        # Connect to the database
        connection = pymysql.connect(host=config['host'],
                                     user=config['user'],
                                     password=config['password'],
                                     database=config['database'],
                                     cursorclass=pymysql.cursors.DictCursor)

        with connection:
            with connection.cursor() as cursor:
                # Execute the query
                cursor.execute(query)
                rows = cursor.fetchall()

                result = []
                for row in rows:
                    processed_row = {key: (value.isoformat() if isinstance(value, datetime.datetime) else value) for key, value in row.items()}
                    result.append(processed_row)

            # Commit the changes if needed
            connection.commit()
        return result

    except Exception as e:
        print(f"Error while forwarding query to {chosen_host}: {e}")
        return {'error': str(e)}

def start_proxy_server():
    try:
        # SSL Context creation and configuration
        context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        context.load_cert_chain(certfile='proxy_cert.pem', keyfile='proxy_key.key')
        
        # Create a socket object
        trusted_node_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        trusted_node_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        
        # Bind to '0.0.0.0', port 5000
        trusted_node_socket.bind(('0.0.0.0', 5000))
        
        # Wrap the socket with SSL
        s_ssl = context.wrap_socket(trusted_node_socket, server_side=True)

        # Listen to Trusted node requests and queue up to 5 requests
        s_ssl.listen(5)
        print(f"Proxy Server listening on port 5000")

        while True:
            # Accept a connection with the Trusted node
            secure_socket, addr = s_ssl.accept()
            print(f"Got a secure secure connection from {addr}")

            # Receive query from Gatekeeper
            query = secure_socket.recv(1024).decode('utf-8')
            result = forward_query(query)

            # Sending the result back to the Trusted node
            secure_socket.send(json.dumps(result).encode('utf-8'))

            secure_socket.close()

    except Exception as e:
        print("Error:", e)
        sys.exit(1)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='SQL Proxy Server')
    parser.add_argument('--mode', type=str, choices=['random', 'directhit', 'customized', 'loadbalance'], default='random',
                        help='Server selection mode: random, directhit, customized, loadbalance')
    args = parser.parse_args()
    server_selection_mode = args.mode

    start_proxy_server()
