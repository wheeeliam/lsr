# COMP3331 Computer Networks and Applications Assignment
# Written by William Chen z5165219

from socket import *
import sys
import threading
import time
import queue

lock = threading.RLock()

UPDATE_INTERVAL = 1
ROUTE_UPDATE_INTERVAL = 30
HEARTBEAT_INTERVAL = 3

class Router:
    def __init__(self, source_router_name, port_number):
        self.source_router_name = source_router_name
        self.port_number = port_number
        self.neighbours = {}
        self.message_queue = queue.Queue(0)
        self.received_messages = {}
        self.heartbeat_count = {}
        self.sequence_number = 0
        self.message = None
        self.latest_sequence_number = {}

    def get_name(self):
        return self.source_router_name

    def get_port_number(self):
        return self.port_number

    def get_neighbours(self):
        s = ""
        for key in self.neighbours:
            s += "{}: Cost from source Router: {}, Port of neighbour: {}\n".format(key, self.neighbours[key][0], self.neighbours[key][1])
        return s

    def get_message(self):
        message = "{} {}".format(self.source_router_name, self.port_number)
        for server in self.neighbours:
            message += "|{} {} {}".format(server, str(self.neighbours[server][0]), str(self.neighbours[server][1]))
        message += f"|{self.sequence_number}"
        self.message = message
        return self.message

    def get_received_messages(self):
        return self.received_messages

    def update_sequence_number(self):
        self.sequence_number += 1

    def __str__(self):
        return "Source Router: {}, Port Number: {}".format(self.source_router_name, self.port_number)

def sending_packets(source_router, server_socket):
    while True:
        with lock:
            message = source_router.get_message().encode('utf-8')

            for server in source_router.neighbours:
                server_socket.sendto(message, ('127.0.0.1', source_router.neighbours[server][1]))
            source_router.update_sequence_number()
        time.sleep(UPDATE_INTERVAL)

def sending_received_packets(source_router, server_socket, routers, direct_neighbours):
    while True:
        with lock:
            try:
                message_details = source_router.message_queue.get_nowait()
                source = message_details[0]
                segment = message_details.split("|")
                sequence_no = segment[-1]

                message_identifier = (message_details, sequence_no, source)

                message_details = message_details.encode('utf-8')

                for server in source_router.neighbours:
                    if server is not source and source in routers:
                        neighbour_port = source_router.neighbours[server][1]
                        if source in direct_neighbours:
                            if direct_neighbours[source][1] not in source_router.heartbeat_count:
                                break
                        if neighbour_port not in source_router.received_messages or \
                        message_identifier not in source_router.received_messages[neighbour_port]:
                            server_socket.sendto(message_details, ('127.0.0.1', neighbour_port))
            except Exception as e:
                pass
        time.sleep(UPDATE_INTERVAL)


def manage_received_messages(source_router, message_identifier, received_from_port, direct_neighbours, routers):
    with lock:
        message_content = message_identifier[0]
        sequence_no = message_identifier[1]
        message_source = message_identifier[2]

        if received_from_port not in source_router.received_messages:
            source_router.received_messages[received_from_port] = [message_identifier]

        append_new_message = True
        if message_identifier not in source_router.received_messages[received_from_port]:
            prev_messages_from_source = source_router.received_messages[received_from_port]
            for prev_message in prev_messages_from_source:
                if message_source == prev_message[2]:
                    append_new_message = False
                    if sequence_no > prev_message[1]:
                        prev_messages_from_source.remove(prev_message)
                        prev_messages_from_source.append(message_identifier)

                        new_message_split = message_content.split("|")
                        old_message_split = prev_message[0].split("|")

                        if (len(new_message_split) < len(old_message_split)):
                            old_nodes = []
                            new_nodes = []
                            for neighbour_details in new_message_split[1:-1]:
                                new_nodes.append(neighbour_details[0])
                            for neighbour_details in old_message_split[1:-1]:
                                old_nodes.append(neighbour_details[0])
                            nodes_to_remove = list(set(old_nodes) - set(new_nodes))
                            for node_to_remove in nodes_to_remove:
                                delete_router(node_to_remove, routers, source_router)
                                remove_from_received_messages(node_to_remove, source_router)

            if append_new_message:
                prev_messages_from_source.append(message_identifier)

def process_message_into_graph(message, routers, direct_neighbours, source_router, received_from_port):
    with lock:
        segments = message.split("|")
        for segment in segments:
            split_segment = segment.split()
            if len(split_segment) == 1:
                sequence_no = split_segment[0]
            elif len(split_segment) == 2:
                received_router_name = split_segment[0]
                received_port = int(split_segment[1])
                if received_router_name not in routers:
                    add_to_routers = True
                    if received_router_name in direct_neighbours:
                        if received_from_port != direct_neighbours[received_router_name][1]:
                            add_to_routers = False
                        elif (received_from_port == direct_neighbours[received_router_name][1]) and (received_router_name not in source_router.neighbours):
                            source_router.neighbours[received_router_name] = (direct_neighbours[received_router_name][0], received_port)
                            add_to_routers = True
                    if add_to_routers:
                        routers[received_router_name] = Router(received_router_name, received_port)
            elif len(split_segment) > 2:
                router_name = split_segment[0]
                router_cost = float(split_segment[1])
                router_port = int(split_segment[2])
                routers[received_router_name].neighbours[router_name] = (router_cost, router_port)

def listening_for_packets(source_router, server_socket, routers, direct_neighbours, known_neighbour_ports):
    while True:
        with lock:
            try:
                message = server_socket.recvfrom(2048)
                message_content = message[0].decode('utf-8')
                received_from_port = message[1][1]
                message_source = message_content[0]
                segment = message_content.split("|")
                sequence_no = int(segment[-1])

                require_to_process = False
                if message_source not in source_router.latest_sequence_number:
                    source_router.latest_sequence_number[message_source] = sequence_no
                    require_to_process = True
                elif sequence_no > source_router.latest_sequence_number[message_source]:
                    source_router.latest_sequence_number[message_source] = sequence_no
                    require_to_process = True

                if received_from_port not in source_router.heartbeat_count:
                    source_router.heartbeat_count[received_from_port] = 1
                else:
                    source_router.heartbeat_count[received_from_port] += 1

                message_identifier = (message_content, sequence_no, message_source)
                
                if require_to_process:
                    process_message_into_graph(message_content, routers, direct_neighbours, source_router, received_from_port)
                    manage_received_messages(source_router, message_identifier, received_from_port, direct_neighbours, routers)
                    source_router.message_queue.put(message_content)

            except Exception as e:
                pass
        time.sleep(0.1)

def check_heartbeats(source_router, routers, direct_neighbours):
    first_iteration = True
    prev_heartbeat_count = {}
    time.sleep(HEARTBEAT_INTERVAL)
    while True:
        with lock:
            routers_to_delete = []
            if not first_iteration:
                for neighbour in source_router.neighbours:
                    neighbour_port = source_router.neighbours[neighbour][1]
                    if neighbour_port not in prev_heartbeat_count:
                        prev_heartbeat_count[neighbour_port] = 0
                    if neighbour_port not in source_router.heartbeat_count \
                    or prev_heartbeat_count[neighbour_port] == source_router.heartbeat_count[neighbour_port]:
                        if neighbour_port in source_router.heartbeat_count:
                            del source_router.heartbeat_count[neighbour_port]
                            del prev_heartbeat_count[neighbour_port]
                            source_router.received_messages.pop(neighbour_port, None)
                        routers_to_delete.append(neighbour)
            else:
                first_iteration = False

            for router_to_delete in routers_to_delete:
                delete_router(router_to_delete, routers, source_router)
            prev_heartbeat_count = source_router.heartbeat_count.copy()

        time.sleep(HEARTBEAT_INTERVAL)

def delete_router(router_name, routers, source_router):
    with lock:
        if router_name is not source_router.get_name():
            routers.pop(router_name, None)
            for router_key in routers:
                router = routers[router_key]
                if router_name in router.neighbours:
                    del router.neighbours[router_name]
                    source_router.latest_sequence_number.pop(router_name, None)

def remove_from_received_messages(source_to_delete, source_router):
    with lock:
        for neighbour_port in source_router.received_messages:
            for prev_message in source_router.received_messages[neighbour_port]:
                if source_to_delete == prev_message[2]:
                    source_router.received_messages[neighbour_port].remove(prev_message)

def calculate_dijkstra(source_router, routers, source_router_name):
    while True:
        time.sleep(ROUTE_UPDATE_INTERVAL)
        with lock:
            nodes = []
            distance = {}
            previous = {}
            active_routers = {}

            for server in routers:
                active_routers[server] = True
                distance[server] = float("inf")
                previous[server] = None
                nodes.append(server)
            distance[source_router_name] = 0

            while nodes:
                unoptimised_nodes = {}
                for node in nodes:
                    unoptimised_nodes[node] = distance[node]

                closest_router = min(unoptimised_nodes, key=unoptimised_nodes.get)
                nodes.remove(closest_router)

                for neighbour in routers[closest_router].neighbours:
                    if neighbour in active_routers:
                        temp_dist = distance[closest_router] + routers[closest_router].neighbours[neighbour][0]
                        if temp_dist < distance[neighbour]:
                            distance[neighbour] = temp_dist
                            previous[neighbour] = closest_router
                    else:
                        pass
            shortest_paths = {}
            for server in routers:
                shortest_paths[server] = server

            for server in routers:
                if server is not source_router_name:
                    prev = previous[server]
                    while prev is not None:
                        shortest_paths[server] += prev
                        prev = previous[prev]

            print("I am Router {}".format(source_router_name))
            for server in routers:
                if server is not source_router_name:
                    print("Least cost path to router {}: {} and the cost is {:.1f}".format(server, shortest_paths[server][::-1], distance[server]))
            if (len(source_router.neighbours) == 0):
                print("I contain no neighbours.")

def main():
    if len(sys.argv) != 2:
        print("Please only enter 1 argument.")
        sys.exit()

    routers = {}
    direct_neighbours = {}
    known_neighbour_ports = {}

    filename = sys.argv[1]
    file = open(filename, "r")
    lines = file.readlines()
    file.close()

    for line in lines:
        line = line.split()
        if len(line) == 1:
            neighbourCount = int(line[0])
        elif len(line) == 2:
            source_router_name = line[0]
            source_port = int(line[1])
            routers[source_router_name] = Router(source_router_name, source_port)
            source_router = routers[source_router_name]
        elif len(line) == 3:
            router_name = line[0]
            router_cost = float(line[1])
            router_port = int(line[2])
            source_router.neighbours[router_name] = (router_cost, router_port)
            direct_neighbours[router_name] = (router_cost, router_port)
            known_neighbour_ports[router_port] = router_name

    server_socket = socket(AF_INET, SOCK_DGRAM)

    server_socket.bind(('127.0.0.1', source_port))

    print('The server is ready to receive')
    server_socket.setblocking(False)

    t1 = threading.Thread(target=sending_packets, args=[source_router, server_socket])
    t2 = threading.Thread(target=listening_for_packets, args=[source_router, server_socket, routers, direct_neighbours, known_neighbour_ports])
    t3 = threading.Thread(target=sending_received_packets, args=[source_router, server_socket, routers, direct_neighbours])
    t4 = threading.Thread(target=calculate_dijkstra, args=[source_router, routers, source_router_name])
    t5 = threading.Thread(target=check_heartbeats, args=[source_router, routers, direct_neighbours])

    t1.start()
    t2.start()
    t3.start()
    t4.start()
    t5.start()
    
if __name__ == '__main__':
    main()