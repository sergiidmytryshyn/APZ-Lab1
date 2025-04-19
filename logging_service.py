import grpc
import logging_service_pb2
import logging_service_pb2_grpc
from concurrent import futures
from hazelcast import HazelcastClient
import subprocess
import atexit
import sys
import requests
import socket
from grpc_health.v1 import health, health_pb2, health_pb2_grpc

class LoggingService(logging_service_pb2_grpc.LoggingServiceServicer):
    def __init__(self, client):
        self.client = client
        self.messages = self.client.get_map("msgs").blocking()

    def LogMessage(self, request, context):
        self.messages.put(request.uuid, request.msg)
        print(f"New msg: {request.msg}")
        return logging_service_pb2.Empty()

    def GetLogs(self, request, context):
        messages = ", ".join(self.messages.values())
        print(f"Returning messages: {messages}")
        return logging_service_pb2.Messages(messages=messages)

def register_with_consul(service_name, service_port):
    hostname = socket.gethostname()
    ip = socket.gethostbyname(hostname)

    consul_url = "http://localhost:8500/v1/agent/service/register"
    payload = {
        "Name": service_name,
        "ID": f"{service_name}-{service_port}",
        "Address": ip,
        "Port": service_port,
        "Check": {
            "GRPC": f"{ip}:{service_port}",
            "GRPCUseTLS": False,
            "Interval": "10s",
            "Timeout": "2s"
        }
    }

    response = requests.put(consul_url, json=payload)
    if response.status_code == 200:
        print(f"Registered {service_name} to Consul on {ip}:{service_port}")
    else:
        print(f"Failed to register: {response.text}")

def run():
    hz_proc = subprocess.Popen(["../../lab2/hazelcast-5.5.0/bin/hz-start"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    atexit.register(kill_proc, hz_proc)
    hz_client = HazelcastClient(cluster_name="hello-world", smart_routing=True)

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    port = int(sys.argv[1])
    logging_service_pb2_grpc.add_LoggingServiceServicer_to_server(LoggingService(hz_client), server)

    health_servicer = health.HealthServicer()
    health_pb2_grpc.add_HealthServicer_to_server(health_servicer, server)
    health_servicer.set("", health_pb2.HealthCheckResponse.SERVING)

    server.add_insecure_port(f"0.0.0.0:{port}")

    register_with_consul("logging-service", port)

    server.start()
    print(f"gRPC logging service is running on port {port}")
    server.wait_for_termination()

def kill_proc(proc):
    proc.terminate()
    proc.wait()
    print("Killed process") 

if __name__ == "__main__":
    run()
