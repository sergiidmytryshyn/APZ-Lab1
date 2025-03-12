import grpc
import logging_service_pb2
import logging_service_pb2_grpc
from concurrent import futures
from hazelcast import HazelcastClient
import subprocess
import atexit
import sys

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

def run():
    hz_proc = subprocess.Popen(["../../lab2/hazelcast-5.5.0/bin/hz-start"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    atexit.register(kill_proc, hz_proc)
    hz_client = HazelcastClient(cluster_name="hello-world", smart_routing=True)

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    logging_service_pb2_grpc.add_LoggingServiceServicer_to_server(LoggingService(hz_client), server)
    port = int(sys.argv[1])
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    print(f"gRPC logging service is running on port {port}")
    server.wait_for_termination()

def kill_proc(proc):
    proc.terminate()
    proc.wait()
    print("Killed process") 

if __name__ == "__main__":
    run()
