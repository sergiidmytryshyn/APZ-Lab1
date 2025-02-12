import grpc
import logging_service_pb2
import logging_service_pb2_grpc
from concurrent import futures

messages = {}

class LoggingService(logging_service_pb2_grpc.LoggingServiceServicer):
    def LogMessage(self, request, context):
        messages[request.uuid] = request.msg
        print(f"New msg: {request.msg}")
        return logging_service_pb2.Empty()

    def GetLogs(self, request, context):
        return logging_service_pb2.Messages(messages=", ".join(messages.values()))

def run():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    logging_service_pb2_grpc.add_LoggingServiceServicer_to_server(LoggingService(), server)
    server.add_insecure_port("[::]:8081")
    server.start()
    print("gRPC logging service is running on port 8081")
    server.wait_for_termination()

if __name__ == "__main__":
    run()
