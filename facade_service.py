import grpc
import uuid
import httpx
import random
import logging_service_pb2
import logging_service_pb2_grpc
from fastapi import FastAPI
from tenacity import retry, stop_after_attempt, wait_exponential
from hazelcast import HazelcastClient

app = FastAPI()

LOGGING_SERVICE_ADDRESSES = ["localhost:8081", "localhost:8082", "localhost:8083"]
MESSAGE_SERVICE_ADDRESSES = ["http://localhost:8001", "http://localhost:8002"]

hz_client = HazelcastClient(cluster_name="hello-world")
msg_queue = hz_client.get_queue("messages-queue").blocking()

@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1))
async def send_log_message(msg):
    addresses = LOGGING_SERVICE_ADDRESSES.copy()
    while addresses:
        LOGGING_SERVICE_ADDRESS = random.choice(addresses)
        try:
            with grpc.insecure_channel(LOGGING_SERVICE_ADDRESS) as channel:
                stub = logging_service_pb2_grpc.LoggingServiceStub(channel)
                stub.LogMessage(logging_service_pb2.LogRequest(uuid=str(uuid.uuid4()), msg=msg))
            return
        except grpc.RpcError:
            addresses.remove(LOGGING_SERVICE_ADDRESS)
    
    raise RuntimeError("No available logging services")

@app.post("/send")
async def send_message(msg: str):
    await send_log_message(msg)
    msg_queue.offer(msg)
    return {"status": "message sent"}

@app.get("/get")
async def get_messages():
    addresses = LOGGING_SERVICE_ADDRESSES.copy()
    while addresses:
        LOGGING_SERVICE_ADDRESS = random.choice(addresses)
        try:
            with grpc.insecure_channel(LOGGING_SERVICE_ADDRESS) as channel:
                stub = logging_service_pb2_grpc.LoggingServiceStub(channel)
                log_response = stub.GetLogs(logging_service_pb2.Empty())
            
            msg_service_url = random.choice(MESSAGE_SERVICE_ADDRESSES)
            async with httpx.AsyncClient() as client:
                msg_response = await client.get(f"{msg_service_url}/message")

            return {
                "logs": log_response.messages,
                "messages": msg_response.json()
            }

        except grpc.RpcError:
            addresses.remove(LOGGING_SERVICE_ADDRESS)
    return {"error": "No available logging services"}
