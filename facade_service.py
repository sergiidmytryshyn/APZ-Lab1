import grpc
import uuid
import httpx
import random
import logging_service_pb2
import logging_service_pb2_grpc
from fastapi import FastAPI
from tenacity import retry, stop_after_attempt, wait_exponential

app = FastAPI()

LOGGING_SERVICE_ADDRESSES = ["localhost:8081", "localhost:8082", "localhost:8083"]
MESSAGES_SERVICE_URL = "http://localhost:8001"

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

@app.get("/get")
async def get_messages():
    addresses = LOGGING_SERVICE_ADDRESSES.copy()
    while addresses:
        LOGGING_SERVICE_ADDRESS = random.choice(addresses)
        try:
            with grpc.insecure_channel(LOGGING_SERVICE_ADDRESS) as channel:
                stub = logging_service_pb2_grpc.LoggingServiceStub(channel)
                log_response = stub.GetLogs(logging_service_pb2.Empty())
            async with httpx.AsyncClient() as client:
                msg_response = await client.get(f"{MESSAGES_SERVICE_URL}/message")    
            return log_response.messages, msg_response.text
        except grpc.RpcError:
            addresses.remove(LOGGING_SERVICE_ADDRESS)
    return {"error": "No available logging services"}