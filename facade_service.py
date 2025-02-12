import grpc
import uuid
import httpx
import logging_service_pb2
import logging_service_pb2_grpc
from fastapi import FastAPI
from tenacity import retry, stop_after_attempt, wait_exponential

app = FastAPI()

LOGGING_SERVICE_ADDRESS = "localhost:8081"
MESSAGES_SERVICE_URL = "http://localhost:8082"

@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1))
async def send_log_message(msg):
    with grpc.insecure_channel(LOGGING_SERVICE_ADDRESS) as channel:
        stub = logging_service_pb2_grpc.LoggingServiceStub(channel)
        stub.LogMessage(logging_service_pb2.LogRequest(uuid=str(uuid.uuid4()), msg=msg))

@app.post("/send")
async def send_message(msg: str):
    await send_log_message(msg)

@app.get("/get")
async def get_messages():
    with grpc.insecure_channel(LOGGING_SERVICE_ADDRESS) as channel:
        stub = logging_service_pb2_grpc.LoggingServiceStub(channel)
        log_response = stub.GetLogs(logging_service_pb2.Empty())
    async with httpx.AsyncClient() as client:
        msg_response = await client.get(f"{MESSAGES_SERVICE_URL}/message")    
    return log_response.messages, msg_response.text
