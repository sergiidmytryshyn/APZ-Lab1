import grpc
import uuid
import httpx
import random
import logging_service_pb2
import logging_service_pb2_grpc
from fastapi import FastAPI
from tenacity import retry, stop_after_attempt, wait_exponential
from hazelcast import HazelcastClient
import os
import socket
import requests

app = FastAPI()

CONSUL_URL = "http://localhost:8500"

def get_config_value(key: str) -> str:
    url = f"{CONSUL_URL}/v1/kv/{key}?raw"
    resp = requests.get(url)
    if resp.status_code == 200:
        return resp.text
    raise RuntimeError(f"Config key '{key}' not found in Consul")

async def discover_service(service_name: str) -> list[str]:
    url = f"{CONSUL_URL}/v1/catalog/service/{service_name}"
    async with httpx.AsyncClient() as client:
        resp = await client.get(url)
        if resp.status_code == 200:
            data = resp.json()
            return [f"{item['Address']}:{item['ServicePort']}" for item in data]
        return []

def register_with_consul(service_name: str, service_port: int):
    hostname = socket.gethostname()
    ip = socket.gethostbyname(hostname)

    payload = {
        "Name": service_name,
        "ID": f"{service_name}-{service_port}",
        "Address": ip,
        "Port": service_port,
        "Check": {
            "HTTP": f"http://{ip}:{service_port}/health",
            "Interval": "10s",
            "Timeout": "2s"
        }
    }

    url = f"{CONSUL_URL}/v1/agent/service/register"
    resp = requests.put(url, json=payload)
    if resp.status_code == 200:
        print(f"Registered {service_name} to Consul on {ip}:{service_port}")
    else:
        print(f"Failed to register: {resp.text}")

@app.on_event("startup")
def startup_event():
    port = int(os.environ.get("PORT", 8000))
    register_with_consul("facade-service", port)

cluster_name = get_config_value("hazelcast/config/cluster_name")
queue_name = get_config_value("queue/config/name")
hz_client = HazelcastClient(cluster_name=cluster_name)
msg_queue = hz_client.get_queue(queue_name).blocking()

@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1))
async def send_log_message(msg: str):
    addresses = await discover_service("logging-service")
    if not addresses:
        raise RuntimeError("No available logging services found")

    while addresses:
        addr = random.choice(addresses)
        try:
            with grpc.insecure_channel(addr) as channel:
                stub = logging_service_pb2_grpc.LoggingServiceStub(channel)
                stub.LogMessage(logging_service_pb2.LogRequest(uuid=str(uuid.uuid4()), msg=msg))
                return
        except grpc.RpcError:
            addresses.remove(addr)
    raise RuntimeError("All logging-service instances failed")

@app.post("/send")
async def send_message(msg: str):
    await send_log_message(msg)
    msg_queue.offer(msg)
    return {"status": "message sent"}

@app.get("/get")
async def get_messages():
    logging_addresses = await discover_service("logging-service")
    message_addresses = await discover_service("messages-service")

    if not logging_addresses or not message_addresses:
        return {"error": "Missing services"}

    while logging_addresses:
        log_addr = random.choice(logging_addresses)
        try:
            with grpc.insecure_channel(log_addr) as channel:
                stub = logging_service_pb2_grpc.LoggingServiceStub(channel)
                log_response = stub.GetLogs(logging_service_pb2.Empty())

            msg_addr = random.choice(message_addresses)
            async with httpx.AsyncClient() as client:
                msg_response = await client.get(f"http://{msg_addr}/message")

            return {
                "logs": log_response.messages,
                "messages": msg_response.json()
            }

        except grpc.RpcError:
            logging_addresses.remove(log_addr)
    return {"error": "No available logging services"}


@app.get("/health")
def health():
    return {"status": "ok"}
