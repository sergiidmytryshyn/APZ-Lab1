from fastapi import FastAPI
from hazelcast import HazelcastClient
import threading
import socket
import requests
import os
import time

app = FastAPI()
messages = []

CONSUL_URL = "http://localhost:8500"

def get_config_value(key: str) -> str:
    url = f"{CONSUL_URL}/v1/kv/{key}?raw"
    resp = requests.get(url)
    if resp.status_code == 200:
        return resp.text
    raise RuntimeError(f"Config key '{key}' not found in Consul")

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
    port = int(os.environ.get("PORT", 8001))
    register_with_consul("messages-service", port)

    cluster_name = get_config_value("hazelcast/config/cluster_name")
    queue_name = get_config_value("queue/config/name")

    hz_client = HazelcastClient(cluster_name=cluster_name)
    msg_queue = hz_client.get_queue(queue_name).blocking()

    def consume_messages():
        while True:
            try:
                msg = msg_queue.take()
                print(f"[Messages-Service] Got message: {msg}")
                messages.append(msg)
            except Exception as e:
                print(f"[Messages-Service] Hazelcast error: {e}, retrying in 5s...")
                time.sleep(5)

    threading.Thread(target=consume_messages, daemon=True).start()

@app.get("/message")
async def get_message():
    return {"messages": messages}

@app.get("/health")
async def health():
    return {"status": "ok"}
