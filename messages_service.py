from fastapi import FastAPI
from hazelcast import HazelcastClient
import threading

app = FastAPI()
messages = []

hz_client = HazelcastClient(cluster_name="hello-world")
msg_queue = hz_client.get_queue("messages-queue").blocking()

def consume_messages():
    while True:
        msg = msg_queue.take()
        print(f"[Messages-Service] Got message: {msg}")
        messages.append(msg)

t = threading.Thread(target=consume_messages, daemon=True)
t.start()

@app.get("/message")
async def get_message():
    return {"messages": messages}
