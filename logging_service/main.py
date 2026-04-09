import os, hazelcast
from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()
POD_NAME = os.getenv("HOSTNAME", "Unknown-Pod")
HZ_MEMBERS = os.getenv("HZ_MEMBERS", "hazelcast:5701").split(",")

client = hazelcast.HazelcastClient(cluster_members=HZ_MEMBERS, cluster_name="dev")
msg_map = client.get_map("messages").blocking()

class Message(BaseModel):
    uuid: str
    msg: dict

@app.post("/log")
def log_message(data: Message):
    msg_map.put(data.uuid, str(data.msg))
    print(f"[{POD_NAME}] Logged message: {data.uuid}")
    return {"status": "ok", "pod": POD_NAME}

@app.get("/logs")
def get_logs():
    return [{"uuid": k, "msg": v} for k, v in msg_map.entry_set()]