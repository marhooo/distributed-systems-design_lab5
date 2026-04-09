import os, time, uuid, httpx, json, threading, hazelcast
from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()

# Зчитуємо конфігурацію, яку K8s передасть через ConfigMap
HZ_MEMBERS = os.getenv("HZ_MEMBERS", "hazelcast:5701").split(",")
MQ_NAME = os.getenv("MQ_NAME", "transaction_queue")

LOGGING_URL = "http://logging-service:8000"
COUNTER_URL = "http://counter-service:8000"

hz_client = None
tx_queue = None

def init_hz():
    global hz_client, tx_queue
    while hz_client is None:
        try:
            hz_client = hazelcast.HazelcastClient(cluster_members=HZ_MEMBERS, cluster_name="dev")
            tx_queue = hz_client.get_queue(MQ_NAME).blocking()
        except Exception as e:
            time.sleep(2)

@app.on_event("startup")
def startup():
    threading.Thread(target=init_hz, daemon=True).start()

class TransactionRequest(BaseModel):
    user_id: str
    amount: int

@app.post("/transaction")
async def process_transaction(tx: TransactionRequest):
    transaction_id = str(uuid.uuid4())
    
    # K8s автоматично збалансує цей запит на одну з 3-х нод logging-service
    async with httpx.AsyncClient() as client:
        try:
            await client.post(f"{LOGGING_URL}/log", json={"uuid": transaction_id, "msg": tx.dict()}, timeout=2.0)
        except Exception as e:
            print(f"Logging failed: {e}")

    # Відправка в MQ
    if tx_queue:
        tx_queue.put(json.dumps(tx.dict()))

    return {"transaction_id": transaction_id, "status": "Sent to MQ via K8s"}

@app.get("/user/{user_id}")
async def get_user_data(user_id: str):
    balance = None
    all_logs = []
    
    async with httpx.AsyncClient() as client:
        try:
            balance_resp = await client.get(f"{COUNTER_URL}/balance/{user_id}", timeout=2.0)
            balance = balance_resp.json().get("balance")
        except:
            pass

        try:
            logs_resp = await client.get(f"{LOGGING_URL}/logs", timeout=2.0)
            all_logs = logs_resp.json()
        except:
            pass
            
    return {"balance": balance, "transactions": all_logs}