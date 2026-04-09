import os
import time
import json
import threading
import psycopg2
import hazelcast
from fastapi import FastAPI

app = FastAPI()

HZ_MEMBERS = os.getenv("HZ_MEMBERS", "hazelcast:5701").split(",")
MQ_NAME = os.getenv("MQ_NAME", "transaction_queue")

def get_db_connection():
    return psycopg2.connect(
        host="postgres-db",
        database="bank_db",
        user="admin",
        password="password"
    )

def consume_queue():
    print(f"Connecting to Hazelcast cluster: {HZ_MEMBERS}")
    client = None

    while client is None:
        try:
            client = hazelcast.HazelcastClient(
                cluster_members=HZ_MEMBERS, 
                cluster_name="dev"
            )
        except Exception as e:
            print(f"Waiting for Hazelcast... {e}")
            time.sleep(2)

    queue = client.get_queue(MQ_NAME).blocking()
    print(f"Started consuming Hazelcast Queue: {MQ_NAME}...")
    
    while True:
        try:
            item = queue.take() 
            tx = json.loads(item)
            
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO balances (user_id, balance) 
                VALUES (%s, %s) 
                ON CONFLICT (user_id) 
                DO UPDATE SET balance = balances.balance + EXCLUDED.balance;
            """, (tx['user_id'], tx['amount']))
            conn.commit()
            cur.close()
            conn.close()
            
            print(f"Processed from MQ: {tx}")
        except Exception as e:
            print(f"MQ processing error: {e}")
            time.sleep(1)

@app.on_event("startup")
def startup():
    retries = 5
    while retries > 0:
        try:
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS balances (
                    user_id VARCHAR(50) PRIMARY KEY,
                    balance INTEGER NOT NULL DEFAULT 0
                )
            """)
            conn.commit()
            cur.close()
            conn.close()
            print("Successfully connected to PostgreSQL and ensured table exists.")
            break
        except Exception as e:
            print(f"Database not ready yet, retrying in 3 seconds... ({retries} left). Error: {e}")
            time.sleep(3)
            retries -= 1

    threading.Thread(target=consume_queue, daemon=True).start()

@app.get("/balance/{user_id}")
def get_balance(user_id: str):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT balance FROM balances WHERE user_id = %s", (user_id,))
        res = cur.fetchone()
        cur.close()
        conn.close()
        return {"balance": res[0] if res else 0}
    except Exception as e:
        print(f"Error fetching balance: {e}")
        # Вимога ЛР: Повертаємо null у випадку недоступності БД
        return {"balance": None}
