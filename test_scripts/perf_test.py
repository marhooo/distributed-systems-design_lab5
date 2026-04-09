import asyncio
import httpx
import time
import argparse

FACADE_URL = "http://192.168.49.2:30080"

async def client_task(client_id, user_id, request_count):
    async with httpx.AsyncClient(timeout=30.0) as client:
        for _ in range(request_count):
            try:
                await client.post(f"{FACADE_URL}/transaction", json={
                    "user_id": user_id,
                    "amount": 1
                })
            except Exception as e:
                print(f"Error: {e}")

async def run_scenario(scenario_type):
    start_time = time.time()
    tasks = []
    
    NUM_CLIENTS = 10
    REQUESTS_PER_CLIENT = 10000
    
    print(f"Starting Scenario {scenario_type} with {NUM_CLIENTS} clients, {REQUESTS_PER_CLIENT} reqs each...")

    for i in range(NUM_CLIENTS):
        user_id = f"user_{i}" if scenario_type == 1 else "user_test"
        tasks.append(client_task(i, user_id, REQUESTS_PER_CLIENT))

    await asyncio.gather(*tasks)
    
    total_time = time.time() - start_time
    total_requests = NUM_CLIENTS * REQUESTS_PER_CLIENT
    rps = total_requests / total_time
    
    print(f"\n--- Results for Scenario {scenario_type} ---")
    print(f"Total Time: {total_time:.2f}s")
    print(f"Total Requests: {total_requests}")
    print(f"RPS (Requests per Second): {rps:.2f}")

    async with httpx.AsyncClient() as client:
        resp = await client.get(f"{FACADE_URL}/accounts")
        print("Final Balances:", resp.json())
        
        stats = await client.get(f"{FACADE_URL}/stats")
        print("Internal Latency Stats:", stats.json())

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("scenario", type=int, choices=[1, 2], help="1 for unique users, 2 for single user")
    args = parser.parse_args()
    
    asyncio.run(run_scenario(args.scenario))
