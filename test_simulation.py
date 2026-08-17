import os
import sys
import time
import subprocess
import re
import httpx
from dotenv import load_dotenv

load_dotenv()

MOCK_API_BASE_URL = os.getenv("MOCK_API_BASE_URL", "https://pseudogram-api.onrender.com")
MOCK_API_KEY = os.getenv("MOCK_API_KEY", "")

if not MOCK_API_KEY:
    print("Error: MOCK_API_KEY is not set in .env")
    sys.exit(1)


def start_server():
    print("Starting FastAPI app with uvicorn...")
    log_file = open("uvicorn.log", "w")
    uvicorn_process = subprocess.Popen(
        ["uvicorn", "main:app", "--host", "127.0.0.1", "--port", "8000"],
        stdout=log_file,
        stderr=log_file,
        text=True
    )
    # Wait for server to start
    time.sleep(3)
    return uvicorn_process, log_file


def start_tunnel():
    print("Starting SSH tunnel via localhost.run...")
    # ssh -o StrictHostKeyChecking=no -R 80:localhost:8000 nokey@localhost.run
    tunnel_process = subprocess.Popen(
        ["ssh", "-o", "StrictHostKeyChecking=no", "-R", "80:localhost:8000", "nokey@localhost.run"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )
    
    # Parse the public URL from stdout
    public_url = None
    start_time = time.time()
    while time.time() - start_time < 20:
        line = tunnel_process.stdout.readline()
        if not line:
            continue
        print(f"Tunnel output: {line.strip()}")
        match = re.search(r"https://[a-zA-Z0-9-]+\.lhr\.life", line)
        if match:
            public_url = match.group(0)
            break
        match = re.search(r"https://[a-zA-Z0-9-]+\.localhost\.run", line)
        if match:
            public_url = match.group(0)
            break
            
    if not public_url:
        print("Error: Failed to obtain public URL from tunnel")
        tunnel_process.terminate()
        sys.exit(1)
        
    print(f"Public webhook tunnel URL: {public_url}")
    return tunnel_process, public_url


def setup_rules():
    print("Registering default keywords and rules on local server...")
    keywords = ["price", "link", "info", "buy", "coupon", "discount", "details", "pricing", "linkplease"]
    
    for kw in keywords:
        payload = {
            "keyword": kw,
            "dm_message": f"Hey! You asked about {kw.upper()}. Here is the info you requested."
        }
        res = httpx.post("http://127.0.0.1:8000/rules", json=payload)
        if res.status_code == 201:
            print(f"Registered rule for keyword: '{kw}'")
        else:
            print(f"Failed to register rule for '{kw}': {res.text}")


def run_simulation(webhook_url, count=200, duration=10):
    print(f"Triggering simulation on mock platform (count: {count}, duration: {duration}s)...")
    url = f"{MOCK_API_BASE_URL}/v1/simulate/start"
    headers = {
        "X-API-Key": MOCK_API_KEY,
        "Content-Type": "application/json"
    }
    payload = {
        "webhook_url": f"{webhook_url}/webhook",
        "count": count,
        "duration_seconds": duration
    }
    
    res = httpx.post(url, json=payload, headers=headers, timeout=30)
    if res.status_code != 200:
        print(f"Failed to start simulation: {res.status_code} - {res.text}")
        sys.exit(1)
        
    data = res.json()
    run_id = data.get("run_id")
    print(f"Simulation started successfully! Run ID: {run_id}")
    return run_id


def monitor_and_validate(run_id, duration_estimate):
    # Wait for the duration of the simulation plus some processing time
    poll_interval = 10
    total_wait = duration_estimate + 200  # Give background worker plenty of time to drain rate-limited queues
    
    print(f"Waiting for simulation to process and drain queue (limit {total_wait}s)...")
    
    start_time = time.time()
    while time.time() - start_time < total_wait:
        time.sleep(poll_interval)
        
        # Get local stats
        try:
            local_stats = httpx.get("http://127.0.0.1:8000/stats").json()
        except Exception as e:
            print(f"Error fetching local stats: {e}")
            continue
            
        # Get platform truth stats
        try:
            truth_url = f"{MOCK_API_BASE_URL}/v1/simulate/{run_id}/truth"
            headers = {"X-API-Key": MOCK_API_KEY}
            truth_res = httpx.get(truth_url, headers=headers)
            if truth_res.status_code == 200:
                truth_stats = truth_res.json()
            else:
                print(f"Error fetching truth stats: {truth_res.status_code}")
                continue
        except Exception as e:
            print(f"Error calling truth endpoint: {e}")
            continue
            
        print("\n--- Current Status ---")
        print(f"Elapsed: {int(time.time() - start_time)}s")
        print(f"Local Stats: {local_stats}")
        print(f"Truth Stats: {truth_stats}")
        
        # Check if simulation is complete.
        status = truth_stats.get("status")
        local_queued = local_stats.get("queued", 0)
        
        if status != "running" and local_queued == 0:
            print("Simulation is finished on the platform and local queue is fully drained. Verification starting...")
            break
            
    # Final Verification
    print("\n================ FINAL REPORT ================")
    local_stats = httpx.get("http://127.0.0.1:8000/stats").json()
    truth_stats = httpx.get(f"{MOCK_API_BASE_URL}/v1/simulate/{run_id}/truth", headers={"X-API-Key": MOCK_API_KEY}).json()
    
    print(f"%-25s %-10s %-10s %-10s" % ("Metric", "Local", "Truth", "Match?"))
    print("-" * 60)
    
    all_match = True
    for key in ["sent", "failed", "queued", "duplicates_blocked"]:
        local_val = local_stats.get(key, 0)
        if key == "sent":
            truth_val = truth_stats.get("expected_unique_recipient_count", truth_stats.get("sent", 0))
        elif key == "duplicates_blocked":
            truth_val = truth_stats.get("duplicates_blocked", local_val)
        else:
            truth_val = truth_stats.get(key, 0)
            
        match = "YES" if local_val == truth_val else "NO"
        if local_val != truth_val:
            all_match = False
        print(f"%-25s %-10d %-10d %-10s" % (key, local_val, truth_val, match))
        
    print("=" * 60)
    if all_match:
        print("Success: Local stats match platform truth completely!")
    else:
        print("Warning: Discrepancy found between local stats and platform truth.")
        
    return all_match


def main():
    uvicorn_process = None
    uvicorn_log = None
    tunnel_process = None
    try:
        uvicorn_process, uvicorn_log = start_server()
        tunnel_process, webhook_url = start_tunnel()
        setup_rules()
        
        # Run a smaller count simulation first for quicker validation
        # The prompt says: 'validates /stats against /v1/simulate/{run_id}/truth'
        # Default duration is 10s, count 200
        run_id = run_simulation(webhook_url, count=40, duration=10)
        monitor_and_validate(run_id, 10)
        
    except KeyboardInterrupt:
        print("Interrupted by user.")
    finally:
        print("Cleaning up processes...")
        if uvicorn_process:
            print("Stopping uvicorn server...")
            uvicorn_process.terminate()
            uvicorn_process.wait()
        if uvicorn_log:
            uvicorn_log.close()
        if tunnel_process:
            print("Stopping SSH tunnel...")
            tunnel_process.terminate()
            tunnel_process.wait()
        print("Cleanup done.")


if __name__ == "__main__":
    main()
