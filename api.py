import asyncio
import json
import os
import subprocess
import uuid
import time
from fastapi import FastAPI, BackgroundTasks, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
import pandas as pd

app = FastAPI()

# Enable CORS for local development
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global store for process output: {job_id: [logs]}
job_logs = {}
# Global store for job status: {job_id: status}
job_status = {}

import sys

async def run_scraper(job_id: str, query: str, max_results: int, out_file: str):
    job_logs[job_id] = [f"Starting search for: {query}..."]
    job_status[job_id] = "running"
    
    cmd = [
        sys.executable, "scraper.py", 
        query, 
        "--max", str(max_results), 
        "--out", out_file
    ]
    
    try:
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT
        )

        while True:
            line = await process.stdout.readline() # type: ignore
            if not line:
                break
            msg = line.decode().strip()
            if msg:
                job_logs[job_id].append(msg)
                print(f"[{job_id}] {msg}")
        
        await process.wait()
        job_logs[job_id].append(f"Scrape completed. Saved to {out_file}")
        job_status[job_id] = "completed"
    except Exception as e:
        error_msg = f"Process Error: {str(e)}"
        job_logs[job_id].append(error_msg)
        job_status[job_id] = "failed"

@app.get("/logs/{job_id}")
async def get_logs(job_id: str):
    if job_id not in job_logs:
        raise HTTPException(status_code=404, detail="Job not found")

    async def event_stream():
        last_idx = 0
        while True:
            if last_idx < len(job_logs[job_id]):
                for i in range(last_idx, len(job_logs[job_id])):
                    yield f"data: {job_logs[job_id][i]}\n\n"
                last_idx = len(job_logs[job_id])
            
            if job_status.get(job_id) in ["completed", "failed"]:
                # Send one last check to ensure all logs were captured
                break
            
            await asyncio.sleep(0.5)
            
    return StreamingResponse(event_stream(), media_type="text/event-stream")

@app.post("/scrape")
async def trigger_scrape(
    query: str = Query(..., min_length=1, max_length=100), 
    max_results: int = Query(10, gt=0, le=50), 
    background_tasks: BackgroundTasks = None
):
    job_id = str(uuid.uuid4())[:8]
    out_file = f"results_{job_id}.csv"
    background_tasks.add_task(run_scraper, job_id, query, max_results, out_file)
    return {"status": "started", "job_id": job_id, "file": out_file}

@app.get("/results/{job_id}")
async def get_results(job_id: str):
    # Try to find the file associated with this job
    file_path = f"results_{job_id}.csv"
    
    if not os.path.exists(file_path):
        # Fallback to dashboard_results if requested without ID (for legacy support if needed)
        if job_id == "latest":
            file_path = "dashboard_results.csv"
        else:
            raise HTTPException(status_code=404, detail="Results file not found")
    
    try:
        if os.path.getsize(file_path) < 10:
             return {"error": "Results file exists but appears empty."}
             
        df = pd.read_csv(file_path)
        df = df.fillna("N/A")
        return df.to_dict(orient="records")
    except Exception as e:
        return {"error": f"Error reading results: {str(e)}"}

@app.get("/jobs")
async def list_jobs():
    return {jid: status for jid, status in job_status.items()}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
