import sys
import os
# Add the cloned repo path so run_opt.py can be imported
BACKEND_PATH = os.environ.get("PYTOPO3D_BACKEND_PATH", "/opt/pytopo3d_backend")
sys.path.insert(0, BACKEND_PATH)
from run_opt import run_optimization_api

from fastapi import FastAPI, WebSocket, status
from fastapi.middleware.cors import CORSMiddleware
import requests
import threading
import queue
import numpy as np
import asyncio
import uuid
import time
from pathlib import Path
import logging
from logging.handlers import RotatingFileHandler
from pydantic import ValidationError
from schemas import OptimizationParams, Force
from typing import List
import json
import shutil
import subprocess

DEPLOYED = os.path.exists("/.dockerenv") #bool to allow for local testing prior to deployment

# cloudflare tunneling / verify token is in the environment variables wherever you deploy this
tunnel_token = os.getenv('TUNNEL_TOKEN') if DEPLOYED else None
middleman_token = os.getenv('MIDDLEMAN_TOKEN') if DEPLOYED else None
middleman_url = os.getenv('MIDDLEMAN_URL') if DEPLOYED else None
runner_url = os.getenv("RUNNER_URL") if DEPLOYED else None
vast_ai_id = os.getenv('VAST_CONTAINERLABEL') if DEPLOYED else None
if DEPLOYED and middleman_token is None:
    print("ERROR: MIDDLEMAN_TOKEN environment variable not set!")
    exit(1)

if DEPLOYED and middleman_url is None:
    print("ERROR: MIDDLEMAN_URL environment variable not set!")
    exit(1)

if DEPLOYED and runner_url is None:
    print("ERROR: RUNNER_URL environment variable not set!")
    exit(1)

if DEPLOYED and tunnel_token is None:
    print("ERROR: TUNNEL_TOKEN environment variable not set!")
    exit(1)
else:
    print(f"Token loaded successfully. Length: {len(tunnel_token)}")
    # The token is already in os.environ, so the child process gets it automatically.
    subprocess.Popen(["cloudflared", "tunnel", "run", "--url", "http://localhost:8000"])

if DEPLOYED and vast_ai_id is None:
    print("ERROR: VAST_CONTAINERLABEL environment variable not set!")
    exit(1)

JSON_DIR = "json_files"
RESULTS_DIR = "results"
CLEANUP_INTERVAL_SECONDS = 3600 #1 hour
FILE_AGE_LIMIT_SECONDS = 24 * 3600
MAX_VOXEL_CT = 64 * 64 * 64
MAX_JOB_LENGTH_SECONDS = 6 * 3600 #6 hours

os.makedirs(JSON_DIR, exist_ok=True)
os.makedirs(RESULTS_DIR, exist_ok=True)

#configure logging
log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

#create file handler that rotates logs when they reach 50mb
file_handler = RotatingFileHandler('backend.log', maxBytes=50 * 1024 * 1024, backupCount=2)
file_handler.setFormatter(logging.Formatter(log_format))

#get logger and add file handler
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(file_handler)

#also keep console output
console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter(log_format))
logger.addHandler(console_handler)

app = FastAPI()

@app.get("/")
async def root():
    return {"status": "ok", "service": "pytopo3d"}

@app.get("/health")
async def health():
    return {"status": "ok"}

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def cleanup_old_files():
    cleanup_files(JSON_DIR, "json")
    cleanup_results()

def cleanup_files(dir: str, ext: str):
    """Delete files in DIR that are older than FILE_AGE_LIMIT_SECONDS."""
    now = time.time()
    deleted_count = 0
    total_files = 0
    for file_path in Path(dir).glob(f"*.{ext}"):
        total_files += 1
        try:
            #get file modification time
            mtime = file_path.stat().st_mtime
            if now - mtime > FILE_AGE_LIMIT_SECONDS:
                Path(file_path).unlink()
                deleted_count += 1
        except Exception as e:
            logger.error(f"Error cleanup up {file_path}: {e}")
    logger.info(f"Cleanup: deleted {deleted_count} old *.{ext} files of {total_files} files")

def cleanup_results():
    """Delete results directories that are older than FILE_AGE_LIMIT_SECONDS."""
    now = time.time()
    deleted_count = 0
    total_dirs = 0
    children = list(Path(RESULTS_DIR).iterdir())
    for child in children:
        total_dirs += 1
        try:
            mtime = child.stat().st_mtime
            if now - mtime > FILE_AGE_LIMIT_SECONDS:
                shutil.rmtree(str(child))
                deleted_count += 1
        except Exception as e:
            logger.error(f"Error cleanup up {child}: {e}")
    logger.info(f"Cleanup: deleted {deleted_count} old result directories of {total_dirs} directories")


def run_cleanup_loop():
    """Background thread: runs cleanup periodically."""
    while True:
        time.sleep(CLEANUP_INTERVAL_SECONDS)
        cleanup_old_files()

#global state
waiting_queue = asyncio.Queue()

async def optimization_worker():
    """Background task that runs one optimization at a time."""
    while True:
        #wait for the next waiting future
        future, arg_list, obstacles_path, supports_path, forces_path, results_path, callback, stop_event, msg_queue, websocket = await waiting_queue.get()
        #notify the client that their optimization is starting
        try:
            await websocket.send_json({"status": "starting"})
        except:
            #client disconnected while waiting
            if obstacles_path is not None:
                Path(obstacles_path).unlink()
            if supports_path is not None:
                Path(supports_path).unlink()
            if forces_path is not None:
                Path(forces_path).unlink()
            future.set_result(True)  #allow the next in queue to start
            continue

        #run the actual optimization in a thread to avoid blocking the event loop
        def run_opt_blocking():
            result = run_optimization_api(arg_list, callback=callback, stop_event=stop_event)
            msg_queue.put(("complete", result))
        
        loop = asyncio.get_event_loop()
        thread = threading.Thread(target=run_opt_blocking)
        thread.start()
        start_time = time.time()

        #monitor the msg_queue for updates and send them to the client
        try:
            while True:
                if time.time() - start_time > MAX_JOB_LENGTH_SECONDS:
                    #gently force the optimization to complete
                    logger.info("job exceeded max time, stopping")
                    stop_event.set()
                    await websocket.send_json({"status": "stopping"})
                try:
                    typ, data = await asyncio.wait_for(loop.run_in_executor(None, msg_queue.get), timeout=3600)  #timeout to prevent hanging indefinitely
                except asyncio.TimeoutError:
                    logger.error("Optimization timed out")
                    break
                if typ == "iteration":
                    await websocket.send_json({"status": "change_update", "change": data[0]})
                    await websocket.send_bytes(quantize_density_field(data[1]))
                elif typ == "complete":
                    if os.path.exists(results_path):
                        with open(results_path, "rb") as f:
                            stl_data = f.read()
                            await websocket.send_json({"status": "complete", "has_stl": True})
                            await websocket.send_bytes(stl_data)
                    else:
                        await websocket.send_json({"status": "complete", "has_stl": False})
                    break
        except Exception as e:
            #client likely disconnected
            logger.error(f"WebSocket error: {e}")
            #signal the optimization to stop if possible
            if "stop_event" in arg_list:
                arg_list["stop_event"].set()
            pass
        finally:
            #cleanup
            thread.join()
            if obstacles_path is not None:
                Path(obstacles_path).unlink()
            if supports_path is not None:
                Path(supports_path).unlink()
            if forces_path is not None:
                Path(forces_path).unlink()
            try:
                if not websocket.client_state == WebSocketState.DISCONNECTED:
                    await websocket.close()
            except:
                pass
            #set the future result to allow the next in queue to start
            future.set_result(True)
            

@app.on_event("startup")
async def startup_event():
    # post URL to middleman
    if DEPLOYED:
        url = f"https://{middleman_url}/set-runner-url"
        payload = {
            "id": vast_ai_id,
            "url": runner_url
        }
        headers = {
            "Authorization": f"Bearer {middleman_token}"
        }
        try:
            response = requests.post(url, params=payload, headers=headers)
            response.raise_for_status()
            logger.info("Successfully posted URL to middleman")
        except requests.exceptions.HTTPError as err:
            print(f"request failed: {err}")
            if response and response.text:
                print("Server error details:", response.json())

    # Ensure upload directory exists
    # Run cleanup once immediately
    cleanup_old_files()
    #start cleanup background thread
    cleanup_thread = threading.Thread(target=run_cleanup_loop, daemon=True)
    cleanup_thread.start()
    #start the background worker
    asyncio.create_task(optimization_worker())

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):  
    # --- Secure Token Check ---
    if (DEPLOYED):
        auth = websocket.headers.get("authorization", "")
        if auth != f"Bearer {middleman_token}":
            logger.warning(f"Rejected unauthorized connection from {websocket.remote_address}")
            await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
            return
    # --------------------------
    
    await websocket.accept()
    #receive initial parameters (e.g., optimization settings) as JSON, converted to a dict
    try:
        raw_msg = await websocket.receive_json()
        await websocket.send_json({"status": "received"})
        # await websocket.send_json(params)
    except:
        logger.error("Error receiving parameters")
        await websocket.send_json({"status": "error", "message": "Error receiving parameters"})
        try:
            if not websocket.client_state == WebSocketState.DISCONNECTED:
                await websocket.close()
        except:
            pass

    try:
        o_p = OptimizationParams(**raw_msg) #strict type enforcing
    except ValidationError as e:
        m = "Invalid parameters"
        logger.warning(f"{m}")
        await websocket.send_json({"error": f"{m}"})
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
        return
    
    #convert validated input parameters into the list of arguments that pytopo3d expects
    arg_list = ["--gpu", "--no_visualization", "--export-stl", 
                "--smooth-iterations", "0", #do not use smoothing, scaling issues occur
                "--nelx", str(o_p.nelx), 
                "--nely", str(o_p.nely), 
                "--nelz", str(o_p.nelz), 
                "--volfrac", str(o_p.volfrac),
                "--penal", str(o_p.penal),
                "--rmin", str(o_p.rmin),
                "--tolx", str(o_p.tolx),
                "--maxloop", str(o_p.maxloop),
                #"--pitch", str(o_p.pitch)
                ]
    
    voxel_ct = o_p.nelx * o_p.nely * o_p.nelz
    if voxel_ct > MAX_VOXEL_CT:
        m = "Invalid parameters"
        logger.warning(f"{m}")
        await websocket.send_json({"error": f"{m}"})
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
        return
    
    obstacles_path = None
    supports_path = None
    forces_path = None
    id = str(uuid.uuid4()) #o_p.design_space_stl_id if o_p.design_space_stl_id is not None else str(uuid.uuid4())
    arg_list.append("--experiment-name")
    arg_list.append(id)
    results_path = os.path.join(RESULTS_DIR, id, "optimized_design.stl")
    if o_p.obstacles is not None:
        obstacles_path = save_mask_json("obstacles", id, o_p.obstacles)
        arg_list.append("--obstacle-config")
        arg_list.append(obstacles_path)
    if o_p.supports is not None:
        supports_path = save_mask_json("supports", id, o_p.supports)
        arg_list.append("--support-config")
        arg_list.append(supports_path)
    if o_p.forces is not None:
        forces_path = save_force_json("forces", id, o_p.forces)
        arg_list.append("--force-config")
        arg_list.append(forces_path)


    msg_queue = queue.Queue()
    stop_event = threading.Event()

    def callback(change, density):
        msg_queue.put(("iteration", (change, density.copy())))

    #create a future that will be resolved when this job is done
    future = asyncio.Future()

    #add to the queue along with all necessary data
    await waiting_queue.put((future, arg_list, obstacles_path, supports_path, forces_path, results_path, callback, stop_event, msg_queue, websocket))

    #send position in queue to client
    queue_position = waiting_queue.qsize() - 1
    await websocket.send_json({"status": "queued", "position": queue_position})

    async def listen_for_stop():
        try:
            while True:
                msg = await websocket.receive_json()
                if msg.get("command") == "stop":
                    stop_event.set()
                    await websocket.send_json({"status": "stopping"})
                    break
                else:
                    logger.warning("Invalid Command")
        except:
            #client disconnected while waiting
            stop_event.set()

    #start listening for stop commands in the background
    stop_listener = asyncio.create_task(listen_for_stop())

    #wait until the worker signals that this job is done (either completed or cancelled)
    try:
        await future
    except asyncio.CancelledError:
        #client disconnected while waiting
        stop_event.set()
        pass
    finally:
        #cancel the stop listener if it's still running
        stop_listener.cancel()
        #usually the websocket will already be closed by the worker, but just in case, try to close it here as well
        try:
            if not websocket.client_state == WebSocketState.DISCONNECTED:
                await websocket.close()
        except:
            pass
        pass

def save_mask_json(name:str, id:str, data:List[int]) -> str:
    data_entries = {"mask": data}
    path = os.path.join(JSON_DIR, f"{id}_{name}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data_entries, f, indent=2)
    return path

def save_force_json(name:str, id:str, data:List[Force]) -> str:
    data_entries = {"forces": [item.dict() for item in data]}
    path = os.path.join(JSON_DIR, f"{id}_{name}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data_entries, f, indent=2)
    return path

def quantize_density_field(density):
    """Converts the density field (shape: nely, nelx, nelz) (float32) to a row major order flat byte array for efficient transmission."""
    # convert to uint8 (0-255)
    return (density * 255).astype(np.uint8).tobytes()
