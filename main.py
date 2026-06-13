import sys
sys.path.insert(0, "../PyTopo3D_callbacks")
from run_opt import run_optimization_api
from fastapi import FastAPI, WebSocket, UploadFile, File, HTTPException, status, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import threading
import queue
import numpy as np
import asyncio
import os
import uuid
from collections import defaultdict, deque
import time
from pathlib import Path
import logging
from logging.handlers import RotatingFileHandler
from pydantic import ValidationError
from schemas import OptimizationParams, Force
from typing import List
import json
import shutil

# Define the origins you want to allow (e.g., your frontend URL)
ALLOWED_WEBSOCKET_ORIGINS = [
    "http://localhost:5173",   # Your local dev environment
    "https://yourusername.github.io" # Your production GitHub Pages URL
]


connection_attempts = defaultdict(list)#dictionary of IP addresses with a list of timestamps for connection time
command_timestamps = defaultdict(lambda: deque(maxLen=10)) #only store last 10 timestamps
http_timestamps = defaultdict(lambda: deque(maxLen=10)) #only store last 10 timestamps
uploads = defaultdict(list) #uploads by ip address

MAX_UPLOADS = 50 #max saved uploads per ip address
MAX_UPLOAD_SIZE = 10_000_000
UPLOAD_DIR = "stl_uploads"
JSON_DIR = "json_files"
RESULTS_DIR = "results"
CLEANUP_INTERVAL_SECONDS = 3600 #1 hour
FILE_AGE_LIMIT_SECONDS = 24 * 3600

os.makedirs(UPLOAD_DIR, exist_ok=True)
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

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],   # update once deployed
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def cleanup_old_files():
    cleanup_files(UPLOAD_DIR, "stl")
    cleanup_files(JSON_DIR, "json")
    cleanup_results()

def cleanup_files(dir: str, ext: str):
    """Delete files in UPLOAD_DIR that are older than FILE_AGE_LIMIT_SECONDS."""
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

def can_connect(ip: str, max_attempts: int = 5, window_seconds: int= 60) -> bool:
    now = time.time()
    #keep only those attempts within sliding window
    attempts = [t for t in connection_attempts[ip] if now - t < window_seconds]
    if len(attempts) > max_attempts:
        return False
    attempts.append(now)
    connection_attempts[ip] = attempts
    return True

@app.post("/upload-stl")
async def upload_stl(request: Request, file: UploadFile = File(...)):
    #check last upload time and deny if too recent
    now = time.time()
    client_ip = request.client.host
    upload_history = http_timestamps[client_ip]
    if upload_history and now - upload_history[-1] < 10: #only allow one upload every 10 seconds
        m = "Too many requests. Try again later"
        logger.warning(f"{client_ip}: {m}")
        raise HTTPException(400, m)
    upload_history.append(now)

    if not file.filename.endswith('.stl'):
        m = "Invalid file type. Only .stl files are allowed"
        logger.warning(f"{client_ip}: {m}")
        raise HTTPException(400, m)
    
    #chunked reading of file size (file.size is not a reliable measure as it is a claim by the client)
    contents = b''
    while chunk := await file.read(8192):
        contents += chunk
        if len(contents) > MAX_UPLOAD_SIZE:
            m = "File too large (max 10MB size)"
            logger.warning(f"{client_ip}: {m}")
            raise HTTPException(status_code=413, detail = m)
    
    # #check header of STL (heuristic, optional for now)
    # if not (contents.startswith(b'solid') or len(contents)> 80):

    try:
        #generate unique ID and a secure temporary file path
        stl_id = str(uuid.uuid4())
        file_path = os.path.join(UPLOAD_DIR, f"{stl_id}.stl")

        #save file
        with open(file_path, "wb") as buffer:
            content = await file.read()
            buffer.write(content)
            #delete old uploads that exceed the number allowed
            if len(uploads[client_ip]) >= MAX_UPLOADS:
                to_del = uploads[client_ip].pop(0)
                if os.path.exists(to_del):
                    os.remove(to_del)
            uploads[client_ip].append(file_path)
            return JSONResponse(content={"stl_id" : stl_id})
    except Exception as e:
        m = "{client_ip}: Could not save file."
        logger.error(m)
        raise HTTPException(500, m)
    
#global state
waiting_queue = asyncio.Queue()
#active_futures = set()

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

        #monitor the msg_queue for updates and send them to the client
        try:
            while True:
                try:
                    typ, data = await asyncio.wait_for(loop.run_in_executor(None, msg_queue.get), timeout=3600)  #timeout to prevent hanging indefinitely
                except asyncio.TimeoutError:
                    logger.error("Optimization timed out")
                    break
                if typ == "iteration":
                    await websocket.send_bytes(quantize_density_field(data))
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
    # Ensure upload directory exists
    os.makedirs(UPLOAD_DIR, exist_ok=True)
    # Run cleanup once immediately
    cleanup_old_files()
    #start cleanup background thread
    cleanup_thread = threading.Thread(target=run_cleanup_loop, daemon=True)
    cleanup_thread.start()
    #start the background worker
    asyncio.create_task(optimization_worker())

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):  
    #check connection attempts for this IP address
    client_ip = websocket.client.host
    if not can_connect(client_ip):
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
    
    # --- Secure Origin Check ---
    origin = websocket.headers.get("origin")
    if not origin or origin not in ALLOWED_WEBSOCKET_ORIGINS:
        logger.warning(f"Rejected WebSocket connection from unauthorized origin: {origin}")
        # Close the connection before accepting it
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
    
    #validate and sanitize incoming message
    if len(raw_msg) > 1024:#reject large messages
        m = "message too large"
        logger.warning(f"{client_ip}:{m}")
        await websocket.send_json({"error": {m}})
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
        return

    try:
        o_p = OptimizationParams(**raw_msg) #strict type enforcing
    except ValidationError as e:
        m = "Invalid parameters"
        logger.warning(f"{client_ip}:{m}:{e.errors()}")
        await websocket.send_json({"error": f"{m}"})
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
        return
    
    #convert validated input parameters into the list of arguments that pytopo3d expects
    arg_list = ["--gpu", "--no_visualization", "--export-stl", 
                "--nelx", str(o_p.nelx), 
                "--nely", str(o_p.nely), 
                "--nelz", str(o_p.nelz), 
                "--volfrac", str(o_p.volfrac),
                "--penal", str(o_p.penal),
                "--rmin", str(o_p.rmin),
                "--tolx", str(o_p.tolx),
                "--maxloop", str(o_p.maxloop),
                "--pitch", str(o_p.pitch)
                ]
    
    if o_p.invert_design_space:
        arg_list.append("--invert-design-space")
    if o_p.design_space_stl_id is not None:
        arg_list.append("--design-space-stl")
        stl_path = os.path.join(UPLOAD_DIR, f"{o_p.design_space_stl_id}.stl")
        arg_list.append(stl_path)
    obstacles_path = None
    supports_path = None
    forces_path = None
    id = o_p.design_space_stl_id if o_p.design_space_stl_id is not None else str(uuid.uuid4())
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

    def callback(density):
        msg_queue.put(("iteration", density.copy()))

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
                    now = time.time()
                    #only allow 1 stop command every 5 seconds
                    cmd_history = command_timestamps[client_ip]
                    if cmd_history and now - cmd_history[-1] < 5:
                        m = "Rate limit exceeded, try again later"
                        logger.warning(f"{client_ip}: m")
                        await websocket.send_json({"error": m})
                        continue
                    cmd_history.append(now)
                    stop_event.set()
                    await websocket.send_json({"status": "stopping"})
                    break
                else:
                    logger.warning(f"{client_ip}:Invalid Command")
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

# html = """
# <!DOCTYPE html>
# <html>
# <head><title>WebSocket Tester</title></head>
# <body>
#     <button id="connect">Connect & Start</button>
#     <button id="stop" disabled>Stop</button>
#     <pre id="log"></pre>
#     <script>
#         let ws = null;
#         const log = msg => document.getElementById('log').innerText += msg + "\\n";

#         document.getElementById('connect').onclick = () => {
#             log("Connecting...");
#             ws = new WebSocket("ws://localhost:8000/ws");
#             ws.onopen = () => {
#                 log("Connected");
#                 const params = { nelx: 32, nely: 16, nelz: 8, volfrac: 0.3, penal: 3.0, rmin: 1.5, disp_thres: 0.1, maxloop: 100, gpu: true };
#                 ws.send(JSON.stringify(params));
#                 document.getElementById('stop').disabled = false;
#             };
#             ws.onmessage = (e) => {
#                 if (typeof e.data === "string") log("JSON: " + e.data);
#                 else log("Binary data received (" + e.data.size + " bytes)");
#             };
#             ws.onclose = () => log("Disconnected");
#         };
#         document.getElementById('stop').onclick = () => {
#             if (ws && ws.readyState === WebSocket.OPEN) {
#                 ws.send(JSON.stringify({ command: "stop" }));
#                 log("Stop command sent");
#             }
#         };
#     </script>
# </body>
# </html>
# """
# @app.get("/")
# async def get():
#     return HTMLResponse(html)
