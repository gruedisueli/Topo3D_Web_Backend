import os
from fastapi import FastAPI, Depends, HTTPException, WebSocket, status
from fastapi.responses import JSONResponse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import websockets
import asyncio
import uuid
from collections import defaultdict, deque
import time
import logging
from logging.handlers import RotatingFileHandler
from pydantic import ValidationError
from schemas import OptimizationParams
import json
from vastai import VastAI
import httpx
from haship import hash_ip
from fastapi.middleware.cors import CORSMiddleware

class Session:
    def __init__(self, start_time=None, end_time=None, job_count = 0):
        self.start_time = start_time if start_time is not None else time.time()
        self.end_time = end_time
        self.job_count = job_count
    
    def end_session(self):
        self.end_time = time.time()

    def add_job(self):
        self.job_count += 1
    
    def to_dict(self):
        return {
            "start_time": self.start_time,
            "end_time": self.end_time,
            "job_count": self.job_count
        }
    
    @classmethod
    def from_dict(cls, data):
        return cls(data["start_time"], data["end_time"], data["job_count"])

class User:
    def __init__(self, past_sessions = None):
        self.past_sessions = past_sessions if past_sessions is not None else []
        self.current_sessions = {}
        self.current_runner_id = None  # there can only be one runner per user to keep resources allocated fairly

    def start_session(self, session_id, runner_id):
        if self.current_runner_id is None:
            self.current_runner_id = runner_id
        self.current_sessions[session_id] = Session()

    def finish_session(self, session_id):
        session = self.current_sessions.pop(session_id, None)
        if not session:
            logger.error("session id is not valid")
            return
        session.end_session()
        self.past_sessions.append(session)
        if len(self.current_sessions) == 0:
            self.current_runner_id = None

    def add_job(self, session_id):
        session = self.current_sessions.get(session_id)
        if not session:
            logger.error("session id is not valid")
            return
        session.add_job()

    def total_usage_time(self):
        now = time.time()
        return sum([session.end_time - session.start_time for session in self.past_sessions]) + sum([now - session.start_time for session in self.current_sessions.values()]) 
    

    def purge_old_sessions(self):
        """Clear out old session data"""
        earliest_allowed_time = time.time() - 30 * 24 * 3600  #only include sessions from the last month
        self.past_sessions = [session for session in self.past_sessions if session.start_time >= earliest_allowed_time]

    def is_stale(self):
        """Very old unused users can be removed"""
        return len(self.past_sessions) == 0 and len(self.current_sessions) == 0

    def to_dict(self):
        return {
            "past_sessions": [session.to_dict() for session in self.past_sessions]
        }
    
    @classmethod
    def from_dict(cls, data):
        sessions = data["past_sessions"]
        past_sessions = [Session.from_dict(s) for s in sessions]
        return cls(past_sessions)

        
class Runner:
    wait_seconds = 3
    timeout_seconds = 300

    def __init__(self, cumulative_job_count = 0, up_time = 0):
        self.url = None
        self.is_running = False
        self.cumulative_job_count = cumulative_job_count
        self.up_time = up_time
        self.current_user_count = 0
        self.is_idle = False
        self.idle_start_time = 0

    def set_url(self, url):
        if self.url == url:
            return
        if self.url is not None:
            logger.error(f"Cannot change the URL of a runner once it exists. Attempted to change {self.url} to {url}")
            return
        self.url = url

    async def url_wait_loop(self):
        while self.url is None:
            await asyncio.sleep(1)

    async def start(self, id) -> bool:
        start_result = vast.start_instance(id=id)
        logger.info(f'Attempted to start {id}, result: {start_result}')
        if not start_result.get("success"):
            logger.info("Host busy. Stopping instance...")
            vast.stop_instance(id=id)
            return False
        boot_time = time.time()
        started = False

        #wait for URL to be set by backend (via POST on middleman)
        try:
            await asyncio.wait_for(self.url_wait_loop(), timeout=self.timeout_seconds)
        except:
            logger.error(f"Instance {id} failed to register its URL to middleman")
            vast.stop_instance(id=id)
            return False

        #final health check on backend
        while time.time() - boot_time < self.timeout_seconds:
            if await isBackendReady(self.url):
                started = True
                break
            await asyncio.sleep(self.wait_seconds)
        if not started:
            logger.error(f'failed to start instance {id}')
            vast.stop_instance(id=id)
            return False
        logger.info(f'Successfully started instance {id}')

        self.is_running = True
        self.start_time = time.time()
        return True

    def mark_started_and_idle(self):
        self.is_running = True
        self.is_idle = True
        self.idle_start_time = time.time()
        self.start_time = self.idle_start_time
        
    def stop(self):
        self.is_running = False
        self.is_idle = False
        self.up_time += time.time() - self.start_time
        vast.stop_instance(id=self.id)
    
    def add_user(self):
        self.current_user_count += 1
        self.is_idle = False

    def remove_user(self):
        if self.current_user_count == 0:
            logger.error("cannot remove a user if there are none on the runner")
            return
        self.current_user_count -= 1
        if self.current_user_count == 0:
            self.is_idle = True
            self.idle_start_time = time.time()

    def add_job(self):
        self.cumulative_job_count += 1

    def idle_time(self):
        return time.time() - self.idle_start_time if self.is_idle else 0

    def to_dict(self):
        return {
            "cumulative_job_count": self.cumulative_job_count,
            "up_time": self.up_time
        }
    
    @classmethod
    def from_dict(cls, data):
        return cls(data["cumulative_job_count"], data["up_time"])

DATA_DIR = "log_files"
USER_LOG = "users.json"
RUNNER_LOG = "runners.json"
RUNNERS_COUNT = 5
RUNNER_MAX_IDLE_SECONDS = 900
RUNNER_CHECK_INTERVAL_SECONDS = 60
SAVE_INTERVAL_SECONDS = 900 #15 minutes
USER_PURGE_INTERVAL_SECONDS = 24 * 3600 #24 hours
USER_MAX_USAGE_SECONDS_PER_PERIOD = 24 * 3600
USER_MAX_SESSION_LENGTH = 3 * 3600
IMAGE_UUID = 'gruedi/topo3d_web_backend_with_lightsail:latest'

#configure logging
log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

#create file handler that rotates logs when they reach 50mb
file_handler = RotatingFileHandler(f'{DATA_DIR}/backend.log', maxBytes=50 * 1024 * 1024, backupCount=2)
file_handler.setFormatter(logging.Formatter(log_format))

#get logger and add file handler
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(file_handler)

#also keep console output
console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter(log_format))
logger.addHandler(console_handler)

os.makedirs(DATA_DIR, exist_ok=True)
users_path = os.path.join(DATA_DIR, USER_LOG)
runners_path = os.path.join(DATA_DIR, RUNNER_LOG)
users = {}
runners = {}
if os.path.exists(users_path):
    #load from history
    try:
        with open(users_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
            for key, inner_obj in data.items():
                users[key] = User.from_dict(inner_obj)
    except Exception as e:
        logger.error(f"Failed to load user file: {e}")

if os.path.exists(runners_path):
    #load from history
    try:
        with open(runners_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
            for key, inner_obj in data.items():
                runners[key] = Runner.from_dict(inner_obj)
    except Exception as e:
        logger.error(f"Failed to load runners file: {e}")

middleman_token = os.getenv('MIDDLEMAN_TOKEN')
if middleman_token is None:
    logger.error("Middleman token is not set")
    exit(1)

# Define the origins you want to allow from frontend
allowed_origins = [os.getenv('ALLOWED_WEBSOCKET_ORIGIN')]
if allowed_origins[0] is None:
    logger.error("Error: could not locate the environment variable for allowed websocket origin")
    allowed_origins.pop(0) 
    #don't exit: allow continuing for local testing
allowed_origins.append("http://localhost:5173")#for local testing


vast_api_key = os.getenv('VAST_AI_API_KEY')
if vast_api_key is None:
    logger.error("Error: environment variable for vastAI API key is not set")
    exit(1)

try:
    vast = VastAI(api_key=vast_api_key)
except Exception as e:
    logger.error(f"Error: instantiating VastAI failed: {e}")

#update statuses of runners / instantiate them if they don't already exist in records
all_instances = vast.show_instances()
for instance in all_instances:
    image = instance.get("image_uuid")
    if not image == IMAGE_UUID:
        #ignore instances with other images on them
        continue
    id = instance.get("id")
    if id is None:
        logger.error("Could not get instance ID")
        continue
    runner = runners.get(id)
    if runner is None:
        runner = Runner()
        runners[id] = runner

    instance_status = instance.get("actual_status")
    instance_current_state = instance.get("cur_state")

    logger.info(f"Instance {id} status: {instance_status}, state: {instance_current_state}")
    if instance_status != "stopped" and instance_current_state != "stopped":
        runner.mark_started_and_idle()

connection_attempts = defaultdict(list)#dictionary of hashed IP addresses with a list of timestamps for connection time
command_timestamps = defaultdict(lambda: deque(maxlen=10)) #only store last 10 timestamps

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,  
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

security = HTTPBearer()


async def run_runner_check_loop():
    """Stops idle instances."""
    while True:
        await asyncio.sleep(RUNNER_CHECK_INTERVAL_SECONDS)
        stop_idle_runners()

def stop_idle_runners():
    for id, runner in runners.items():
        if not runner.is_running or not runner.is_idle:
            continue
        if runner.idle_time() < RUNNER_MAX_IDLE_SECONDS:
            continue
        logger.info(f"Shutting down idle instance: {id}")
        runner.stop()

async def run_user_purge_loop():
    """Purges old user data periodically."""
    while True:
        await asyncio.sleep(USER_PURGE_INTERVAL_SECONDS)
        purge_users()
        purge_old_connection_attempts()

def purge_users():
    to_remove = []
    for id, user in list(users.items()):
        user.purge_old_sessions()
        if user.is_stale():
            to_remove.append(id)
    for id in to_remove:
        users.pop(id)
    logger.info(f"Purged {len(to_remove)} old users")

async def run_save_loop():
    """Saves logs periodically."""
    while True:
        await asyncio.sleep(SAVE_INTERVAL_SECONDS)
        save_logs()

def save_logs():
    users_dict = {key: value.to_dict() for key, value in users.items()}
    runners_dict = {key: value.to_dict() for key, value in runners.items()}
    with open(users_path, "w", encoding="utf-8") as file:
        json.dump(users_dict, file, indent=4)
    with open(runners_path, "w", encoding="utf-8") as file:
        json.dump(runners_dict, file, indent=4)
    logger.info("Saved log files")

def can_connect(ip_hash: str, max_attempts: int = 5, window_seconds: int= 60) -> bool:
    now = time.time()
    #keep only those attempts within sliding window
    attempts = [t for t in connection_attempts[ip_hash] if now - t < window_seconds]
    if len(attempts) > max_attempts:
        return False
    attempts.append(now)
    connection_attempts[ip_hash] = attempts
    return True

def purge_old_connection_attempts():
    """Periodically clean up stale connection tracking data"""
    now = time.time()
    stale_ips = []
    for ip_hash, attempts in connection_attempts.items():
        attempts[:] = [t for t in attempts if now - t < 3600]  # keep only last hour
        if not attempts:
            stale_ips.append(ip_hash)
    for ip in stale_ips:
        del connection_attempts[ip]


async def isBackendReady(backend_url) -> bool:
    async with httpx.AsyncClient() as client:
        try:
            r = await client.get(f'https://{backend_url}/health')
            try:
                data = r.json()
                return data.get("status") == "ok"
            except (json.JSONDecodeError, ValueError):
                return False
        except httpx.RequestError:
            return False
        
def verify_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
    token = credentials.credentials
    if token != middleman_token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return token

@app.get("/")
async def root():
    return {"status": "ok", "service": "topo3d_orchestrator"}

@app.get("/runners")
async def get_runners_data():
    out_dict = {}
    total_jobs = 0
    total_up_time = 0
    for id, runner in runners.items():
        job_ct = runner.cumulative_job_count
        up_time = runner.up_time
        total_jobs += job_ct
        total_up_time += up_time
        out_dict[f"runner_{id}"] = {
            "is_running": runner.is_running,
            "is_idle": runner.is_idle,
            "idle_time": runner.idle_time(),
            "cumulative_job_count": job_ct,
            "up_time": up_time,
            "current_user_count": runner.current_user_count
        }
    out_dict["total_cumulative_jobs"] = total_jobs
    out_dict["total_up_time"] = total_up_time
    pretty_string = json.dumps(out_dict, indent=4)
    return JSONResponse(content=json.loads(pretty_string))

@app.post("/set-runner-url", status_code=status.HTTP_202_ACCEPTED)
def set_runner_url(id: str, url: str, token: str = Depends(verify_token)):
    clean_id = ''.join(char for char in id if char.isdigit())
    #strip non-numerals from ID eg "C.123456" becomes 123456
    runner = runners.get(clean_id)
    if runner is None:
        runner = Runner()
        runners[clean_id] = runner
    runner.set_url(url)
    logger.info(f"Set URL={url} on runner {clean_id}")
        
@app.on_event("startup")
async def startup_event():
    asyncio.create_task(run_save_loop())
    asyncio.create_task(run_user_purge_loop())
    asyncio.create_task(run_runner_check_loop())
    logger.info("Application started")

@app.websocket("/ws")
async def websocket_endpoint(client_websocket: WebSocket):  
    #check connection attempts for this IP address
    logger.info("received connection request")
    client_ip_hash = hash_ip(client_websocket.client.host)
    if not can_connect(client_ip_hash):
        logger.warning(f"Rejected connection from {client_ip_hash}")
        await client_websocket.close(code=status.WS_1008_POLICY_VIOLATION)
        return
    
    # --- Secure Origin Check ---
    origin = client_websocket.headers.get("origin")
    if not origin or origin not in allowed_origins:
        logger.warning(f"Rejected WebSocket connection from unauthorized origin: {origin}")
        # Close the connection before accepting it
        await client_websocket.close(code=status.WS_1008_POLICY_VIOLATION)
        return
    # --------------------------
    
    logger.info(f"accepting connection with {client_ip_hash}")
    await client_websocket.accept()
    logger.info(f"connection to {client_ip_hash} established")

    if not users.get(client_ip_hash):
        users[client_ip_hash] = User()
        logger.info(f"added new user {client_ip_hash}")
    user = users[client_ip_hash]
    logger.info(f"user {client_ip_hash} is registered")

    if user.total_usage_time() > USER_MAX_USAGE_SECONDS_PER_PERIOD:
        logger.info(f"user {client_ip_hash} exceeded allowed usage time")
        await client_websocket.send_json({"status": "exceeded_limit"})
        await client_websocket.close()
        return

    backend_id = ''
    logger.info(f"allocating gpu resources for user {client_ip_hash}")
    logger.info(f"user {client_ip_hash} has {len(user.current_sessions)} current sessions")
    if len(user.current_sessions) == 0:
        found = False
        idle_id = next((id for id in runners.keys() if runners[id].is_idle), None)
        if idle_id is not None:
            logger.info("Found existing started instance with no active users")
            backend_id = idle_id
            found = True
            runners[idle_id].add_user()
        else:
            for id, r in runners.items():
                if r.is_running:
                    continue
                if not await r.start(id):
                    continue
                backend_id = id
                r.add_user()
                found = True
                break
        if not found:
            if not any(runners[k].is_running for k in list(runners.keys())):
                logger.warning('no instances are startable')
                await client_websocket.send_json({"status": "busy"})
                await client_websocket.close()
                return
            backend_id = min((k for k, v in runners.items() if v.is_running), key=lambda k: runners[k].current_user_count)
            runners[backend_id].add_user()
    else:
        logger.info(f"user {client_ip_hash} has existing session, using same gpu")
        backend_id = user.current_runner_id
    await client_websocket.send_json({"status": "connected"})
    
    logger.info("Backend started or already running")
    session_id = str(uuid.uuid4())
    user.start_session(session_id, backend_id)
    logger.info(f"started session {session_id}")
    session_start_time = time.time()

    async def listen_for_client_msgs():
        backend_websocket = None
        backend_listener = None
        async def listen_for_backend_msgs(): 
            nonlocal backend_websocket
            try:
                if backend_websocket is None:
                    logger.error("Backend websocket does not exist")
                    return
                expecting_stl = False
                while True:
                    msg = await backend_websocket.recv()
                    #detect if frame indicates a string or bytes
                    if isinstance(msg, str):
                        try:
                            data = json.loads(msg)
                        except json.JSONDecodeError:
                            logger.error("Got TEXT but not JSON: %s", msg)
                            continue
                        logger.info("sending metrics data to client")
                        await client_websocket.send_json(data)
                        s = data.get("status")
                        if s == "complete":
                            expecting_stl = data.get("has_stl")
                            if not expecting_stl:
                                await backend_websocket.close()
                                backend_websocket = None
                                return
                    elif isinstance(msg, bytes):
                        if expecting_stl:
                            logger.info("sending final STL to client")
                            await client_websocket.send_bytes(msg)
                            await backend_websocket.close()
                            backend_websocket = None
                            return
                        logger.info("sending iteration data to client")
                        await client_websocket.send_bytes(msg)
            except Exception as e:
                logger.info(f"client or backend disconnected ({e})")
                #await client_websocket.close()
                await backend_websocket.close()
                backend_websocket = None
                return
        
        async def close_backend():
            nonlocal backend_listener
            nonlocal backend_websocket
            if backend_listener:
                backend_listener.cancel()
                backend_listener = None
            if backend_websocket:
                await backend_websocket.close()
                backend_websocket = None
            return

        stop_msg_sent = False
        try:
            while True:
                if not backend_listener or (not backend_listener.done() and not stop_msg_sent):
                    try:
                        msg = await asyncio.wait_for(client_websocket.receive_json(), timeout=300) #timeout for periodic user status checks
                    except asyncio.TimeoutError:
                        #check session length, cull idle users
                        if time.time() - session_start_time > USER_MAX_SESSION_LENGTH:
                            logger.info(f"session length exceeded for {client_ip_hash}")
                            await close_backend()
                            return
                        continue #continue listening for messages
                else:
                    try:
                        await asyncio.wait_for(backend_listener, timeout=300)
                    except asyncio.TimeoutError:
                        logger.warning(f"{client_ip_hash}: backend listener timed out after stop")
                    await close_backend()
                    stop_msg_sent = False
                    continue #keep main websocket open and listening for a new job
                
                # #validate and sanitize incoming message
                if msg.get("command") == "start" and msg.get("data"):
                    logger.info("received start command")
                    await close_backend() #extra check in case backend is still open for some reason from a previous run.
                    #prevent indefinitely long sessions
                    if time.time() - session_start_time > USER_MAX_SESSION_LENGTH:
                        logger.info(f"session length exceeded for {client_ip_hash}")
                        return
                    
                    data = msg.get("data")
                    try:
                        o_p = OptimizationParams(**data) #strict type enforcing
                    except ValidationError as e:
                        logger.warning(f"{client_ip_hash}:Invalid parameters")
                        return
                    
                    #create a new connection to the backend for each optimization
                    try:
                        backend_url = runners[backend_id].url
                        backend_websocket = await websockets.connect(f"wss://{backend_url}/ws", additional_headers={"Authorization": f"Bearer {middleman_token}"})
                        backend_listener = asyncio.create_task(listen_for_backend_msgs())
                        logger.info(f"Connected to backend at {backend_url}")
                    except Exception as e:
                        logger.error(f"Failed to connect to backend: {e}")
                        await client_websocket.send_json({"status": "error"})
                        await close_backend()
                        return
                    logger.info("sending job to backend")
                    await backend_websocket.send(json.dumps(data))
                    runners[backend_id].add_job()
                    user.add_job(session_id)
                elif msg.get("command") == "stop":
                    logger.info("stop message received")
                    now = time.time()
                    #only allow 1 stop command every 5 seconds
                    cmd_history = command_timestamps[client_ip_hash]
                    if cmd_history and now - cmd_history[-1] < 5:
                        m = "Rate limit exceeded, try again later"
                        logger.warning(f"{client_ip_hash}: {m}")
                        #await client_websocket.send_json({"error": m})
                        continue
                    cmd_history.append(now)
                    if backend_websocket is None:
                        logger.warning(f"{client_ip_hash}: stop command received before any backend connection")
                        continue
                    logger.info("sending stop message to backend")
                    await backend_websocket.send(json.dumps(msg))
                    stop_msg_sent = True
                    #keep the frontend listener active because it contains the backend listener...
                    # the backend must send it's final data after stopping
                else:
                    logger.warning(f"{client_ip_hash}:Invalid Command")
        except Exception as e:
            logger.info(f"client or backend disconnected ({e})")
            await close_backend()
            return

    #start listening for messages
    await listen_for_client_msgs()

    #clean up
    rnr = runners[backend_id]
    rnr.remove_user() #keep instance hot for some time after they leave (allow instant reconnecting if they accidentally closed window, eg)
    user.finish_session(session_id)
    logger.info(f"ended session {session_id}")

    await client_websocket.close()

