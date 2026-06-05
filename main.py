import sys
sys.path.insert(0, "../PyTopo3D_callbacks")
from run_opt import run_optimization_api
from fastapi import FastAPI, WebSocket
from fastapi.responses import HTMLResponse
import threading
import queue
import numpy as np
import asyncio

app = FastAPI()

#global state
waiting_queue = asyncio.Queue()
#active_futures = set()

async def optimization_worker():
    """Background task that runs one optimization at a time."""
    while True:
        #wait for the next waiting future
        future, params, callback, stop_event, msg_queue, websocket = await waiting_queue.get()
        #notify the client that their optimization is starting
        try:
            await websocket.send_json({"status": "starting"})
        except:
            #client disconnected while waiting
            future.set_result(True)  #allow the next in queue to start
            continue

        #run the actual optimization in a thread to avoid blocking the event loop
        def run_opt_blocking():
            result = run_optimization_api(params, callback=callback, stop_event=stop_event)
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
                    print("Optimization timed out")
                    break
                if typ == "iteration":
                    await websocket.send_bytes(quantize_density_field(data))
                elif typ == "complete":
                    await websocket.send_json({"status": "complete"})
                    break
        except Exception as e:
            #client likely disconnected
            print(f"WebSocket error: {e}")
            #signal the optimization to stop if possible
            if "stop_event" in params:
                params["stop_event"].set()
            pass
        finally:
            #cleanup
            thread.join()
            try:
                if not websocket.client_state == WebSocketState.DISCONNECTED:
                    await websocket.close()
            except:
                pass
            #set the future result to allow the next in queue to start
            future.set_result(True)
            

@app.on_event("startup")
async def startup_event():
    #start the background worker
    asyncio.create_task(optimization_worker())

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):  
    await websocket.accept()
    #receive initial parameters (e.g., optimization settings) as JSON, converted to a dict
    try:
        params = await websocket.receive_json()
        await websocket.send_json({"status": "received"})
        await websocket.send_json(params)
    except:
        await websocket.send_json({"status": "error", "message": "Error receiving parameters"})
        try:
            if not websocket.client_state == WebSocketState.DISCONNECTED:
                await websocket.close()
        except:
            pass
    
    msg_queue = queue.Queue()
    stop_event = threading.Event()

    def callback(density):
        msg_queue.put(("iteration", density.copy()))

    #create a future that will be resolved when this job is done
    future = asyncio.Future()

    #add to the queue along with all necessary data
    await waiting_queue.put((future, params, callback, stop_event, msg_queue, websocket))

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


def quantize_density_field(density):
    """Converts the density field (shape: nely, nelx, nelz) (float32) to a row major order flat byte array for efficient transmission."""
    # convert to uint8 (0-255)
    return (density * 255).astype(np.uint8).tobytes()

html = """
<!DOCTYPE html>
<html>
<head><title>WebSocket Tester</title></head>
<body>
    <button id="connect">Connect & Start</button>
    <button id="stop" disabled>Stop</button>
    <pre id="log"></pre>
    <script>
        let ws = null;
        const log = msg => document.getElementById('log').innerText += msg + "\\n";

        document.getElementById('connect').onclick = () => {
            log("Connecting...");
            ws = new WebSocket("ws://localhost:8000/ws");
            ws.onopen = () => {
                log("Connected");
                const params = { nelx: 32, nely: 16, nelz: 8, volfrac: 0.3, penal: 3.0, rmin: 1.5, disp_thres: 0.1, maxloop: 100, gpu: true };
                ws.send(JSON.stringify(params));
                document.getElementById('stop').disabled = false;
            };
            ws.onmessage = (e) => {
                if (typeof e.data === "string") log("JSON: " + e.data);
                else log("Binary data received (" + e.data.size + " bytes)");
            };
            ws.onclose = () => log("Disconnected");
        };
        document.getElementById('stop').onclick = () => {
            if (ws && ws.readyState === WebSocket.OPEN) {
                ws.send(JSON.stringify({ command: "stop" }));
                log("Stop command sent");
            }
        };
    </script>
</body>
</html>
"""
@app.get("/")
async def get():
    return HTMLResponse(html)
