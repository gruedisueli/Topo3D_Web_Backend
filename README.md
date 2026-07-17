# Topo3D_Web_Backend

This is the code that runs the backend server for the Topo3D project.

Below you can see the system architecture. This backend repository is the "uvicorn server" portion that runs on the GPU backend. You can find the optimizer code on the dev branch of my [fork of the PyTopo3D repository](https://github.com/gruedisueli/PyTopo3D_Backend/tree/dev).

![architecture_diagram](architecture.png)

## Installation and launching
If you would like to set up your own GPU-enabled service, this is the procedure you would follow. For the GPU-host, I found that Vast.ai was by far the most economical choice. There are others out there that manage SSL certificates, etc, but the process below will provide the encrypted connection to the backend without having to manually install certificates or find some service that will include them. 

### Buy a domain
You need your own domain for communicating with the backend over Cloudflare via secure Websocket. 

### Launch the front-end Github Page
1) Fork my [frontend repo](https://github.com/gruedisueli/Topo3D_Web_Frontend)
2) In src/composables/useOptimization.ts update the websocket target use your custom domain instead of mine.
3) Create your own Github page from this or host it somewhere. Refer to Vite documentation on deploying Github pages.

### Update the backend codebase 
1) Fork this repo.
2) In your fork, in main.py, modify ALLOWED_WEBSOCKET_ORIGINS to include your personal frontend URL.

### Set up Cloudflare
1) Create a Cloudflare account.
2) Install Cloudflared on your machine.  Refer to Cloudflare documentation for this procedure.
3) Create a new tunnel, and register your domain to this tunnel. Refer to Cloudflare documentation for this procedure.
4) Create a new tunnel for each backend runner you plan to create, noting the tunnel_token value

### Building the Docker image
1) Create a Docker Hub account
2) Set up Docker on your machine. Refer to Docker documentation for this procedure.
3) Build the image: ` docker build -t {username}/topo3d_web_backend:latest . `
4) Push the image: ` docker push {username}/topo3d_web_backend:latest `

### Set up VPS with orchestrator
1) Set up a VPS (I use Amazon Lightsail)
2) clone this repo
3) create a service that runs the orchestrator python script
4) Set up a cloudflare service as well, attached to the tunnel that the frontend talks to the orchestrator through.
   
### Setting up the backend
1) Create a Vast.ai account
2) Create an environment variables in your account --> settings --> environment variables:
   - MIDDLEMAN_URL = {your middleman/orchestrator's url}
   - MIDDLEMAN_TOKEN = {your secret key for communication between the middleman and backend}
   - IP_HASH_PEPPER = {your unique pepper for anonymizing IP addresses)
3) Create a base template from the default NVIDIA one
   - Set the container size to the minimum (8gb is more than enough)
   - Reference in your image from docker hub
   - For OPEN_BUTTON_PORT, set it to "8000"
   - add a variable: TUNNEL_TOKEN = {leave blank for now}
4) make sure your template is set to private since you are attaching secrets to it!
5) copy the base template and fill in the TUNNEL_TOKEN for each instance. Important: name the template the same as the subdomain of the tunnel referenced in TUNNEL_TOKEN. The orchestrator looks at the template name when it starts to determine the URL of each GPU instance.
6) Spin up instances for each runner you plan to use, each with its own template
   - I run on RTX 5060ti instances, but the key specs are min 12gb VRAM, min 32gb system RAM (number of CPU cores shouldn't really matter)
8) Once they are started, you can safely stop them

