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

### Building the Docker image
1) Create a Docker Hub account
2) Set up Docker on your machine. Refer to Docker documentation for this procedure.
3) Build the image: ` docker build -t {username}/topo3d_web_backend:latest . `
4) Push the image: ` docker push {username}/topo3d_web_backend:latest `
   
### Launching a Server
1) Create a Vast.ai account
2) Create an environment variable in your account --> settings --> environment variables: Key = TUNNEL_TOKEN Value = {your tunnel ID}
3) Spawn an instance on Vast.AI using the following template (change the image field to reference your own docker image, hosted on docker hub): https://cloud.vast.ai?ref_id=590682&template_id=32e61f3568510c7ddd28f7917c05e689

