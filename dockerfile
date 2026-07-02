FROM python:3.10-slim

WORKDIR /app

# Install git first (needed to clone the GitHub dependency)
RUN apt-get update && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

# Clone the optimizer repo
RUN git clone --branch dev https://github.com/gruedisueli/PyTopo3D_Backend.git /opt/pytopo3d_backend

# Install the backend with GPU support (this installs all deps from its pyproject.toml)
RUN pip install --no-cache-dir -e "/opt/pytopo3d_backend[gpu]"

# Copy ONLY requirements file(s)
COPY requirements.txt .

# Install dependencies (cached unless requirements.txt changes)
RUN pip install --no-cache-dir -r requirements.txt

# Copy all source code
COPY . .

# Copy and set up entrypoint
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

#install cloudflare
RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/* \
    && curl -L https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64 -o /usr/local/bin/cloudflared \
    && chmod +x /usr/local/bin/cloudflared

EXPOSE 8000

ENTRYPOINT ["/entrypoint.sh"]