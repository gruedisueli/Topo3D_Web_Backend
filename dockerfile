FROM python:3.10-slim

WORKDIR /app

# Copy all source code
COPY . .

# Install the package with GPU support
# CuPy[ctk] bundles its own CUDA runtime libraries as wheels,
# so no system CUDA install is needed — the NVIDIA host driver is sufficient.
RUN pip install --no-cache-dir -e ".[gpu,web]"

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