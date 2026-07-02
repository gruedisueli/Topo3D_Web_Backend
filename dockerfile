FROM python:3.10-slim

WORKDIR /app

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