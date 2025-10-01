#!/bin/bash

# SemBench Website Public Deployment Script
# Deploys the website with public access via ngrok (no password required)
# Runs persistently in background even after disconnect

# Add user bin to PATH for ngrok
export PATH="$HOME/bin:$PATH"

PORT=${1:-8080}
DOCS_DIR="../docs"
LOG_DIR="/tmp/sembench_logs"

echo "🚀 Starting SemBench Website Deployment"
echo "============================================"

# Check if docs directory exists
if [ ! -d "$DOCS_DIR" ]; then
    echo "❌ Error: docs/ directory not found!"
    echo "Please run this script from the MMBench-System/scripts/ directory"
    exit 1
fi

# Create log directory
mkdir -p "$LOG_DIR"

# Check if ngrok is installed
if ! command -v ngrok &> /dev/null; then
    echo "❌ Error: ngrok is not installed!"
    echo ""
    echo "📦 To install ngrok:"
    echo "   1. Download from https://ngrok.com/download"
    echo "   2. Or use: snap install ngrok (Linux)"
    echo "   3. Or use: brew install ngrok (macOS)"
    echo ""
    echo "After installation, you may need to authenticate:"
    echo "   ngrok config add-authtoken <your-token>"
    echo "   Get your token from: https://dashboard.ngrok.com/get-started/your-authtoken"
    exit 1
fi

# Check if already running
if [ -f "/tmp/sembench_server.pid" ] && [ -f "/tmp/sembench_tunnel.pid" ]; then
    SERVER_PID=$(cat /tmp/sembench_server.pid)
    TUNNEL_PID=$(cat /tmp/sembench_tunnel.pid)
    if ps -p "$SERVER_PID" > /dev/null 2>&1 && ps -p "$TUNNEL_PID" > /dev/null 2>&1; then
        echo "⚠️  Website is already running!"
        echo "Run ./scripts/stop-website.sh first to stop it."
        exit 1
    fi
fi

echo "🌐 Starting local server on port $PORT..."
cd "$DOCS_DIR"
nohup python3 -m http.server "$PORT" > "$LOG_DIR/server.log" 2>&1 &
SERVER_PID=$!

# Wait for server to start
sleep 2

# Verify server started
if ! ps -p "$SERVER_PID" > /dev/null 2>&1; then
    echo "❌ Error: Failed to start local server"
    cat "$LOG_DIR/server.log"
    exit 1
fi

echo "🔗 Creating public tunnel with ngrok..."
nohup ngrok http "$PORT" --domain=sembench.ngrok.io --log=stdout > "$LOG_DIR/ngrok.log" 2>&1 &
TUNNEL_PID=$!

# Wait for ngrok to establish connection
sleep 4

# Verify ngrok started
if ! ps -p "$TUNNEL_PID" > /dev/null 2>&1; then
    echo "❌ Error: Failed to start ngrok tunnel"
    cat "$LOG_DIR/ngrok.log"
    kill "$SERVER_PID" 2>/dev/null
    exit 1
fi

# Extract public URL from ngrok
NGROK_URL=$(curl -s http://localhost:4040/api/tunnels 2>/dev/null | grep -o '"public_url":"https://[^"]*' | head -1 | cut -d'"' -f4)

echo ""
echo "✅ Website is now publicly accessible!"
echo "============================================"
if [ -n "$NGROK_URL" ]; then
    echo "📍 Public URL: $NGROK_URL"
else
    echo "📍 Public URL: Check ngrok dashboard or logs"
    echo "   Dashboard: http://localhost:4040"
fi
echo "🏠 Local URL: http://localhost:$PORT"
echo "📊 Ngrok Dashboard: http://localhost:4040"
echo ""
echo "🔓 No password required - share the URL directly!"
echo ""
echo "📝 Logs:"
echo "   Server: $LOG_DIR/server.log"
echo "   Ngrok:  $LOG_DIR/ngrok.log"
echo ""
echo "🛑 To stop the website, run: ./scripts/stop-website.sh"
echo ""
echo "✨ Processes are running in background with nohup"
echo "   They will continue running even after you disconnect"

# Save PIDs for cleanup script
echo "$SERVER_PID" > /tmp/sembench_server.pid
echo "$TUNNEL_PID" > /tmp/sembench_tunnel.pid
echo "$NGROK_URL" > /tmp/sembench_url.txt

echo ""
echo "🎉 Deployment complete!"