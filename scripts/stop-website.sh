#!/bin/bash

# SemBench Website Stop Script
# Stops the local server and ngrok tunnel created by deploy-website.sh

echo "🛑 Stopping SemBench Website..."
echo "================================"

# Function to kill process by PID file
kill_process() {
    local pid_file="$1"
    local process_name="$2"

    if [ -f "$pid_file" ]; then
        local pid=$(cat "$pid_file")
        if ps -p "$pid" > /dev/null 2>&1; then
            echo "⏹️  Stopping $process_name (PID: $pid)..."
            kill "$pid"
            sleep 1

            # Force kill if still running
            if ps -p "$pid" > /dev/null 2>&1; then
                echo "🔥 Force stopping $process_name..."
                kill -9 "$pid"
            fi
            echo "✅ $process_name stopped"
        else
            echo "ℹ️  $process_name was not running"
        fi
        rm -f "$pid_file"
    else
        echo "ℹ️  No $process_name PID file found"
    fi
}

# Stop ngrok tunnel
kill_process "/tmp/sembench_tunnel.pid" "ngrok tunnel"

# Stop local server
kill_process "/tmp/sembench_server.pid" "Local server"

# Clean up any remaining processes
echo "🧹 Cleaning up remaining processes..."
pkill -f "python3 -m http.server" 2>/dev/null || true
pkill -f "ngrok http" 2>/dev/null || true

# Clean up temporary files
echo "🗑️  Removing temporary files..."
rm -f /tmp/sembench_url.txt

echo ""
echo "✅ Website stopped successfully!"
echo "💡 Logs are preserved at /tmp/sembench_logs/"
echo "   To view logs: cat /tmp/sembench_logs/server.log"
echo "   To view logs: cat /tmp/sembench_logs/ngrok.log"
echo "================================"