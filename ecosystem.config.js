module.exports = {
  apps: [
    { name: "tg-message-collector", script: ".venv/bin/python", args: "app.py run", interpreter: "none", autorestart: true },
    { name: "tg-message-collector-mcp", script: ".venv/bin/python", args: "-m collector.mcp_server", interpreter: "none", autorestart: true }
  ]
};
