#!/bin/bash
# setup-claude-code.sh — Add dm-agent MCP server to a lab member's Claude Code
#
# Usage:
#   bash bin/setup-claude-code.sh
#
# This adds the dm-agent MCP server to ~/.claude/settings.json so that
# Claude Code can search and query lab data from ANY project directory.
#
# What lab members get:
#   - dm_search_data    — search the data catalog by keywords
#   - dm_list_datasets  — list all available datasets
#   - dm_dataset_info   — detailed info about a specific dataset
#   - dm_inspect_directory — look at directory contents
#   - dm_submit_feedback — send requests to the data admin
#
# What lab members do NOT get:
#   - No write access to the database
#   - No catalog/organize/delete operations
#   - No ability to modify data

set -euo pipefail

# Resolve the dm-agent project directory (parent of bin/)
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
AGENT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

# On HPC, use the shared installation path
if [ -d "/nfs/roberts/project/pi_yz875/yz2489/proj/AI_agent/Data_Management" ]; then
    AGENT_DIR="/nfs/roberts/project/pi_yz875/yz2489/proj/AI_agent/Data_Management"
fi

VENV_PYTHON="${AGENT_DIR}/venv/bin/python"
CONFIG_PATH="${AGENT_DIR}/config.yaml"
SETTINGS_FILE="${HOME}/.claude/settings.json"

echo "=== DM Agent — Claude Code MCP Setup ==="
echo ""
echo "Agent directory: ${AGENT_DIR}"
echo "Config:          ${CONFIG_PATH}"
echo "Settings:        ${SETTINGS_FILE}"
echo ""

# Check prerequisites
if [ ! -f "${CONFIG_PATH}" ]; then
    echo "ERROR: config.yaml not found at ${CONFIG_PATH}"
    exit 1
fi

if [ ! -f "${VENV_PYTHON}" ]; then
    echo "WARNING: venv python not found at ${VENV_PYTHON}"
    echo "         Will use 'python3' — make sure dm_agent is importable."
    VENV_PYTHON="python3"
fi

# Install mcp package if needed
echo "Checking mcp package..."
if "${VENV_PYTHON}" -c "import mcp" 2>/dev/null; then
    echo "  mcp package already installed."
else
    echo "  Installing mcp package..."
    "${VENV_PYTHON}" -m pip install "mcp>=1.0.0" --quiet 2>/dev/null || {
        echo "  WARNING: Could not install mcp package automatically."
        echo "  Please run: ${VENV_PYTHON} -m pip install 'mcp>=1.0.0'"
    }
fi

# Create ~/.claude if needed
mkdir -p "$(dirname "${SETTINGS_FILE}")"

# Initialize settings file if it doesn't exist
if [ ! -f "${SETTINGS_FILE}" ]; then
    echo "{}" > "${SETTINGS_FILE}"
    echo "Created ${SETTINGS_FILE}"
fi

# Add MCP server config using Python (safe JSON manipulation)
"${VENV_PYTHON}" -c "
import json, sys

settings_path = '${SETTINGS_FILE}'
agent_dir = '${AGENT_DIR}'
venv_python = '${VENV_PYTHON}'
config_path = '${CONFIG_PATH}'

try:
    with open(settings_path) as f:
        settings = json.load(f)
except (json.JSONDecodeError, FileNotFoundError):
    settings = {}

settings.setdefault('mcpServers', {})

# Check if already configured
if 'dm-agent' in settings.get('mcpServers', {}):
    print('dm-agent MCP server already configured. Updating...')

settings['mcpServers']['dm-agent'] = {
    'command': venv_python,
    'args': ['-m', 'dm_agent.mcp_server'],
    'env': {
        'DM_AGENT_CONFIG': config_path,
    },
}

with open(settings_path, 'w') as f:
    json.dump(settings, f, indent=2)

print(f'MCP server config written to {settings_path}')
" || {
    echo "ERROR: Failed to update settings. Please add manually:"
    echo ""
    echo "  Add to ${SETTINGS_FILE}:"
    echo "  {"
    echo "    \"mcpServers\": {"
    echo "      \"dm-agent\": {"
    echo "        \"command\": \"${VENV_PYTHON}\","
    echo "        \"args\": [\"-m\", \"dm_agent.mcp_server\"],"
    echo "        \"env\": {"
    echo "          \"DM_AGENT_CONFIG\": \"${CONFIG_PATH}\""
    echo "        }"
    echo "      }"
    echo "    }"
    echo "  }"
    exit 1
}

echo ""
echo "=== Setup complete! ==="
echo ""
echo "Restart Claude Code to activate. Your Claude Code now has these tools:"
echo "  - dm_search_data     Search the data catalog"
echo "  - dm_list_datasets   List all datasets"
echo "  - dm_dataset_info    Get dataset details"
echo "  - dm_inspect_directory  Explore directories"
echo "  - dm_submit_feedback Submit requests to admin"
echo ""
echo "Try asking Claude Code: 'What structural connectivity data do we have?'"
