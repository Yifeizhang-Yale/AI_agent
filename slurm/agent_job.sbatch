#!/bin/bash
#SBATCH --job-name=dm-agent
#SBATCH --partition=day
#SBATCH --time=01:00:00
#SBATCH --cpus-per-task=1
#SBATCH --mem=2G
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --output=%x_%j.log
#SBATCH --error=%x_%j.err

# ============================================================
# HPC Data Management Agent — Yale Bouchet Cluster
#
# Resources: 1 CPU, 2GB RAM, 1 hour
# This is an I/O-bound job (filesystem metadata + API calls).
# ============================================================

set -euo pipefail

AGENT_DIR="${AGENT_DIR:-${HOME}/data_management_agent}"
VENV_DIR="${VENV_DIR:-${AGENT_DIR}/venv}"
CONFIG_FILE="${CONFIG_FILE:-${AGENT_DIR}/config.yaml}"
SECRETS_FILE="${SECRETS_FILE:-${AGENT_DIR}/.env}"

# Activate virtual environment
source "${VENV_DIR}/bin/activate"

# Load secrets (ANTHROPIC_API_KEY, DM_SMTP_USER, DM_SMTP_PASS)
if [ -f "${SECRETS_FILE}" ]; then
    set -a
    source "${SECRETS_FILE}"
    set +a
fi

echo "=== DM Agent starting at $(date -u) on $(hostname) ==="

# Run the full weekly cycle
python -m dm_agent run --config "${CONFIG_FILE}"
run_exit=$?

# Catalog next dataset if enabled
if [ $run_exit -eq 0 ]; then
    echo "=== Running data cataloger ==="
    python -m dm_agent catalog --config "${CONFIG_FILE}" || true
fi

echo "=== DM Agent finished at $(date -u) with exit code ${run_exit} ==="
exit ${run_exit}
