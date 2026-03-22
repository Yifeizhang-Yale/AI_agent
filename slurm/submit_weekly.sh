#!/bin/bash
# ============================================================
# Manual submission script — use this for ad-hoc runs.
#
# For scheduled weekly runs, use scrontab instead:
#   scrontab slurm/scrontab.example
#
# This script is for when you want to run manually:
#   bash slurm/submit_weekly.sh
# ============================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SBATCH_FILE="${SCRIPT_DIR}/agent_job.sbatch"

# Check if a dm-agent job is already queued or running
if squeue -u "$USER" -n dm-agent --noheader 2>/dev/null | grep -q .; then
    echo "dm-agent job already in queue, skipping submission"
    squeue -u "$USER" -n dm-agent
    exit 0
fi

job_id=$(sbatch --parsable "${SBATCH_FILE}")
echo "Submitted dm-agent job ${job_id}"
echo "Monitor: squeue -j ${job_id}"
echo "Logs:    cat dm-agent_${job_id}.log"
