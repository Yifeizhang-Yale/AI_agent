# DM Agent — HPC Data Management Agent

## Quick Start

```bash
cd /nfs/roberts/project/pi_yz875/yz2489/proj/AI_agent/Data_Management
source venv/bin/activate
```

## Commands

```bash
# Catalog a specific dataset
python -m dm_agent -c config.yaml catalog --dataset <NAME>

# View catalog report (all or specific)
python -m dm_agent -c config.yaml catalog-report
python -m dm_agent -c config.yaml catalog-report --dataset <NAME> --verbose
python -m dm_agent -c config.yaml catalog-report --dataset <NAME> --subjects  # show per-modality subject lists

# Search data catalog (natural language keywords)
python -m dm_agent -c config.yaml query "SC matrix"              # human-readable
python -m dm_agent -c config.yaml query "SC matrix" --format json  # structured JSON

# Organize a dataset (remove redundant files + restructure)
python -m dm_agent -c config.yaml organize --dataset <NAME> --dry-run   # preview
python -m dm_agent -c config.yaml organize --dataset <NAME>             # execute

# Run full weekly cycle (admin only)
python -m dm_agent -c config.yaml run

# Check status
python -m dm_agent -c config.yaml status
```

## Helping Lab Members Find Data

When a user asks to find or select experimental data (e.g., "I need structural connectivity matrices", "which datasets have resting-state fMRI?", "find PET amyloid data"):

**Step 1 — Read the manifest** (fast, no CLI needed):
Read `DATA_MANIFEST.yaml` in this directory. It contains per-dataset, per-modality summaries with subject counts, file paths, and descriptions. This is enough to answer most questions.

**Step 2 — Detailed search** (only if needed):
For specific file-level queries (e.g., "find the exact .mat file for VBM data"), use the query command:
```bash
cd /nfs/roberts/project/pi_yz875/yz2489/proj/AI_agent/Data_Management
source venv/bin/activate
python -m dm_agent -c config.yaml query "<keywords>" --format json
```

Present results to the user including:
- Which datasets match and where (path)
- Per-modality subject counts
- Specific directory paths
- Data stage (raw / preprocessed / derivatives)

## Available Datasets

A4, ABCD, ADNI, Atlas, HCP, IMAGEN, NACC, OHSU, UKB

## Notes

- Always run `--dry-run` first before organizing
- The agent uses Claude CLI for analysis — ensure `module load GCCcore/13.3.0 nodejs/20.13.1-GCCcore-13.3.0` is loaded on compute nodes
- Results are stored in `dm_agent.db` (SQLite)
- Config: `config.yaml`, lab context: `lab_context.yaml`
