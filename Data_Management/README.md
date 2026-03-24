# DM Agent - HPC Data Management Agent

Automated data cataloging, search, and management for neuroimaging datasets on HPC clusters.

## Features

- **Data Cataloging** - Automatically scan and classify datasets by modality, subject count, data stage, and BIDS compliance
- **Natural Language Search** - Query across all datasets with keywords (e.g. "structural connectivity", "amyloid PET")
- **Participants.tsv Generation** - BIDS-style subject x modality cross-tables per dataset
- **Dataset Organization** - Detect and remove redundant files, restructure directories
- **Quota Monitoring** - Track disk usage with Lustre/GPFS/du support
- **Auto README Generation** - Claude-powered README creation for undocumented directories
- **Email Reporting** - Per-member reports with cleanup recommendations
- **Deletion Workflow** - Token-based confirmation for safe data removal

## Supported Datasets

A4, ABCD, ADNI, Atlas, HCP (YA/Aging/Development), IMAGEN, NACC, OHSU, UKB

## Quick Start

```bash
# Clone
git clone git@github.com:Yifeizhang-Yale/AI_agent.git
cd AI_agent/Data_Management

# Setup
python -m venv venv
source venv/bin/activate
pip install -e .

# Configure
cp config.example.yaml config.yaml
cp lab_context.example.yaml lab_context.yaml
# Edit both files for your environment

# On HPC compute nodes, load Node.js for Claude CLI:
module load GCCcore/13.3.0 nodejs/20.13.1-GCCcore-13.3.0
```

## Usage

```bash
# Catalog a dataset
python -m dm_agent -c config.yaml catalog --dataset NACC

# View catalog report
python -m dm_agent -c config.yaml catalog-report
python -m dm_agent -c config.yaml catalog-report --dataset NACC --subjects

# Search data (natural language)
python -m dm_agent -c config.yaml query "SC matrix"
python -m dm_agent -c config.yaml query "resting-state fMRI" --format json

# Generate participants.tsv
python -m dm_agent -c config.yaml participants --dataset ABCD

# Organize a dataset (always dry-run first!)
python -m dm_agent -c config.yaml organize --dataset NACC --dry-run
python -m dm_agent -c config.yaml organize --dataset NACC

# Check status
python -m dm_agent -c config.yaml status

# Run full weekly cycle (admin only)
python -m dm_agent -c config.yaml run
```

## Architecture

The agent is built around a **skill-based architecture**. Each skill handles one aspect of data management:

| Skill | Phase | Description |
|-------|-------|-------------|
| `scanner` | scan | Discover directory changes and stale data |
| `data_cataloger` | scan | Deep dataset analysis with modality/subject detection |
| `analyzer` | analyze | Claude-powered analysis and recommendations |
| `quota_monitor` | analyze | Disk usage monitoring |
| `reporter` | report | Email reports to lab members |
| `readme_generator` | report | Auto-generate missing READMEs |
| `lab_overview` | report | Generate lab-wide data overview |
| `confirmer` | cleanup | Token-based deletion confirmation |
| `deleter` | cleanup | Execute confirmed deletions |
| `dataset_organizer` | cleanup | Reorganize and deduplicate |

Skills run in phase order via the `Orchestrator`. Each can be enabled/disabled independently in `config.yaml`.

## Data Storage

- **Database**: `dm_agent.db` (SQLite) - catalog entries, modality stats, deletion records
- **Config**: `config.yaml` (paths, email, skill settings) + `lab_context.yaml` (lab members, projects)
- **Outputs**: `DATA_MANIFEST.yaml`, `LAB_DATA_OVERVIEW.md`, `participants/*.tsv`

## Requirements

- Python >= 3.9
- Claude CLI (`claude` command) for AI-powered analysis
- HPC environment with access to data directories
