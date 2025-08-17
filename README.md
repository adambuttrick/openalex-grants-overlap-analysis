# OpenAlex Grants Overlap Analysis

## Overview

Tools for reconciling grant funding data between OpenAlex and institutional/funder datasets using DuckDB.

## Installation

```bash
pip install -r requirements.txt
```

## Data Prep

1. Follow the instructions at [OpenAlex Documentation](https://docs.openalex.org/download-all-data/openalex-snapshot) to download the OpenAlex snapshot.

2. Then, use [openalex-fast-field-parse](https://github.com/adambuttrick/openalex-fast-field-parse) to extract the grants field from OpenAlex works into a CSV file. 

## Usage

### Step 1: Build the Grants Database

Create a DuckDB database from the OpenAlex grants CSV:

```bash
python build_grants_db.py --grants-csv grants.csv --db-output grants.db
```

Options:
- `--grants-csv`: Path to the grants CSV file (required)
- `--db-output`: Output database file (default: grants.db)
- `--chunk-size`: CSV reading chunk size (default: 100000)
- `--verbose`: Enable verbose output
- `--force`: Overwrite existing database without prompting

### Step 2: Reconcile Grant Data

Compare your institutional/funder grant data with OpenAlex:

```bash
python reconcile_grants_db.py query --db grants.db -i your_grants.csv -f https://openalex.org/F4320306577
```

Options:
- `--db`: Path to the grants database (required)
- `-i, --input-file`: Your grants CSV file with DOI and award_id columns (required)
- `-f, --funder-id`: OpenAlex Funder ID to match (required)
- `-a, --award-field`: Column name for award ID in input file (default: award_id)
- `-o, --output-dir`: Output directory for results (default: output)
- `-e, --excel`: Generate consolidated Excel report
- `-v, --verbose`: Enable verbose output

### View Database Information

```bash
python reconcile_grants_db.py info --db grants.db
```

## Input File Format

Your input CSV should contain the following columns:
- `doi`: Digital Object Identifier (lowercase, trimmed)
- `award_id`: Grant award identifier (or custom field specified with -a flag)

## Output Files

The reconciliation process generates four CSV files:

1. `funder_work_and_grant_id_match_in_openalex.csv`: Records where both DOI and grant ID match
2. `funder_work_matched_in_openalex_grant_id_differs.csv`: Records where DOI matches but grant ID differs
3. `funder_grants_not_in_openalex.csv`: Funder grants not found in OpenAlex
4. `openalex_grants_not_in_funder.csv`: OpenAlex grants not in the input dataset

Additional outputs:
- `reconciliation_stats_*.txt`: Detailed statistics report
- `grants_overlap_analysis.xlsx`: Consolidated Excel report (if --excel flag used)



