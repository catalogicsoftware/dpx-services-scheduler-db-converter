# DPX Services Scheduler DB Converter

Migration tool for converting Python APScheduler SQLite databases to JSON file dumps.

## Overview

This tool migrates scheduled jobs from APScheduler SQLite databases to JSON format. It processes all SQLite database files (`.sqlite` or `.db`) in a specified directory and generates:

- Raw table dumps in JSON format for each table
- Converted APScheduler job definitions in API-compatible JSON format
- Summary logs with processing statistics

## Features

- Converts APScheduler job stores (SQLite) to JSON format
- Handles various trigger types: DateTrigger, IntervalTrigger, CronTrigger, and combined triggers
- Supports hourly, daily, weekly, and monthly schedules
- Processes multiple database files in batch
- Generates detailed logs and summary reports
- Creates portable binary compatible with GLIBC 2.28+ (AlmaLinux 8 compatible)

## Building the Binary

The project uses Docker to build a portable binary using PyInstaller. The Dockerfile creates a self-contained executable compatible with systems running GLIBC 2.28+.

### Prerequisites

- Docker with BuildKit support enabled
- Git (to clone the repository)

### Build Steps

1. **Clone the repository:**
   ```bash
   git clone <repository-url>
   cd dpx-services-scheduler-db-converter
   ```

2. **Build and extract the binary:**

   **Method 1: Using Docker BuildKit --output** (recommended):
   ```bash
   # Build targeting the export stage and extract directly
   docker build --target export --output type=local,dest=./dist -f Dockerfile .
   ```

   The binary will be extracted to `./dist/output/scheduler_migration`. The Dockerfile uses a multi-stage build with a minimal final stage (scratch), so only the binary file is extracted, not the entire build environment.

   Optionally, move it to a more convenient location:
   ```bash
   mv ./dist/output/scheduler_migration ./dist/scheduler_migration
   rmdir ./dist/output
   ```

   **Method 2: Using docker cp** (alternative):
   ```bash
   # Build the Docker image
   docker build -t scheduler-migration-builder -f Dockerfile .

   # Create a temporary container and copy only the binary file
   docker create --name temp-container scheduler-migration-builder
   mkdir -p ./dist
   docker cp temp-container:/output/scheduler_migration ./dist/scheduler_migration
   docker rm temp-container
   ```

   The binary will be extracted to `./dist/scheduler_migration`.

4. **Make the binary executable** (Linux/macOS):
   ```bash
   chmod +x ./dist/scheduler_migration
   ```

### Build Details

- **Build Stage:** Debian 10 (buster) with Python 3.9
- **Export Stage:** Minimal scratch image (contains only the binary)
- **GLIBC Compatibility:** 2.28+ (compatible with AlmaLinux 8)
- **Build Tool:** PyInstaller (creates a single-file executable)
- **Output Location:** `/output/scheduler_migration` in the export stage

The Dockerfile uses a multi-stage build to ensure that when using `docker build --output`, only the final binary is extracted, not the entire build environment with all dependencies and build tools.

## Usage

### Running the Binary

```bash
./dist/scheduler_migration <db_directory> [--output-dir <output_directory>]
```

### Arguments

- `db_directory` (required): Directory containing SQLite database files (`.sqlite` or `.db` extensions)
- `--output-dir` (optional): Directory to save JSON dump files (default: `db_dumps`)

### Example

```bash
# Process databases in /path/to/databases and save output to ./output
./dist/scheduler_migration /path/to/databases --output-dir ./output
```

### Output Files

For each database file processed, the tool generates:

1. **Table dumps:** `{db_name}_{table_name}.json` - Raw JSON dumps of each table
2. **APScheduler jobs:** `{db_name}_as_api_resource.json` - Converted job definitions in API format
3. **Log file:** `dump_scheduler_dbs_{timestamp}.log` - Detailed processing log
4. **Summary:** `dump_scheduler_dbs_summary_{timestamp}.json` - Processing statistics

### Output Format

The `{db_name}_as_api_resource.json` file contains an array of job objects with the following structure:

## Compatibility

- **Build System:** Linux (Debian 10+), macOS, Windows with Docker
- **Target System:** Linux with GLIBC 2.28+ (tested on AlmaLinux 8)
- **Database Format:** SQLite 3.x databases created by APScheduler

## Notes

- The tool processes databases in read-only mode and creates temporary copies for processing
- Jobs that cannot be restored (e.g., missing modules) are logged but processing continues
- The binary is statically linked and portable across compatible Linux distributions

