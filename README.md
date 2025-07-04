# pepi

A fast, user-friendly MongoDB log analysis tool.

## Features
- Summarize MongoDB log files (start/end date, number of lines, OS and DB version)
- Print replica set configuration (`--rs-conf`)
- Print replica set node status (`--rs-state`)

## Usage

```sh
pepi /path/to/mongod.log           # Show summary details
pepi /path/to/mongod.log --rs-conf # Show replica set configuration
pepi /path/to/mongod.log --rs-state # Show replica set node status
```

## Installation

1. Clone this repo
2. (Optional) Create and activate a Python virtual environment
3. Install dependencies:
   ```sh
   pip install -r requirements.txt
   ```
4. Run the tool:
   ```sh
   python pepi.py /path/to/mongod.log
   ```

## Roadmap
- Multithreaded log parsing for large files
- Export to JSON/CSV
- More advanced analytics and filtering
