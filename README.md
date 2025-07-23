# pepi

A fast, user-friendly MongoDB log analysis tool for extracting insights from MongoDB log files.

## Features

### Core Analysis
- **MongoDB Log Summary**: Start/end date, number of lines, OS and DB version, host information
- **Command Line Reconstruction**: Reconstructs the original mongod startup command
- **Replica Set Analysis**: Configuration and state transitions
- **Connection Analysis**: Connection statistics with duration tracking
- **Client/Driver Information**: Detailed client and driver analysis
- **Query Pattern Analysis**: Query statistics and performance analysis with pattern recognition

### Advanced Features
- **Progress Bars**: Visual feedback during large file processing for all analysis modes
- **Connection Duration Statistics**: Average, minimum, and maximum connection durations
- **Sorting Options**: Sort connections by opened/closed counts
- **Query Performance Statistics**: Min, max, 95th percentile, sum, and mean execution times
- **Query Pattern Recognition**: Groups similar queries by structure with normalized patterns
- **Comparison Tools**: Compare specific hostnames/IPs side-by-side
- **Clean Output Formatting**: Well-organized, aligned output for easy reading

## Installation

1. Clone this repository:
   ```bash
   git clone https://github.com/jenunes/pepi.git
   cd pepi
   ```

## Performance

pepi is designed to handle large log files efficiently:
- **Progress bars** provide visual feedback during processing
- **Streaming processing** minimizes memory usage
- **Optimized parsing** for large files (tested up to 2GB+)
- **Early termination** support for long-running operations

## Cache System

pepi uses an automatic cache system to speed up repeated analysis of large log files:
- **Automatic 7-day TTL**: Cache files are kept for 7 days since their last use. Every time a cache file is used, its timer resets (sliding expiration).
- **No manual cleanup needed**: If a cache file is not used for 7 days, it is automatically deleted and rebuilt on next use.
- **Location**: Cache files are stored in `~/.pepi_cache/`.
- **Safe and efficient**: This ensures your cache stays fresh and never grows indefinitely.

You can always force a re-parse and clear the cache with the `--clear-cache` flag.

2. **Option A: Install globally (recommended)**
   ```bash
   pip install -e .
   ```
   After installation, you can run `pepi` from anywhere on your system:
   ```bash
   pepi --fetch /path/to/mongod.log
   pepi --fetch /path/to/mongod.log --connections
   ```

3. **Option B: Use with virtual environment**
   ```bash
   # Create and activate a Python virtual environment
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   
   # Install dependencies
   pip install -r requirements.txt
   
   # Run the tool
   python pepi.py --fetch /path/to/mongod.log
   ```

## Usage

### Basic Usage
```bash
# Show MongoDB log summary and command line startup
pepi --fetch /path/to/mongod.log

# Show replica set configuration
pepi --fetch /path/to/mongod.log --rs-conf

# Show replica set node status and transitions
pepi --fetch /path/to/mongod.log --rs-state

# Show client/driver information
pepi --fetch /path/to/mongod.log --clients

# Show query pattern statistics and performance analysis
pepi --fetch /path/to/mongod.log --queries
```

### Connection Analysis
```bash
# Basic connection information
pepi --fetch /path/to/mongod.log --connections

# Connection information with duration statistics
pepi --fetch /path/to/mongod.log --connections --stats

# Sort connections by opened count (descending)
pepi --fetch /path/to/mongod.log --connections --sort-by opened

# Sort connections by closed count (descending)
pepi --fetch /path/to/mongod.log --connections --sort-by closed

# Compare specific hostnames/IPs (2-3 required)
pepi --fetch /path/to/mongod.log --connections --compare 127.0.0.1 --compare 192.168.1.100

# Combine multiple options
pepi --fetch /path/to/mongod.log --connections --stats --sort-by opened --compare 127.0.0.1 --compare 192.168.1.100
```

### Query Analysis

The `--queries` flag analyzes query patterns and provides performance statistics:

- **Index Information**: Shows which indexes are used by each query pattern (extracted from `planSummary`)
  - `COLLSCAN` indicates a collection scan (no index used)
  - Index names like `age_1`, `status_1` show specific indexes used
  - `N/A` for operations that don't use indexes (like inserts)
- **Pattern Truncation**: By default, query patterns longer than 150 characters are truncated with "..." to keep terminal output readable
- **Full Patterns**: Use `--report-full-patterns <file>` to write complete patterns to a file instead of printing to terminal
- **Namespace Filtering**: Use `--namespace` to filter queries by specific database.collection
- **Operation Filtering**: Use `--operation` to filter queries by operation type (find, insert, update, delete, aggregate)
- **Histogram**: Use `--report-histogram` to show overall execution time distribution for the filtered data
  - Time ranges use mathematical notation: `[a,b)` means "a to less than b"
  - Example: `[10,100)ms` = 10ms to less than 100ms
- **Sorting**: Use `--sort-by` with values like `count`, `mean`, `max`, `min`, `95%`, or `sum`

Example:
```bash
# Show truncated patterns in terminal
pepi --fetch mongodb.log --queries

# Write complete patterns to file
pepi --fetch mongodb.log --queries --report-full-patterns query_report.txt

# Sort by execution count
pepi --fetch mongodb.log --queries --sort-by count

# Filter by specific namespace
pepi --fetch mongodb.log --queries --namespace test.users

# Filter by namespace and sort by count
pepi --fetch mongodb.log --queries --namespace test.users --sort-by count

# Filter by namespace and generate full report
pepi --fetch mongodb.log --queries --namespace test.users --report-full-patterns report.txt

# Filter by namespace, sort by mean, and generate full report
pepi --fetch mongodb.log --queries --namespace test.users --sort-by mean --report-full-patterns slow_queries.txt

# Filter by operation type
pepi --fetch mongodb.log --queries --operation find

# Filter by operation and namespace
pepi --fetch mongodb.log --queries --operation find --namespace test.users

# Filter by operation, sort by count, and generate report
pepi --fetch mongodb.log --queries --operation aggregate --sort-by count --report-full-patterns aggregate_report.txt

# Show execution time distribution histogram
pepi --fetch mongodb.log --queries --report-histogram

# Show execution time distribution histogram
pepi --fetch mongodb.log --queries --report-histogram

# Filter by namespace and show histogram for that namespace
pepi --fetch mongodb.log --queries --namespace test.users --report-histogram

# Filter by namespace and operation, show histogram for that specific slice
pepi --fetch mongodb.log --queries --namespace test.users --operation find --report-histogram
```

### Query Analysis Examples
```bash
# Basic query pattern statistics (includes index information)
pepi --fetch /path/to/mongod.log --queries

# Sort queries by count (most frequent first)
pepi --fetch /path/to/mongod.log --queries --sort-by count

# Sort queries by mean execution time (slowest first)
pepi --fetch /path/to/mongod.log --queries --sort-by mean

# Sort queries by 95th percentile execution time
pepi --fetch /path/to/mongod.log --queries --sort-by 95%

# Sort queries by total execution time
pepi --fetch /path/to/mongod.log --queries --sort-by sum

# Sort queries by minimum execution time
pepi --fetch /path/to/mongod.log --queries --sort-by min

# Sort queries by maximum execution time
pepi --fetch /path/to/mongod.log --queries --sort-by max
```

## Output Examples

### Default Summary
```
===== Node Command Line Startup =====
mongod --port 27017 --replSet myReplicaSet --dbpath /data/db

===== MongoDB Log Summary =====
Log file        : /path/to/mongod.log
Start date      : 2025-01-01T10:00:00.000Z
End date        : 2025-01-01T10:00:30.000Z
Number of lines : 1000
Host            : localhost:27017
OS version      : Ubuntu 20.04.3 LTS
Kernel version  : 5.4.0-74-generic
MongoDB version : 6.0.24-19
ReplicaSet Name : myReplicaSet
Nodes           : 3
```

### Connection Analysis with Statistics
```
===== Connection Details =====
Total Connections Opened: 150
Total Connections Closed: 145
Overall Average Connection Duration: 45.23s
Overall Minimum Connection Duration: 0.01s
Overall Maximum Connection Duration: 300.45s
--------Sorted by opened--------
192.168.1.100 | opened:75  | closed:72  | dur-avg:45.23s | dur-min:0.01s | dur-max:300.45s
127.0.0.1     | opened:50  | closed:48  | dur-avg:42.15s | dur-min:0.05s | dur-max:250.30s
10.0.0.50     | opened:25  | closed:25  | dur-avg:48.67s | dur-min:0.10s | dur-max:180.20s
```

### Client/Driver Information
```
===== Client/Driver Information =====

PyMongo v4.8.0 (myapp v1.2.3)
├─ Connections: 45
├─ IP Addresses: 192.168.1.100, 127.0.0.1
└─ Users: admin@admin, user1@mydb

NetworkInterfaceTL v4.2.23-23
├─ Connections: 30
├─ IP Addresses: 10.0.0.50
└─ Users: __system@local
```

### Replica Set Configuration
```
===== Replica Set Configuration =====
Timestamp: 2025-01-01T10:00:15.000Z
--------------------------------------------------
{
  "_id": "myReplicaSet",
  "version": 1,
  "members": [
    {
      "_id": 0,
      "host": "localhost:27017",
      "priority": 1
    },
    {
      "_id": 1,
      "host": "localhost:27018",
      "priority": 1
    }
  ]
}
```

### Query Pattern Statistics
```
===== Query Pattern Statistics =====
Namespace  | Operation | Pattern                                                | Count | Min(ms) | Max(ms) | 95%(ms) | Sum(ms) | Mean(ms) | Index
test.users | find      | {"age": {"$gt": "?"}}                                  | 7     | 41.0    | 52.0    | 48.0    | 320.0   | 45.7     | age_1
test.users | find      | {"status": "?"}                                        | 7     | 28.0    | 35.0    | 33.0    | 218.0   | 31.1     | status_1
test.users | aggregate | [$match,$group]                                        | 3     | 120.0   | 135.0   | 125.0   | 380.0   | 126.7    | COLLSCAN
test.users | update    | [{"q": {"name": "?"}, "u": {"$set": {"status": "?"}}}] | 2     | 18.0    | 19.0    | 18.0    | 37.0    | 18.5     | name_1
test.users | insert    | insert_keys:age,name                                   | 1     | 15.0    | 15.0    | 15.0    | 15.0    | 15.0     | N/A
test.users | delete    | [{"limit": "?", "q": {"status": "?"}}]                 | 1     | 22.0    | 22.0    | 22.0    | 22.0    | 22.0     | status_1
```

## Command Line Options

### Required Options
- `--fetch, -f PATH`: MongoDB log file to analyze

### Main Analysis Modes
- `--rs-conf`: Print replica set configuration(s)
- `--rs-state`: Print replica set node status and transitions
- `--clients`: Print client/driver information
- `--queries`: Print query pattern statistics and performance analysis
- `--report-full-patterns <file>`: Write complete query patterns to file (requires output file path)
- `--clear-cache`: Clear all cached data and re-parse files

### Connection Analysis
- `--connections`: Print connection information and statistics

#### Connection Sub-options (use with `--connections`)
- `--stats`: Include connection duration statistics
- `--sort-by`: Sort by `opened` or `closed` count
- `--compare`: Compare 2-3 specific hostnames/IPs

#### Query Sub-options (use with `--queries`)
- `--sort-by`: Sort by `count`, `min`, `max`, `95%`, `sum`, or `mean`

## Features in Detail

### Connection Duration Tracking
- Tracks connection start and end times using `connectionId`
- Calculates duration statistics per IP and overall
- Handles MongoDB's ISO timestamp format with timezone support

### Sorting and Comparison
- Sort connections by opened or closed count (descending order)
- Compare specific hostnames/IPs side-by-side
- Aligned output for easy comparison

### Client Analysis
- Extracts driver information from connection events
- Tracks authentication events per driver
- Shows connections, IPs, and authenticated users per driver

### Replica Set Analysis
- Parses replica set configuration changes
- Tracks state transitions per node
- Shows current node status with timestamps

### Query Pattern Analysis
- Extracts query patterns from MongoDB command logs
- Groups similar queries by structure (normalized patterns)
- Calculates comprehensive performance statistics
- Tracks disk usage flags for aggregation queries
- Supports sorting by any performance metric

## Development

### Project Structure
```
pepi/
├── pepi.py              # Main application
├── requirements.txt     # Python dependencies
├── README.md           # This file
├── tests/              # Test files
│   ├── README.md       # Test documentation
│   ├── test_sort.log   # Test sorting functionality
│   ├── test_sort2.log  # Test comparison functionality
│   └── test_queries.log # Test query pattern functionality
└── venv/               # Virtual environment
```

### Running Tests
```bash
# Test sorting functionality
python pepi.py --fetch tests/test_sort.log --connections --sort-by opened

# Test comparison functionality
python pepi.py --fetch tests/test_sort2.log --connections --compare 127.0.0.1 --compare 192.168.1.100

# Test with statistics
python pepi.py --fetch tests/test_sort2.log --connections --stats --compare 127.0.0.1 --compare 192.168.1.100
```

## Requirements

- Python 3.7+
- click (for CLI interface)
- Standard library modules (json, datetime, collections)

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Version History

- **v0.0.1.6**: Added index information column to query analysis showing which indexes are used
- **v0.0.1.5**: Added `--queries` flag with query pattern analysis and performance statistics
- **v0.0.1.4**: Major improvements: sorting, comparison, and comprehensive documentation
- **v0.0.1.3**: Added `--clients` flag with clean tree-like display format
- **v0.0.1.2**: Added `--stats` flag for connection duration statistics
- **v0.0.1.1**: Enhanced default summary with host information and node count
- **v0.0.1**: Initial release with basic MongoDB log analysis features
