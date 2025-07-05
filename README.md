# pepi

A fast, user-friendly MongoDB log analysis tool for extracting insights from MongoDB log files.

## Features

### Core Analysis
- **MongoDB Log Summary**: Start/end date, number of lines, OS and DB version, host information
- **Command Line Reconstruction**: Reconstructs the original mongod startup command
- **Replica Set Analysis**: Configuration and state transitions
- **Connection Analysis**: Connection statistics with duration tracking
- **Client/Driver Information**: Detailed client and driver analysis

### Advanced Features
- **Connection Duration Statistics**: Average, minimum, and maximum connection durations
- **Sorting Options**: Sort connections by opened/closed counts
- **Comparison Tools**: Compare specific hostnames/IPs side-by-side
- **Clean Output Formatting**: Well-organized, aligned output for easy reading

## Installation

1. Clone this repository:
   ```bash
   git clone https://github.com/jenunes/pepi.git
   cd pepi
   ```

2. Create and activate a Python virtual environment (recommended):
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Usage

### Basic Usage
```bash
# Show MongoDB log summary and command line startup
python pepi.py --fetch /path/to/mongod.log

# Show replica set configuration
python pepi.py --fetch /path/to/mongod.log --rs-conf

# Show replica set node status and transitions
python pepi.py --fetch /path/to/mongod.log --rs-state

# Show client/driver information
python pepi.py --fetch /path/to/mongod.log --clients
```

### Connection Analysis
```bash
# Basic connection information
python pepi.py --fetch /path/to/mongod.log --connections

# Connection information with duration statistics
python pepi.py --fetch /path/to/mongod.log --connections --stats

# Sort connections by opened count (descending)
python pepi.py --fetch /path/to/mongod.log --connections --sort-by opened

# Sort connections by closed count (descending)
python pepi.py --fetch /path/to/mongod.log --connections --sort-by closed

# Compare specific hostnames/IPs (2-3 required)
python pepi.py --fetch /path/to/mongod.log --connections --compare 127.0.0.1 --compare 192.168.1.100

# Combine multiple options
python pepi.py --fetch /path/to/mongod.log --connections --stats --sort-by opened --compare 127.0.0.1 --compare 192.168.1.100
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

## Command Line Options

### Required Options
- `--fetch, -f PATH`: MongoDB log file to analyze

### Main Analysis Modes
- `--rs-conf`: Print replica set configuration(s)
- `--rs-state`: Print replica set node status and transitions
- `--clients`: Print client/driver information

### Connection Analysis
- `--connections`: Print connection information and statistics

#### Connection Sub-options (use with `--connections`)
- `--stats`: Include connection duration statistics
- `--sort-by`: Sort by `opened` or `closed` count
- `--compare`: Compare 2-3 specific hostnames/IPs

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
│   └── test_sort2.log  # Test comparison functionality
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

- **v0.0.1.3**: Added `--clients` flag with clean tree-like display format
- **v0.0.1.2**: Added `--stats` flag for connection duration statistics
- **v0.0.1.1**: Enhanced default summary with host information and node count
- **v0.0.1**: Initial release with basic MongoDB log analysis features
