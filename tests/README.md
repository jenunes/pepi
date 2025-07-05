# Test Files for Pepi

This directory contains test log files for testing various features of the pepi MongoDB log analysis tool.

## Test Files

### `test_sort.log`
- **Purpose**: Test sorting functionality with equal connection counts
- **Content**: 3 IPs (192.168.1.100, 10.0.0.50, 127.0.0.1) with 2 opened/2 closed connections each
- **Use case**: Test `--sort-by hostname` functionality

### `test_sort2.log`
- **Purpose**: Test sorting functionality with different connection counts
- **Content**: 
  - 192.168.1.100: 2 opened, 1 closed
  - 10.0.0.50: 1 opened, 1 closed  
  - 127.0.0.1: 2 opened, 2 closed
- **Use case**: Test `--sort-by opened` and `--sort-by closed` functionality

## Usage Examples

```bash
# Test hostname sorting
python pepi.py --fetch tests/test_sort.log --connections --sort-by hostname

# Test opened connections sorting
python pepi.py --fetch tests/test_sort2.log --connections --sort-by opened

# Test closed connections sorting
python pepi.py --fetch tests/test_sort2.log --connections --sort-by closed
``` 