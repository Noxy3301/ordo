# Ordo

## Quick Start

```bash
git clone --recursive https://github.com/Noxy3301/ordo.git
cd ordo
./build.sh
```

### Initialize and Start MySQL Server

```bash
cd build
./runtime_output_directory/mysqld --initialize-insecure --user=$USER --datadir=./data
./runtime_output_directory/mysqld --datadir=./data --socket=/tmp/mysql.sock --port=3307 &
```

### Running Ordo Server

```bash
./scripts/start_server.sh
```

### Connect to MySQL and Install LineairDB Plugin

```bash
./build/runtime_output_directory/mysql -u root --socket=/tmp/mysql.sock --port=3307
```

Then run the following SQL command:
```sql
INSTALL PLUGIN lineairdb SONAME 'ha_lineairdb_storage_engine.so';
```

#### Simple Example: Using MySQL with Ordo Server

```bash
SHOW ENGINES;

DROP DATABASE IF EXISTS lineairdb_test;
CREATE DATABASE lineairdb_test;
USE lineairdb_test;

CREATE TABLE test (
    id INT PRIMARY KEY,
    name VARCHAR(20)
) ENGINE=LINEAIRDB;

INSERT INTO test VALUES (1, 'hello');
INSERT INTO test VALUES (2, 'world');

SELECT * FROM test;

SELECT * FROM test WHERE id = 1;
```

## Benchmark

```bash
# Build
./scripts/build.sh

# Start servers
./scripts/start_server.sh
./scripts/start_mysql.sh --mysqld-port 3307 --server-host 127.0.0.1 --server-port 9999

# Patch BenchBase (first time only)
python3 bench/bin/patch_benchbase.py

# Run benchmark (TPC-C example)
python3 bench/bin/benchrun.py tpcc --terminals 64
```
