# Ordo Benchmarking

## Quick Start


### 1. Setup Benchbase

```bash
./bench/setup_benchbase.sh
```

### 2. Start servers
```bash
./scripts/start_mysql.sh
cd build/server && ./ordo_server
```

### 3. Install plugin and create DB
```bash
./build/runtime_output_directory/mysql -u root --socket=/tmp/mysql.sock --port=3307 < bench/setup.sql
```

### 4. Run YCSB benchmark
```bash
cd bench/benchbase/benchbase-mysql
java -jar benchbase.jar -b ycsb -c /home/noxy/work/ordo/bench/config/ycsb.xml --create=true --load=true --execute=true
```