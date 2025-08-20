-- Setup script for Ordo benchmarking with Benchbase

-- Install plugin only if it doesn't exist
SET @plugin_exists = (SELECT COUNT(*) FROM INFORMATION_SCHEMA.PLUGINS WHERE PLUGIN_NAME = 'lineairdb');
SET @sql = IF(@plugin_exists = 0, 'INSTALL PLUGIN lineairdb SONAME ''ha_lineairdb_storage_engine.so''', 'SELECT ''Plugin lineairdb already exists'' AS status');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

-- Create benchmark database
DROP DATABASE IF EXISTS ordo_bench;
CREATE DATABASE ordo_bench;

-- Show available engines
SHOW ENGINES;
