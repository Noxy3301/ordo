## LineairDB

<p>
  <img alt="Version" src="https://img.shields.io/badge/version-0.1.0-blue.svg?cacheSeconds=2592000" />
  <a href="#Documentation" target="_blank">
    <img alt="Documentation" src="https://img.shields.io/badge/documentation-yes-brightgreen.svg" />
  </a>
  <a href="https://www.apache.org/licenses/LICENSE-2.0" target="_blank">
    <img alt="License: Apache--2.0" src="https://img.shields.io/badge/License-Apache--2.0-yellow.svg" />
  </a>
  <img alt="CI" src="https://github.com/LineairDB/LineairDB/workflows/C/C++ CI/badge.svg" />

</p>

**LineairDB is a fast transactional key-value storage library. It provides transaction processing for multiple keys with strict serializability.**

### Features

---

- Keys and values are arbitrary byte arrays.
- The basic operations are Read(table, key), the range scans, and ValidateAndCommit(reads, writes).
- Changes in a transaction for multiple key-value pairs are made with atomicity and durability.
- Concurrent transactions are processed with strict serializability.
- In contended write-heavy workloads, high scalability for many-core CPUs is provided.

### Notes

---

- LineairDB is not an SQL (Relational) database.
- There is no client-server support in the library (i.e., LineairDB is an embedded database).

### Usage

```c++

#include <lineairdb/lineairdb.h>

int main() {
  LineairDB::Database db;
  db.CreateTable("accounts");

  // Read: the returned tid is the evidence the commit is validated against.
  auto alice = db.Read("accounts", "alice");

  // Commit: hand back what was read and what to install. The read set is
  // revalidated and the writes are installed atomically, or nothing is.
  const std::vector<LineairDB::ExternalReadEntry> reads = {
      {"accounts", "alice", alice.tid, alice.found}};
  const std::vector<LineairDB::ExternalWriteEntry> writes = {
      {"accounts", "bob", "1"}};
  const bool committed =
      db.ValidateAndCommit(reads, writes, {}, {}, LineairDB::CommitPolicy::Sync);
}
```

### Getting the Source

```
git clone --recurse-submodules https://github.com/lineairdb/lineairdb.git
```

### Building

Quick start:

```
mkdir -p build && cd build
cmake -DBUILD_SHARED_LIBS=ON -DCMAKE_BUILD_TYPE=Release .. && make && sudo make install
```

Then you can use LineairDB by including the header `lineairdb/lineairdb.h`.

### Compatibility

We have been tested LineairDB in the following environments:

- Apple clang version 11.0.3
- Clang >= 6 on Linux
- GCC >= 7.5

### Documentation

[The LineairDB library documentation](https://lineairdb.github.io/LineairDB/) is available.

The research paper LineairDB grew out of is available [at this link](https://arxiv.org/abs/1904.08119).

### Contributing

This project welcomes contributions, issues, suggestions, and feature requests.
<br />Feel free to check [issues page](/issues).

### Question & Discussion

If you have any questions, please feel free to ask on slack.
[Join to slack](https://join.slack.com/t/lineairdb/shared_invite/zt-dvf52aoi-45skLlXcdi7IuQcIM8ARKw)
