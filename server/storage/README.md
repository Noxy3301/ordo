# Helios storage

The storage layer of Helios: an embedded transactional key-value engine with
strict serializability, linked into the storage server one directory up. One
process runs one of these, and the query layers reach it only through the RPC
the server exposes.

## Building

```bash
cmake -S server/storage -B build/storage -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTS=ON
cmake --build build/storage
ctest --test-dir build/storage -j1
```

The tests share one working directory and run serially.

## Provenance

Derived from [LineairDB](https://github.com/LineairDB/LineairDB) (Apache-2.0,
Nippon Telegraph and Telephone Corporation), by way of the frozen fork at
<https://github.com/Noxy3301/LineairDB> (commit 66288a1e). `LICENSE`,
`LICENSE-3RD-PARTY.md` and `NOTICE` are kept here. The research paper the
engine grew out of is at <https://arxiv.org/abs/1904.08119>.

This tree is an independently pruned derivative, not LineairDB itself. The
upstream API and documentation do not describe this code.
