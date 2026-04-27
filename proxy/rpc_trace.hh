#ifndef LINEAIRDB_RPC_TRACE_HH
#define LINEAIRDB_RPC_TRACE_HH

#include <chrono>
#include <cstdint>
#include <fstream>
#include <map>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "lineairdb_proxy.hh"  // MessageType

// Per-RPC record captured by send_message_with_header.
struct RpcEntry {
  MessageType type;
  uint64_t us;          // duration microseconds
  uint64_t off_us;      // offset from tx_begin in microseconds
  uint32_t req_b;       // serialized request bytes (proto)
  uint32_t resp_b;      // serialized response bytes (proto/binary)
  uint32_t stmt_idx;    // index into statements_; UINT32_MAX if pre-stmt
  std::string meta;     // optional: key, index_name, prefix, etc.
};

// Per-statement record. SQL captured at statement boundary (external_lock /
// start_stmt) when thd_query() differs from previously-recorded last_sql.
struct StatementEntry {
  std::string sql;
  uint64_t started_off_us;
  uint32_t first_rpc_idx;  // first RPC under this statement
  uint32_t last_rpc_idx;   // last RPC under this statement (inclusive)
};

// Per-LineairDBTransaction trace state. The proxy's send_message_with_header
// updates the active TxRpcTrace via a current_tx_ pointer; transactions
// register themselves at begin_transaction and unregister at end.
class TxRpcTrace {
 public:
  // Reset state and start tracing for a new transaction. No-op if the
  // RpcTraceLogger is disabled (env not set), so callers can start
  // unconditionally.
  void start(int64_t tx_id, std::thread::id tid);

  // Update tx_id once the begin-RPC has returned the server-assigned ID.
  // Useful because we want to start the trace before TX_BEGIN_TRANSACTION
  // is sent (so the begin RPC itself appears in the trace).
  void set_tx_id(int64_t tx_id) { tx_id_ = tx_id; }

  // Mark a new statement boundary. Caller is responsible for SQL extraction
  // (thd->query()). If sql matches last recorded, this is a no-op (dedupes
  // multiple external_lock calls per statement).
  void on_stmt(const std::string& sql);

  // Append an RPC record. Called from send_message_with_header.
  void record(MessageType type, uint64_t us, uint32_t req_b,
              uint32_t resp_b, const std::string& meta);

  // Finalize the trace and return the JSONL line. Caller writes to logger.
  std::string finalize_jsonl(bool committed);

  bool active() const { return active_; }

 private:
  bool active_ = false;
  int64_t tx_id_ = -1;
  std::thread::id tid_;
  std::chrono::steady_clock::time_point started_;
  std::chrono::system_clock::time_point started_wall_;
  std::vector<RpcEntry> rpcs_;
  std::vector<StatementEntry> statements_;

  // Per-type aggregation accumulated in lockstep with rpcs_.
  struct Agg {
    uint32_t n = 0;
    uint64_t us = 0;
    uint64_t req_b = 0;
    uint64_t resp_b = 0;
  };

  std::map<MessageType, Agg> by_type_;
};

// Singleton logger: opens a JSONL file (one per process, named by pid)
// when ENABLE_RPC_TRACE is non-empty in the environment. Serializes writes
// with a mutex (per-conn proxies are different threads).
class RpcTraceLogger {
 public:
  static RpcTraceLogger& instance();
  bool enabled() const { return enabled_; }
  void log_line(const std::string& jsonl);

 private:
  RpcTraceLogger();
  ~RpcTraceLogger();
  bool enabled_ = false;
  std::mutex mu_;
  std::ofstream file_;
};

// Helper exposed for unit testing / readability.
const char* message_type_name(MessageType t);
std::string json_escape(const std::string& s, size_t max_len = 1024);

#endif  // LINEAIRDB_RPC_TRACE_HH
