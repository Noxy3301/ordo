#include "lineairdb_transaction.hh"

#include <cassert>
#include <chrono>
#include <cstdint>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <mutex>
#include <optional>

#include "../common/log.h"

namespace {

enum class MessageType : uint32_t {
  kUnknown = 0,
  kTxBegin = 1,
  kTxAbort = 2,
  kTxRead = 3,
  kTxWrite = 4,
  kTxScan = 5,
  kDbFence = 6,
  kDbEnd = 7,
};

const char* MessageTypeToString(MessageType type) {
  switch (type) {
    case MessageType::kTxBegin:
      return "TX_BEGIN_TRANSACTION";
    case MessageType::kTxAbort:
      return "TX_ABORT";
    case MessageType::kTxRead:
      return "TX_READ";
    case MessageType::kTxWrite:
      return "TX_WRITE";
    case MessageType::kTxScan:
      return "TX_SCAN";
    case MessageType::kDbFence:
      return "DB_FENCE";
    case MessageType::kDbEnd:
      return "DB_END_TRANSACTION";
    default:
      return "UNKNOWN";
  }
}

const std::string& GetTimingLogPath() {
  static const std::string path = []() {
    const char* env = std::getenv("LINEAIRDB_PROTOBUF_TIMING_LOG");
    if (env && env[0] != '\0') {
      return std::string(env);
    }
    return std::string("/home/noxy/ordo/lineairdb_logs/protobuf_timing.log");
  }();
  return path;
}

void AppendTimingRecord(MessageType type,
                        std::chrono::steady_clock::time_point total_start,
                        std::chrono::steady_clock::time_point total_end,
                        std::chrono::steady_clock::time_point exec_start,
                        std::chrono::steady_clock::time_point exec_end,
                        size_t request_bytes,
                        size_t response_bytes) {
  const std::string& path = GetTimingLogPath();
  if (path.empty()) return;

  static std::mutex file_mutex;
  std::lock_guard<std::mutex> lock(file_mutex);

  std::ofstream out(path, std::ios::app);
  if (!out) return;

  auto to_ns = [](const std::chrono::steady_clock::time_point& tp) {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               tp.time_since_epoch())
        .count();
  };
  const long long exec_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                                exec_end - exec_start)
                                .count();
  const long long total_ns =
      std::chrono::duration_cast<std::chrono::nanoseconds>(total_end - total_start)
          .count();
  const auto start_ns = to_ns(total_start);
  const auto end_ns = to_ns(total_end);

  out << "message=" << MessageTypeToString(type)
      << " serialize_start_ns=" << start_ns
      << " serialize_end_ns=" << start_ns
      << " deserialize_start_ns=" << end_ns
      << " deserialize_end_ns=" << end_ns
      << " serialize_ns=0"
      << " deserialize_ns=0"
      << " send_ns=0"
      << " recv_ns=0"
      << " roundtrip_ns=" << total_ns
      << " lineairdb_exec_ns=" << exec_ns
      << " request_bytes=" << request_bytes
      << " response_bytes=" << response_bytes
      << " parse_ok=1" << std::endl;
}

}  // namespace

LineairDBTransaction::LineairDBTransaction(THD* thd,
                                           LineairDB::Database* ldb,
                                           handlerton* lineairdb_hton,
                                           bool isFence)
    : tx(nullptr),
      db(ldb),
      thread(thd),
      isTransaction(false),
      hton(lineairdb_hton),
      isFence(isFence) {}

std::string LineairDBTransaction::get_selected_table_name() {
  return db_table_key;
}

void LineairDBTransaction::choose_table(std::string db_table_name) {
  db_table_key = db_table_name;
}

bool LineairDBTransaction::table_is_not_chosen() {
  if (db_table_key.empty()) {
    LOG_WARNING("Database and Table is not chosen in LineairDBTransaction");
    return true;
  }
  return false;
}

const std::pair<const std::byte* const, const size_t>
LineairDBTransaction::read(std::string key) {
  if (table_is_not_chosen()) {
    return {nullptr, 0};
  }
  assert(tx != nullptr);
  auto total_start = std::chrono::steady_clock::now();
  const std::string full_key = build_prefixed_key(key);
  auto exec_start = std::chrono::steady_clock::now();
  const auto result = tx->Read(full_key);
  auto exec_end = std::chrono::steady_clock::now();
  auto total_end = exec_end;
  AppendTimingRecord(MessageType::kTxRead, total_start, total_end, exec_start,
                     exec_end, full_key.size(), result.second);
  return result;
}

std::vector<std::string> LineairDBTransaction::get_all_keys() {
  if (table_is_not_chosen()) return {};

  assert(tx != nullptr);
  std::vector<std::string> keyList;
  const std::string begin_key = db_table_key;
  size_t response_bytes = 0;
  auto total_start = std::chrono::steady_clock::now();
  auto exec_start = total_start;
  tx->Scan(begin_key, std::nullopt, [&](auto key, auto) {
    auto stripped = strip_table_prefix(key);
    if (!stripped) return true;  // keys for other tables; stop scanning
    const size_t len = stripped->size();
    keyList.emplace_back(std::move(*stripped));
    response_bytes += len;
    return false;
  });
  auto exec_end = std::chrono::steady_clock::now();
  auto total_end = exec_end;
  AppendTimingRecord(MessageType::kTxScan, total_start, total_end, exec_start,
                     exec_end, begin_key.size(), response_bytes);
  return keyList;
}

std::vector<std::string> LineairDBTransaction::get_matching_keys(
    std::string first_key_part) {
  if (table_is_not_chosen()) return {};

  assert(tx != nullptr);
  std::vector<std::string> keyList;
  const std::string search_begin = db_table_key + first_key_part;
  const size_t match_len = first_key_part.size();
  size_t response_bytes = 0;
  auto total_start = std::chrono::steady_clock::now();
  auto exec_start = total_start;
  tx->Scan(search_begin, std::nullopt, [&](auto key, auto) {
    auto stripped = strip_table_prefix(key);
    if (!stripped) return true;

    if (match_len == 0 ||
        stripped->compare(0, match_len, first_key_part) == 0) {
      const size_t len = stripped->size();
      keyList.emplace_back(std::move(*stripped));
      response_bytes += len;
      return false;
    }

    // Once we see a key outside the prefix, remaining keys will also be out.
    return true;
  });
  auto exec_end = std::chrono::steady_clock::now();
  auto total_end = exec_end;
  AppendTimingRecord(MessageType::kTxScan, total_start, total_end, exec_start,
                     exec_end, search_begin.size(), response_bytes);
  return keyList;
}

bool LineairDBTransaction::write(std::string key, const std::string value) {
  if (table_is_not_chosen()) return false;
  assert(tx != nullptr);
  auto total_start = std::chrono::steady_clock::now();
  const std::string full_key = build_prefixed_key(key);
  auto exec_start = std::chrono::steady_clock::now();
  tx->Write(full_key, reinterpret_cast<const std::byte*>(value.data()),
            value.length());
  auto exec_end = std::chrono::steady_clock::now();
  auto total_end = exec_end;
  AppendTimingRecord(MessageType::kTxWrite, total_start, total_end, exec_start,
                     exec_end, full_key.size() + value.size(), 0);
  return true;
}

bool LineairDBTransaction::delete_value(std::string key) {
  if (table_is_not_chosen()) return false;
  assert(tx != nullptr);
  auto total_start = std::chrono::steady_clock::now();
  const std::string full_key = build_prefixed_key(key);
  auto exec_start = std::chrono::steady_clock::now();
  tx->Write(full_key, nullptr, 0);
  auto exec_end = std::chrono::steady_clock::now();
  auto total_end = exec_end;
  AppendTimingRecord(MessageType::kTxWrite, total_start, total_end, exec_start,
                     exec_end, full_key.size(), 0);
  return true;
}

void LineairDBTransaction::begin_transaction() {
  assert(is_not_started());
  auto total_start = std::chrono::steady_clock::now();
  auto exec_start = total_start;
  tx = &db->BeginTransaction();
  auto exec_end = std::chrono::steady_clock::now();

  if (thd_is_transaction()) {
    isTransaction = true;
    register_transaction_to_mysql();
  } else {
    register_single_statement_to_mysql();
  }
  auto total_end = std::chrono::steady_clock::now();
  AppendTimingRecord(MessageType::kTxBegin, total_start, total_end, exec_start,
                     exec_end, 0, 0);
}

void LineairDBTransaction::set_status_to_abort() {
  if (tx != nullptr) {
    auto total_start = std::chrono::steady_clock::now();
    auto exec_start = total_start;
    tx->Abort();
    auto exec_end = std::chrono::steady_clock::now();
    auto total_end = exec_end;
    AppendTimingRecord(MessageType::kTxAbort, total_start, total_end,
                       exec_start, exec_end, 0, 0);
  }
}

bool LineairDBTransaction::end_transaction() {
  assert(tx != nullptr);
  auto total_start = std::chrono::steady_clock::now();
  auto exec_start = total_start;
  bool committed = db->EndTransaction(*tx, [&](auto) {});
  auto exec_end = std::chrono::steady_clock::now();
  if (!committed) {
    thd_mark_transaction_to_rollback(thread, 1);
  }

  auto total_end = std::chrono::steady_clock::now();
  AppendTimingRecord(MessageType::kDbEnd, total_start, total_end, exec_start,
                     exec_end, 0, 0);

  if (isFence) fence();
  delete this;
  return committed;
}

void LineairDBTransaction::fence() const {
  auto total_start = std::chrono::steady_clock::now();
  auto exec_start = total_start;
  db->Fence();
  auto exec_end = std::chrono::steady_clock::now();
  auto total_end = exec_end;
  AppendTimingRecord(MessageType::kDbFence, total_start, total_end, exec_start,
                     exec_end, 0, 0);
}

bool LineairDBTransaction::thd_is_transaction() const {
  return ::thd_test_options(thread,
                            OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN |
                                OPTION_TABLE_LOCK);
}

void LineairDBTransaction::register_transaction_to_mysql() {
  const ulonglong threadID = static_cast<ulonglong>(thread->thread_id());
  ::trans_register_ha(thread, isTransaction, hton, &threadID);
}

void LineairDBTransaction::register_single_statement_to_mysql() {
  register_transaction_to_mysql();
}

std::string LineairDBTransaction::build_prefixed_key(
    const std::string& key) const {
  if (db_table_key.empty()) return key;
  if (key.rfind(db_table_key, 0) == 0) {
    return key;
  }
  return db_table_key + key;
}

std::optional<std::string> LineairDBTransaction::strip_table_prefix(
    std::string_view key) const {
  if (db_table_key.empty()) return std::string(key);
  if (key.rfind(db_table_key, 0) != 0) {
    return std::nullopt;
  }
  return std::string(key.substr(db_table_key.size()));
}
