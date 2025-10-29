#include "lineairdb_transaction.hh"

#include <cassert>
#include <optional>

#include "../common/log.h"

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
  const std::string full_key = build_prefixed_key(key);
  return tx->Read(full_key);
}

std::vector<std::string> LineairDBTransaction::get_all_keys() {
  if (table_is_not_chosen()) return {};

  assert(tx != nullptr);
  std::vector<std::string> keyList;
  const std::string begin_key = db_table_key;
  tx->Scan(begin_key, std::nullopt, [&](auto key, auto) {
    auto stripped = strip_table_prefix(key);
    if (!stripped) return true;  // keys for other tables; stop scanning
    keyList.emplace_back(std::move(*stripped));
    return false;
  });
  return keyList;
}

std::vector<std::string> LineairDBTransaction::get_matching_keys(
    std::string first_key_part) {
  if (table_is_not_chosen()) return {};

  assert(tx != nullptr);
  std::vector<std::string> keyList;
  const std::string search_begin = db_table_key + first_key_part;
  const size_t match_len = first_key_part.size();

  tx->Scan(search_begin, std::nullopt, [&](auto key, auto) {
    auto stripped = strip_table_prefix(key);
    if (!stripped) return true;

    if (match_len == 0 ||
        stripped->compare(0, match_len, first_key_part) == 0) {
      keyList.emplace_back(std::move(*stripped));
      return false;
    }

    // Once we see a key outside the prefix, remaining keys will also be out.
    return true;
  });
  return keyList;
}

bool LineairDBTransaction::write(std::string key, const std::string value) {
  if (table_is_not_chosen()) return false;
  assert(tx != nullptr);
  const std::string full_key = build_prefixed_key(key);
  tx->Write(full_key, reinterpret_cast<const std::byte*>(value.data()),
            value.length());
  return true;
}

bool LineairDBTransaction::delete_value(std::string key) {
  if (table_is_not_chosen()) return false;
  assert(tx != nullptr);
  const std::string full_key = build_prefixed_key(key);
  tx->Write(full_key, nullptr, 0);
  return true;
}

void LineairDBTransaction::begin_transaction() {
  assert(is_not_started());
  tx = &db->BeginTransaction();

  if (thd_is_transaction()) {
    isTransaction = true;
    register_transaction_to_mysql();
  } else {
    register_single_statement_to_mysql();
  }
}

void LineairDBTransaction::set_status_to_abort() {
  if (tx != nullptr) {
    tx->Abort();
  }
}

bool LineairDBTransaction::end_transaction() {
  assert(tx != nullptr);
  bool committed = db->EndTransaction(*tx, [&](auto) {});
  if (!committed) {
    thd_mark_transaction_to_rollback(thread, 1);
  }

  if (isFence) fence();
  delete this;
  return committed;
}

void LineairDBTransaction::fence() const { db->Fence(); }

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
