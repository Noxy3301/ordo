#include "lineairdb_transaction.hh"

LineairDBTransaction::LineairDBTransaction(THD* thd, 
                                            LineairDBClient* lineairdb_client,
                                            handlerton* lineairdb_hton,
                                            bool isFence) 
    : tx_id(-1), 
      lineairdb_client(lineairdb_client),
      thread(thd), 
      isTransaction(false), 
      hton(lineairdb_hton),
      isFence(isFence) {}

std::string LineairDBTransaction::get_selected_table_name() { return db_table_key; }

void LineairDBTransaction::choose_table(std::string db_table_name) {
  db_table_key = db_table_name;
}

bool LineairDBTransaction::table_is_not_chosen() {
  if (db_table_key.size() == 0) {
    std::cout << "Database and Table is not chosen in LineairDBTransaction" << std::endl;
    return true;
  }
  return false;
}

const std::pair<const std::byte *const, const size_t> 
LineairDBTransaction::read(std::string key) {
  if (table_is_not_chosen()) return std::pair<const std::byte *const, const size_t>{nullptr, 0};
  std::string value = lineairdb_client->tx_read(tx_id, db_table_key + key);
  if (value.empty()) return std::pair<const std::byte *const, const size_t>{nullptr, 0};

  // cache data to maintain pointer validity until transaction ends
  read_cache_[key] = value;
  const std::string& cached_data = read_cache_[key];
  return {reinterpret_cast<const std::byte*>(cached_data.data()), cached_data.size()};
}

std::vector<std::string> 
LineairDBTransaction::get_all_keys() {
  if (table_is_not_chosen()) return {};

  std::vector<std::string> keyList = lineairdb_client->tx_scan(tx_id, db_table_key, "");
  return keyList;
}

std::vector<std::string> 
LineairDBTransaction::get_matching_keys(std::string first_key_part) {
  if (table_is_not_chosen()) return {};

  std::vector<std::string> keyList = lineairdb_client->tx_scan(tx_id, db_table_key, first_key_part);
  return keyList;
}

bool LineairDBTransaction::write(std::string key, const std::string value) {
  if (table_is_not_chosen()) return false;
  return lineairdb_client->tx_write(tx_id, db_table_key + key, value);
}

bool LineairDBTransaction::delete_value(std::string key) {
  if (table_is_not_chosen()) return false;
  
  // If key already starts with db_table_key, don't add it again
  std::string full_key;
  if (key.find(db_table_key) == 0) {
    full_key = key;  // key is already a full key
  } else {
    full_key = db_table_key + key;  // key needs db_table_key prefix
  }
  
  return lineairdb_client->tx_write(tx_id, full_key, "");
}


void LineairDBTransaction::begin_transaction() {
  assert(is_not_started());
  tx_id = lineairdb_client->tx_begin_transaction();
  // TODO: maybe need error handling when tx_id == -1
  assert(tx_id != -1);

  if (thd_is_transaction()) {
    isTransaction = true;
    register_transaction_to_mysql();
  }
  else {
    register_single_statement_to_mysql();
  }
}

void LineairDBTransaction::set_status_to_abort() {
  lineairdb_client->tx_abort(tx_id);
}

void LineairDBTransaction::end_transaction() {
  assert(tx_id != -1);
  lineairdb_client->db_end_transaction(tx_id, isFence);
  if (isFence) lineairdb_client->db_fence();
  delete this;
}

void LineairDBTransaction::fence() const { lineairdb_client->db_fence(); }




bool LineairDBTransaction::thd_is_transaction() const {
  return ::thd_test_options(thread, OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN | OPTION_TABLE_LOCK);
}

void LineairDBTransaction::register_transaction_to_mysql() {
  const ulonglong threadID = static_cast<ulonglong>(thread->thread_id());
  ::trans_register_ha(thread, isTransaction, hton, &threadID);
}

void LineairDBTransaction::register_single_statement_to_mysql() {
  register_transaction_to_mysql();
}