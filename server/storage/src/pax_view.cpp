// pax_view.cpp
// PAX schema installation and the fenced columnar read view.

#include <chrono>
#include <string>
#include <utility>

#include "database_impl.h"
#include "pax/version_store.hpp"
#include "util/debug_sync.hpp"

namespace helios::storage {

pax::PaxStore *Database::Impl::GetPaxStore(const std::string_view table_name) {
  auto table = GetTable(table_name);
  if (!table.has_value()) return nullptr;
  return table.value()->GetPaxStore();
}

Database::PaxReadView Database::Impl::AcquirePaxReadView(
    uint32_t fence_timeout_ms) {
  Database::PaxReadView handle;
  auto token = pax::VersionStore::Global().BeginCapture();
  if (!token.valid) {
    handle.error =
        "columnar read view rejected: the active capture generation is "
        "poisoned";
    return handle;
  }
  // Fence order (do not reorder): arm the capture flag (seq_cst
  // increment in BeginCapture), then load the cut epoch. An install
  // whose flag check missed the capture belongs to a commit with epoch
  // <= cut, and a thread online in epoch e keeps the global epoch at or
  // below e + 1; once the global epoch reaches cut + 2 those installs
  // have drained. Refuse when the fence target would reach or cross
  // the high-water mark, compared without addition to stay exact at
  // the numeric limit.
  const EpochNumber cut = epoch_framework_.GetGlobalEpoch();
  if (cut >= epoch::EpochFramework::kEpochHighWater - 2) {
    pax::VersionStore::Global().EndCapture(token);
    handle.error =
        "columnar read view rejected: epoch space is near its wrap "
        "high-water mark, restart the server";
    return handle;
  }
  if (!epoch_framework_.WaitGlobalEpochAtLeast(
          cut + 2, std::chrono::milliseconds(fence_timeout_ms))) {
    pax::VersionStore::Global().EndCapture(token);
    handle.error =
        "columnar read view fence timed out; a long-running transaction is "
        "holding the epoch";
    return handle;
  }
  // Test hook: holds the read view open between the fence and the scan.
  HELIOS_DEBUG_SYNC("pax_read_view.after_fence");
  // A poison landing during acquisition must fail it here; callers
  // treat a valid handle as a serviceable read view.
  if (pax::VersionStore::Global().Poisoned(token)) {
    pax::VersionStore::Global().EndCapture(token);
    handle.error = "columnar read view poisoned during acquisition";
    return handle;
  }
  handle.valid = true;
  handle.cut_epoch = cut;
  handle.token = token.id;
  return handle;
}

void Database::Impl::ReleasePaxReadView(const Database::PaxReadView &view) {
  if (!view.valid) return;
  pax::VersionStore::ReadViewToken token;
  token.id = view.token;
  token.valid = true;
  pax::VersionStore::Global().EndCapture(token);
}

bool Database::Impl::PaxReadViewPoisoned(
    const Database::PaxReadView &view) const {
  if (!view.valid) return true;
  if (epoch_framework_.GetGlobalEpoch() - view.cut_epoch >=
      kPaxReadViewEpochLifetime) {
    return true;  // expired: comparisons could leave the wrap-free window
  }
  pax::VersionStore::ReadViewToken token;
  token.id = view.token;
  token.valid = true;
  return pax::VersionStore::Global().Poisoned(token);
}

bool Database::Impl::InstallPaxSchema(
    const std::string_view table_name,
    const std::vector<uint32_t> &field_max_bytes,
    const std::vector<uint8_t> &field_kind,
    const std::vector<int8_t> &field_scale) {
  if (!config_.enable_pax_storage) return false;
  if (field_max_bytes.empty()) return false;
  auto table = GetTable(table_name);
  if (!table.has_value()) return false;
  pax::TableSchema schema;
  schema.table_name = std::string(table_name);
  schema.field_max_bytes = field_max_bytes;
  // Typed cells only when the kinds vector matches the field count; otherwise
  // every field stays UNTYPED (byte-identical to the untyped layout).
  if (field_kind.size() == field_max_bytes.size()) {
    schema.field_kind = field_kind;
    if (field_scale.size() == field_max_bytes.size())
      schema.field_scale = field_scale;
    else
      schema.field_scale.assign(field_max_bytes.size(), 0);
  }
  return table.value()->InstallPaxSchema(std::move(schema));
}

}  // namespace helios::storage
