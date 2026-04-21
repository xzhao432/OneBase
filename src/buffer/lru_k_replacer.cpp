#include "onebase/buffer/lru_k_replacer.h"
#include "onebase/common/exception.h"

namespace onebase {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k)
    : max_frames_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> guard(latch_);

  bool found_inf{false};
  bool found_finite{false};
  frame_id_t inf_victim{0};
  size_t inf_first_ts{0};

  frame_id_t finite_victim{0};
  size_t finite_best_distance{0};

  for (const auto &kv : entries_) {
    const auto fid = kv.first;
    const auto &entry = kv.second;
    if (!entry.is_evictable_) {
      continue;
    }
    if (entry.history_.size() < k_) {
      const size_t first_ts = entry.history_.empty() ? 0 : entry.history_.front();
      if (!found_inf || first_ts < inf_first_ts || (first_ts == inf_first_ts && fid < inf_victim)) {
        found_inf = true;
        inf_victim = fid;
        inf_first_ts = first_ts;
      }
      continue;
    }

    const size_t distance = current_timestamp_ - entry.history_.front();
    if (!found_finite || distance > finite_best_distance ||
        (distance == finite_best_distance && fid < finite_victim)) {
      found_finite = true;
      finite_victim = fid;
      finite_best_distance = distance;
    }
  }

  if (!found_inf && !found_finite) {
    return false;
  }

  const frame_id_t victim = found_inf ? inf_victim : finite_victim;
  if (frame_id != nullptr) {
    *frame_id = victim;
  }
  auto it = entries_.find(victim);
  if (it != entries_.end()) {
    if (it->second.is_evictable_) {
      curr_size_--;
    }
    entries_.erase(it);
  }
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  if (frame_id < 0 || static_cast<size_t>(frame_id) >= max_frames_) {
    throw OneBaseException("LRUKReplacer::RecordAccess frame_id out of range",
                           ExceptionType::OUT_OF_RANGE);
  }

  std::scoped_lock<std::mutex> guard(latch_);
  auto &entry = entries_[frame_id];
  entry.history_.push_back(current_timestamp_);
  if (entry.history_.size() > k_) {
    entry.history_.pop_front();
  }
  current_timestamp_++;
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::mutex> guard(latch_);
  auto it = entries_.find(frame_id);
  if (it == entries_.end()) {
    return;
  }
  const bool was_evictable = it->second.is_evictable_;
  if (was_evictable != set_evictable) {
    if (set_evictable) {
      curr_size_++;
    } else {
      curr_size_--;
    }
    it->second.is_evictable_ = set_evictable;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> guard(latch_);
  auto it = entries_.find(frame_id);
  if (it == entries_.end()) {
    return;
  }
  if (!it->second.is_evictable_) {
    throw OneBaseException("LRUKReplacer::Remove called on a non-evictable frame",
                           ExceptionType::INVALID);
  }
  curr_size_--;
  entries_.erase(it);
}

auto LRUKReplacer::Size() const -> size_t {
  return curr_size_;
}

}  // namespace onebase
