#include "onebase/storage/page/b_plus_tree_leaf_page.h"
#include <functional>
#include "onebase/common/exception.h"

namespace onebase {

template <typename KeyType, typename ValueType, typename KeyComparator>
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetMaxSize(max_size);
  SetSize(0);
  next_page_id_ = INVALID_PAGE_ID;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  return array_[index].first;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  return array_[index].second;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &comparator) const -> int {
  int l = 0;
  int r = GetSize();
  while (l < r) {
    int m = l + (r - l) / 2;
    // if array_[m].key < key => move right
    if (comparator(array_[m].first, key)) {
      l = m + 1;
    } else {
      r = m;
    }
  }
  return l;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, ValueType *value,
                                         const KeyComparator &comparator) const -> bool {
  const int idx = KeyIndex(key, comparator);
  if (idx < GetSize() && !comparator(key, array_[idx].first) && !comparator(array_[idx].first, key)) {
    if (value != nullptr) {
      *value = array_[idx].second;
    }
    return true;
  }
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value,
                                         const KeyComparator &comparator) -> int {
  const int idx = KeyIndex(key, comparator);
  if (idx < GetSize() && !comparator(key, array_[idx].first) && !comparator(array_[idx].first, key)) {
    return GetSize();
  }
  for (int i = GetSize(); i > idx; i--) {
    array_[i] = array_[i - 1];
  }
  array_[idx] = {key, value};
  IncreaseSize(1);
  return GetSize();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(const KeyType &key,
                                                        const KeyComparator &comparator) -> int {
  const int idx = KeyIndex(key, comparator);
  if (idx >= GetSize() || comparator(key, array_[idx].first) || comparator(array_[idx].first, key)) {
    return GetSize();
  }
  for (int i = idx; i < GetSize() - 1; i++) {
    array_[i] = array_[i + 1];
  }
  IncreaseSize(-1);
  return GetSize();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(BPlusTreeLeafPage *recipient) {
  const int n = GetSize();
  const int split = n / 2;
  const int move_cnt = n - split;
  for (int i = 0; i < move_cnt; i++) {
    recipient->array_[i] = array_[split + i];
  }
  recipient->SetSize(move_cnt);
  SetSize(split);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *recipient) {
  const int start = recipient->GetSize();
  for (int i = 0; i < GetSize(); i++) {
    recipient->array_[start + i] = array_[i];
  }
  recipient->IncreaseSize(GetSize());
  recipient->SetNextPageId(GetNextPageId());
  SetSize(0);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeLeafPage *recipient) {
  if (GetSize() <= 0) {
    return;
  }
  recipient->array_[recipient->GetSize()] = array_[0];
  recipient->IncreaseSize(1);
  for (int i = 0; i < GetSize() - 1; i++) {
    array_[i] = array_[i + 1];
  }
  IncreaseSize(-1);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeLeafPage *recipient) {
  if (GetSize() <= 0) {
    return;
  }
  // shift recipient right
  for (int i = recipient->GetSize(); i > 0; i--) {
    recipient->array_[i] = recipient->array_[i - 1];
  }
  recipient->array_[0] = array_[GetSize() - 1];
  recipient->IncreaseSize(1);
  IncreaseSize(-1);
}

// Explicit template instantiation (keep at end so definitions are visible).
template class BPlusTreeLeafPage<int, RID, std::less<int>>;

}  // namespace onebase
