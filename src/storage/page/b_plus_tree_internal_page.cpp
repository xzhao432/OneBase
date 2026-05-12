#include "onebase/storage/page/b_plus_tree_internal_page.h"
#include <functional>
#include "onebase/common/exception.h"

namespace onebase {

template <typename KeyType, typename ValueType, typename KeyComparator>
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetMaxSize(max_size);
  SetSize(0);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  return array_[index].first;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  array_[index].first = key;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  return array_[index].second;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) {
  array_[index].second = value;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const -> int {
  for (int i = 0; i < GetSize(); i++) {
    if (array_[i].second == value) {
      return i;
    }
  }
  return -1;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comparator) const -> ValueType {
  if (GetSize() <= 0) {
    throw OneBaseException("BPlusTreeInternalPage::Lookup on empty internal page");
  }
  ValueType result = array_[0].second;
  for (int i = 1; i < GetSize(); i++) {
    // if key < array_[i].first, route to previous pointer
    if (comparator(key, array_[i].first)) {
      break;
    }
    result = array_[i].second;
  }
  return result;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(const ValueType &old_value, const KeyType &key,
                                                      const ValueType &new_value) {
  SetSize(2);
  array_[0].second = old_value;
  array_[1].first = key;
  array_[1].second = new_value;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(const ValueType &old_value, const KeyType &key,
                                                      const ValueType &new_value) -> int {
  const int idx = ValueIndex(old_value);
  if (idx < 0) {
    throw OneBaseException("BPlusTreeInternalPage::InsertNodeAfter old_value not found");
  }
  const int insert_pos = idx + 1;
  for (int i = GetSize(); i > insert_pos; i--) {
    array_[i] = array_[i - 1];
  }
  array_[insert_pos] = {key, new_value};
  IncreaseSize(1);
  return GetSize();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
  if (index < 0 || index >= GetSize()) {
    return;
  }
  for (int i = index; i < GetSize() - 1; i++) {
    array_[i] = array_[i + 1];
  }
  IncreaseSize(-1);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() -> ValueType {
  if (GetSize() != 1) {
    throw OneBaseException("RemoveAndReturnOnlyChild requires size==1");
  }
  const ValueType child = array_[0].second;
  SetSize(0);
  return child;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(BPlusTreeInternalPage *recipient, const KeyType &middle_key) {
  // recipient appends: first entry's key is replaced with middle_key
  const int start = recipient->GetSize();
  if (GetSize() <= 0) {
    return;
  }
  // copy array_[0] as a normal entry with middle_key
  recipient->array_[start] = {middle_key, array_[0].second};
  for (int i = 1; i < GetSize(); i++) {
    recipient->array_[start + i] = array_[i];
  }
  recipient->IncreaseSize(GetSize());
  SetSize(0);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage *recipient, const KeyType &middle_key) {
  const int n = GetSize();
  const int split = n / 2;
  // move [split .. n-1] to recipient, but make recipient[0] a dummy key, and keep keys from split+1...
  recipient->SetPageType(IndexPageType::INTERNAL_PAGE);
  recipient->SetMaxSize(GetMaxSize());
  recipient->SetSize(0);
  recipient->SetParentPageId(GetParentPageId());

  const int right_size = n - split;
  // recipient[0] uses middle_key as its (ignored) key, value is the first child pointer of right side
  recipient->array_[0] = {middle_key, array_[split].second};
  for (int i = 1; i < right_size; i++) {
    recipient->array_[i] = array_[split + i];
  }
  recipient->SetSize(right_size);

  // shrink current
  SetSize(split);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key) {
  if (GetSize() <= 0) {
    return;
  }
  // first entry's key is replaced by middle_key when appending
  const int end = recipient->GetSize();
  recipient->array_[end] = {middle_key, array_[0].second};
  recipient->IncreaseSize(1);
  Remove(0);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key) {
  if (GetSize() <= 0) {
    return;
  }
  const int last = GetSize() - 1;
  // shift recipient right by 1
  for (int i = recipient->GetSize(); i > 0; i--) {
    recipient->array_[i] = recipient->array_[i - 1];
  }
  recipient->array_[0] = {middle_key, array_[last].second};
  recipient->IncreaseSize(1);
  SetSize(last);
}

// Explicit template instantiation (keep at end so definitions are visible).
template class BPlusTreeInternalPage<int, page_id_t, std::less<int>>;

}  // namespace onebase
