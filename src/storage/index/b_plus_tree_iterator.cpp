#include "onebase/storage/index/b_plus_tree_iterator.h"
#include <functional>
#include "onebase/common/exception.h"
#include "onebase/buffer/buffer_pool_manager.h"
#include "onebase/storage/page/b_plus_tree_leaf_page.h"

namespace onebase {

template <typename KeyType, typename ValueType, typename KeyComparator>
BPLUSTREE_ITERATOR_TYPE::BPlusTreeIterator(page_id_t page_id, int index, BufferPoolManager *bpm)
    : page_id_(page_id), index_(index), bpm_(bpm) {}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto BPLUSTREE_ITERATOR_TYPE::IsEnd() const -> bool {
  return page_id_ == INVALID_PAGE_ID;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto BPLUSTREE_ITERATOR_TYPE::operator*() -> const std::pair<KeyType, ValueType> & {
  if (IsEnd()) {
    throw OneBaseException("BPlusTreeIterator::operator* on end iterator");
  }
  if (bpm_ == nullptr) {
    throw OneBaseException("BPlusTreeIterator::operator* requires BufferPoolManager");
  }
  auto *page = bpm_->FetchPage(page_id_);
  if (page == nullptr) {
    throw OneBaseException("BPlusTreeIterator::operator* failed to fetch page");
  }
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;
  auto *leaf = reinterpret_cast<LeafPage *>(page->GetData());
  current_ = {leaf->KeyAt(index_), leaf->ValueAt(index_)};
  bpm_->UnpinPage(page_id_, false);
  return current_;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto BPLUSTREE_ITERATOR_TYPE::operator++() -> BPlusTreeIterator & {
  if (IsEnd()) {
    return *this;
  }
  if (bpm_ == nullptr) {
    throw OneBaseException("BPlusTreeIterator::operator++ requires BufferPoolManager");
  }
  auto *page = bpm_->FetchPage(page_id_);
  if (page == nullptr) {
    throw OneBaseException("BPlusTreeIterator::operator++ failed to fetch page");
  }
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;
  auto *leaf = reinterpret_cast<LeafPage *>(page->GetData());
  index_++;
  if (index_ >= leaf->GetSize()) {
    const page_id_t next = leaf->GetNextPageId();
    bpm_->UnpinPage(page_id_, false);
    page_id_ = next;
    index_ = 0;
    return *this;
  }
  bpm_->UnpinPage(page_id_, false);
  return *this;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto BPLUSTREE_ITERATOR_TYPE::operator==(const BPlusTreeIterator &other) const -> bool {
  return page_id_ == other.page_id_ && index_ == other.index_;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto BPLUSTREE_ITERATOR_TYPE::operator!=(const BPlusTreeIterator &other) const -> bool {
  return !(*this == other);
}

// Explicit template instantiation (keep at end so definitions are visible).
template class BPlusTreeIterator<int, RID, std::less<int>>;

}  // namespace onebase
