#include "onebase/storage/index/b_plus_tree.h"
#include "onebase/storage/index/b_plus_tree_iterator.h"
#include <functional>
#include "onebase/common/exception.h"

namespace onebase {

namespace {
template <typename KeyType, typename ValueType, typename KeyComparator>
auto AsBPlusTreePage(Page *page) -> BPlusTreePage * {
  return reinterpret_cast<BPlusTreePage *>(page->GetData());
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto AsLeaf(Page *page) -> BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> * {
  return reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(page->GetData());
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto AsInternal(Page *page) -> BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> * {
  return reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(page->GetData());
}
}  // namespace

template <typename KeyType, typename ValueType, typename KeyComparator>
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *bpm, const KeyComparator &comparator,
                           int leaf_max_size, int internal_max_size)
    : Index(std::move(name)), bpm_(bpm), comparator_(comparator),
      leaf_max_size_(leaf_max_size), internal_max_size_(internal_max_size) {
  if (leaf_max_size_ == 0) {
    leaf_max_size_ = static_cast<int>(
        (ONEBASE_PAGE_SIZE - sizeof(BPlusTreePage) - sizeof(page_id_t)) /
        (sizeof(KeyType) + sizeof(ValueType)));
  }
  if (internal_max_size_ == 0) {
    internal_max_size_ = static_cast<int>(
        (ONEBASE_PAGE_SIZE - sizeof(BPlusTreePage)) /
        (sizeof(KeyType) + sizeof(page_id_t)));
  }
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  return root_page_id_ == INVALID_PAGE_ID;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value) -> bool {
  // Helper to find leaf page id for a key (optionally leftmost for Begin()).
  auto find_leaf = [&](const KeyType *search_key) -> page_id_t {
    page_id_t pid = root_page_id_;
    while (pid != INVALID_PAGE_ID) {
      Page *page = bpm_->FetchPage(pid);
      auto *node = AsBPlusTreePage<KeyType, ValueType, KeyComparator>(page);
      if (node->IsLeafPage()) {
        bpm_->UnpinPage(pid, false);
        return pid;
      }
      auto *internal = AsInternal<KeyType, ValueType, KeyComparator>(page);
      page_id_t next = internal->ValueAt(0);
      if (search_key != nullptr) {
        next = internal->Lookup(*search_key, comparator_);
      }
      bpm_->UnpinPage(pid, false);
      pid = next;
    }
    return INVALID_PAGE_ID;
  };

  // Insert into parent after split.
  std::function<void(page_id_t, const KeyType &, page_id_t)> insert_into_parent;

  insert_into_parent = [&](page_id_t left_pid, const KeyType &push_key, page_id_t right_pid) {
    Page *left_page = bpm_->FetchPage(left_pid);
    auto *left_node = AsBPlusTreePage<KeyType, ValueType, KeyComparator>(left_page);
    const page_id_t parent_pid = left_node->GetParentPageId();
    bpm_->UnpinPage(left_pid, false);

    if (parent_pid == INVALID_PAGE_ID) {
      // create new root
      page_id_t new_root_pid;
      Page *new_root_page = bpm_->NewPage(&new_root_pid);
      auto *root = AsInternal<KeyType, ValueType, KeyComparator>(new_root_page);
      root->Init(internal_max_size_);
      root->SetParentPageId(INVALID_PAGE_ID);
      root->PopulateNewRoot(left_pid, push_key, right_pid);
      root_page_id_ = new_root_pid;

      // update children's parent pointers
      Page *l = bpm_->FetchPage(left_pid);
      AsBPlusTreePage<KeyType, ValueType, KeyComparator>(l)->SetParentPageId(new_root_pid);
      bpm_->UnpinPage(left_pid, true);
      Page *r = bpm_->FetchPage(right_pid);
      AsBPlusTreePage<KeyType, ValueType, KeyComparator>(r)->SetParentPageId(new_root_pid);
      bpm_->UnpinPage(right_pid, true);

      bpm_->UnpinPage(new_root_pid, true);
      return;
    }

    Page *parent_page = bpm_->FetchPage(parent_pid);
    auto *parent = AsInternal<KeyType, ValueType, KeyComparator>(parent_page);
    parent->InsertNodeAfter(left_pid, push_key, right_pid);
    // set right child's parent
    Page *r = bpm_->FetchPage(right_pid);
    AsBPlusTreePage<KeyType, ValueType, KeyComparator>(r)->SetParentPageId(parent_pid);
    bpm_->UnpinPage(right_pid, true);

    if (parent->GetSize() <= parent->GetMaxSize()) {
      bpm_->UnpinPage(parent_pid, true);
      return;
    }

    // split internal
    const int n = parent->GetSize();
    const int split = n / 2;
    const KeyType middle_key = parent->KeyAt(split);

    page_id_t new_internal_pid;
    Page *new_internal_page = bpm_->NewPage(&new_internal_pid);
    auto *new_internal = AsInternal<KeyType, ValueType, KeyComparator>(new_internal_page);
    new_internal->Init(internal_max_size_);
    new_internal->SetParentPageId(parent->GetParentPageId());

    // move right side to new internal (including pointer at split)
    new_internal->SetSize(0);
    const int right_size = n - split;
    // index 0: dummy key, value = parent->ValueAt(split)
    new_internal->SetValueAt(0, parent->ValueAt(split));
    new_internal->SetKeyAt(0, middle_key);
    for (int i = 1; i < right_size; i++) {
      new_internal->SetKeyAt(i, parent->KeyAt(split + i));
      new_internal->SetValueAt(i, parent->ValueAt(split + i));
    }
    new_internal->SetSize(right_size);
    parent->SetSize(split);

    // fix children's parent pointers for new_internal
    for (int i = 0; i < new_internal->GetSize(); i++) {
      page_id_t child_pid = new_internal->ValueAt(i);
      Page *child_page = bpm_->FetchPage(child_pid);
      AsBPlusTreePage<KeyType, ValueType, KeyComparator>(child_page)->SetParentPageId(new_internal_pid);
      bpm_->UnpinPage(child_pid, true);
    }

    bpm_->UnpinPage(parent_pid, true);
    bpm_->UnpinPage(new_internal_pid, true);

    insert_into_parent(parent_pid, middle_key, new_internal_pid);
  };

  if (IsEmpty()) {
    page_id_t leaf_pid;
    Page *leaf_page = bpm_->NewPage(&leaf_pid);
    auto *leaf = AsLeaf<KeyType, ValueType, KeyComparator>(leaf_page);
    leaf->Init(leaf_max_size_);
    leaf->SetParentPageId(INVALID_PAGE_ID);
    leaf->Insert(key, value, comparator_);
    root_page_id_ = leaf_pid;
    bpm_->UnpinPage(leaf_pid, true);
    return true;
  }

  const page_id_t leaf_pid = find_leaf(&key);
  Page *leaf_page = bpm_->FetchPage(leaf_pid);
  auto *leaf = AsLeaf<KeyType, ValueType, KeyComparator>(leaf_page);
  const int old_size = leaf->GetSize();
  leaf->Insert(key, value, comparator_);
  if (leaf->GetSize() == old_size) {
    bpm_->UnpinPage(leaf_pid, false);
    return false;  // duplicate
  }

  if (leaf->GetSize() <= leaf->GetMaxSize()) {
    bpm_->UnpinPage(leaf_pid, true);
    return true;
  }

  // split leaf
  page_id_t new_leaf_pid;
  Page *new_leaf_page = bpm_->NewPage(&new_leaf_pid);
  auto *new_leaf = AsLeaf<KeyType, ValueType, KeyComparator>(new_leaf_page);
  new_leaf->Init(leaf_max_size_);
  new_leaf->SetParentPageId(leaf->GetParentPageId());

  new_leaf->SetNextPageId(leaf->GetNextPageId());
  leaf->SetNextPageId(new_leaf_pid);
  leaf->MoveHalfTo(new_leaf);
  const KeyType push_key = new_leaf->KeyAt(0);

  bpm_->UnpinPage(leaf_pid, true);
  bpm_->UnpinPage(new_leaf_pid, true);

  insert_into_parent(leaf_pid, push_key, new_leaf_pid);
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void BPLUSTREE_TYPE::Remove(const KeyType &key) {
  if (IsEmpty()) {
    return;
  }

  auto find_leaf = [&](const KeyType &search_key) -> page_id_t {
    page_id_t pid = root_page_id_;
    while (pid != INVALID_PAGE_ID) {
      Page *page = bpm_->FetchPage(pid);
      auto *node = AsBPlusTreePage<KeyType, ValueType, KeyComparator>(page);
      if (node->IsLeafPage()) {
        bpm_->UnpinPage(pid, false);
        return pid;
      }
      auto *internal = AsInternal<KeyType, ValueType, KeyComparator>(page);
      page_id_t next = internal->Lookup(search_key, comparator_);
      bpm_->UnpinPage(pid, false);
      pid = next;
    }
    return INVALID_PAGE_ID;
  };

  std::function<void(page_id_t)> adjust_root;
  adjust_root = [&](page_id_t root_pid) {
    Page *root_page = bpm_->FetchPage(root_pid);
    auto *root_node = AsBPlusTreePage<KeyType, ValueType, KeyComparator>(root_page);
    if (root_node->IsLeafPage()) {
      auto *leaf = AsLeaf<KeyType, ValueType, KeyComparator>(root_page);
      if (leaf->GetSize() == 0) {
        bpm_->UnpinPage(root_pid, false);
        bpm_->DeletePage(root_pid);
        root_page_id_ = INVALID_PAGE_ID;
        return;
      }
      bpm_->UnpinPage(root_pid, false);
      return;
    }
    auto *internal = AsInternal<KeyType, ValueType, KeyComparator>(root_page);
    if (internal->GetSize() == 1) {
      const page_id_t child = internal->ValueAt(0);
      bpm_->UnpinPage(root_pid, false);
      bpm_->DeletePage(root_pid);
      root_page_id_ = child;
      Page *child_page = bpm_->FetchPage(child);
      AsBPlusTreePage<KeyType, ValueType, KeyComparator>(child_page)->SetParentPageId(INVALID_PAGE_ID);
      bpm_->UnpinPage(child, true);
      return;
    }
    bpm_->UnpinPage(root_pid, false);
  };

  std::function<void(page_id_t)> coalesce_or_redistribute;

  auto fix_internal_children_parent = [&](BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *internal,
                                          page_id_t internal_pid) {
    for (int i = 0; i < internal->GetSize(); i++) {
      const page_id_t child_pid = internal->ValueAt(i);
      Page *child_page = bpm_->FetchPage(child_pid);
      AsBPlusTreePage<KeyType, ValueType, KeyComparator>(child_page)->SetParentPageId(internal_pid);
      bpm_->UnpinPage(child_pid, true);
    }
  };

  coalesce_or_redistribute = [&](page_id_t node_pid) {
    if (node_pid == INVALID_PAGE_ID) {
      return;
    }
    Page *node_page = bpm_->FetchPage(node_pid);
    auto *node = AsBPlusTreePage<KeyType, ValueType, KeyComparator>(node_page);

    // root handling
    if (node_pid == root_page_id_) {
      bpm_->UnpinPage(node_pid, true);
      adjust_root(node_pid);
      return;
    }

    if (node->GetSize() >= node->GetMinSize()) {
      bpm_->UnpinPage(node_pid, true);
      return;
    }

    const page_id_t parent_pid = node->GetParentPageId();
    bpm_->UnpinPage(node_pid, true);

    Page *parent_page = bpm_->FetchPage(parent_pid);
    auto *parent = AsInternal<KeyType, ValueType, KeyComparator>(parent_page);
    const int idx = parent->ValueIndex(node_pid);
    const int left_idx = idx - 1;
    const int right_idx = idx + 1;
    const bool has_left = left_idx >= 0;
    const bool has_right = right_idx < parent->GetSize();

    // Choose a sibling (prefer left if exists).
    int sibling_idx = has_left ? left_idx : right_idx;
    const page_id_t sibling_pid = parent->ValueAt(sibling_idx);

    Page *curr_page = bpm_->FetchPage(node_pid);
    node = AsBPlusTreePage<KeyType, ValueType, KeyComparator>(curr_page);
    Page *sib_page = bpm_->FetchPage(sibling_pid);
    auto *sib_node = AsBPlusTreePage<KeyType, ValueType, KeyComparator>(sib_page);

    if (node->IsLeafPage()) {
      auto *curr = AsLeaf<KeyType, ValueType, KeyComparator>(curr_page);
      auto *sib = AsLeaf<KeyType, ValueType, KeyComparator>(sib_page);

      // Try redistribute
      if (sib->GetSize() > sib->GetMinSize()) {
        if (sibling_idx < idx) {
          // left sibling -> move last to front of curr
          sib->MoveLastToFrontOf(curr);
          parent->SetKeyAt(idx, curr->KeyAt(0));
        } else {
          // right sibling -> move first to end of curr
          sib->MoveFirstToEndOf(curr);
          parent->SetKeyAt(sibling_idx, sib->KeyAt(0));
        }
        bpm_->UnpinPage(sibling_pid, true);
        bpm_->UnpinPage(node_pid, true);
        bpm_->UnpinPage(parent_pid, true);
        return;
      }

      // Merge
      if (sibling_idx < idx) {
        // merge curr into left sibling
        curr->MoveAllTo(sib);
        parent->Remove(idx);
        bpm_->UnpinPage(node_pid, false);
        bpm_->DeletePage(node_pid);
        bpm_->UnpinPage(sibling_pid, true);
      } else {
        // merge right sibling into curr
        sib->MoveAllTo(curr);
        parent->Remove(sibling_idx);
        bpm_->UnpinPage(sibling_pid, false);
        bpm_->DeletePage(sibling_pid);
        bpm_->UnpinPage(node_pid, true);
      }
      bpm_->UnpinPage(parent_pid, true);
      coalesce_or_redistribute(parent_pid);
      return;
    }

    // Internal page
    auto *curr = AsInternal<KeyType, ValueType, KeyComparator>(curr_page);
    auto *sib = AsInternal<KeyType, ValueType, KeyComparator>(sib_page);

    // separator key in parent:
    const int sep_key_idx = (sibling_idx < idx) ? idx : sibling_idx;
    const KeyType sep_key = parent->KeyAt(sep_key_idx);

    // Try redistribute
    if (sib->GetSize() > sib->GetMinSize()) {
      if (sibling_idx < idx) {
        // borrow from left sibling: move its last pointer to front of curr
        const int sib_last = sib->GetSize() - 1;
        const page_id_t borrowed_child = sib->ValueAt(sib_last);
        const KeyType borrowed_key = sib->KeyAt(sib_last);

        // shift curr right by 1
        for (int i = curr->GetSize(); i > 0; i--) {
          curr->SetKeyAt(i, curr->KeyAt(i - 1));
          curr->SetValueAt(i, curr->ValueAt(i - 1));
        }
        // curr[1] should take old first pointer with sep_key
        const page_id_t old_first_ptr = curr->ValueAt(1);
        curr->SetKeyAt(1, sep_key);
        curr->SetValueAt(1, old_first_ptr);
        // new first pointer comes from borrowed child
        curr->SetValueAt(0, borrowed_child);
        curr->SetKeyAt(0, borrowed_key);  // ignored by Lookup (index starts at 1)
        curr->IncreaseSize(1);

        // remove last from sibling
        sib->SetSize(sib_last);
        // update parent separator key to borrowed_key
        parent->SetKeyAt(sep_key_idx, borrowed_key);

        // fix borrowed child's parent
        Page *child_page = bpm_->FetchPage(borrowed_child);
        AsBPlusTreePage<KeyType, ValueType, KeyComparator>(child_page)->SetParentPageId(node_pid);
        bpm_->UnpinPage(borrowed_child, true);
      } else {
        // borrow from right sibling: take its first pointer into end of curr
        const page_id_t borrowed_child = sib->ValueAt(0);
        const KeyType new_sep = sib->KeyAt(1);

        const int end = curr->GetSize();
        curr->SetKeyAt(end, sep_key);
        curr->SetValueAt(end, borrowed_child);
        curr->IncreaseSize(1);

        // shift sibling left by 1 (remove value[0], key[1] is promoted)
        for (int i = 0; i < sib->GetSize() - 1; i++) {
          sib->SetKeyAt(i, sib->KeyAt(i + 1));
          sib->SetValueAt(i, sib->ValueAt(i + 1));
        }
        sib->IncreaseSize(-1);
        parent->SetKeyAt(sep_key_idx, new_sep);

        // fix borrowed child's parent
        Page *child_page = bpm_->FetchPage(borrowed_child);
        AsBPlusTreePage<KeyType, ValueType, KeyComparator>(child_page)->SetParentPageId(node_pid);
        bpm_->UnpinPage(borrowed_child, true);
      }

      bpm_->UnpinPage(sibling_pid, true);
      bpm_->UnpinPage(node_pid, true);
      bpm_->UnpinPage(parent_pid, true);
      return;
    }

    // Merge
    if (sibling_idx < idx) {
      // merge curr into left sibling with sep_key
      curr->MoveAllTo(sib, sep_key);
      fix_internal_children_parent(sib, sibling_pid);
      parent->Remove(idx);
      bpm_->UnpinPage(node_pid, false);
      bpm_->DeletePage(node_pid);
      bpm_->UnpinPage(sibling_pid, true);
    } else {
      // merge right sibling into curr with sep_key
      sib->MoveAllTo(curr, sep_key);
      fix_internal_children_parent(curr, node_pid);
      parent->Remove(sibling_idx);
      bpm_->UnpinPage(sibling_pid, false);
      bpm_->DeletePage(sibling_pid);
      bpm_->UnpinPage(node_pid, true);
    }
    bpm_->UnpinPage(parent_pid, true);
    coalesce_or_redistribute(parent_pid);
  };

  const page_id_t leaf_pid = find_leaf(key);
  Page *leaf_page = bpm_->FetchPage(leaf_pid);
  auto *leaf = AsLeaf<KeyType, ValueType, KeyComparator>(leaf_page);
  const int old_size = leaf->GetSize();
  leaf->RemoveAndDeleteRecord(key, comparator_);
  if (leaf->GetSize() == old_size) {
    bpm_->UnpinPage(leaf_pid, false);
    return;
  }

  bpm_->UnpinPage(leaf_pid, true);
  coalesce_or_redistribute(leaf_pid);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result) -> bool {
  if (result != nullptr) {
    result->clear();
  }
  if (IsEmpty()) {
    return false;
  }

  page_id_t pid = root_page_id_;
  while (pid != INVALID_PAGE_ID) {
    Page *page = bpm_->FetchPage(pid);
    auto *node = AsBPlusTreePage<KeyType, ValueType, KeyComparator>(page);
    if (node->IsLeafPage()) {
      auto *leaf = AsLeaf<KeyType, ValueType, KeyComparator>(page);
      ValueType val;
      const bool found = leaf->Lookup(key, &val, comparator_);
      bpm_->UnpinPage(pid, false);
      if (!found) {
        return false;
      }
      if (result != nullptr) {
        result->push_back(val);
      }
      return true;
    }
    auto *internal = AsInternal<KeyType, ValueType, KeyComparator>(page);
    const page_id_t next = internal->Lookup(key, comparator_);
    bpm_->UnpinPage(pid, false);
    pid = next;
  }
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto BPLUSTREE_TYPE::Begin() -> Iterator {
  if (IsEmpty()) {
    return End();
  }
  page_id_t pid = root_page_id_;
  while (pid != INVALID_PAGE_ID) {
    Page *page = bpm_->FetchPage(pid);
    auto *node = AsBPlusTreePage<KeyType, ValueType, KeyComparator>(page);
    if (node->IsLeafPage()) {
      bpm_->UnpinPage(pid, false);
      return Iterator(pid, 0, bpm_);
    }
    auto *internal = AsInternal<KeyType, ValueType, KeyComparator>(page);
    page_id_t next = internal->ValueAt(0);
    bpm_->UnpinPage(pid, false);
    pid = next;
  }
  return End();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> Iterator {
  if (IsEmpty()) {
    return End();
  }
  page_id_t pid = root_page_id_;
  while (pid != INVALID_PAGE_ID) {
    Page *page = bpm_->FetchPage(pid);
    auto *node = AsBPlusTreePage<KeyType, ValueType, KeyComparator>(page);
    if (node->IsLeafPage()) {
      auto *leaf = AsLeaf<KeyType, ValueType, KeyComparator>(page);
      page_id_t cur_pid = pid;
      int idx = leaf->KeyIndex(key, comparator_);
      while (true) {
        if (idx < leaf->GetSize()) {
          bpm_->UnpinPage(cur_pid, false);
          return Iterator(cur_pid, idx, bpm_);
        }
        const page_id_t next_pid = leaf->GetNextPageId();
        bpm_->UnpinPage(cur_pid, false);
        if (next_pid == INVALID_PAGE_ID) {
          return End();
        }
        cur_pid = next_pid;
        page = bpm_->FetchPage(cur_pid);
        leaf = AsLeaf<KeyType, ValueType, KeyComparator>(page);
        idx = leaf->KeyIndex(key, comparator_);
      }
    }
    auto *internal = AsInternal<KeyType, ValueType, KeyComparator>(page);
    page_id_t next = internal->Lookup(key, comparator_);
    bpm_->UnpinPage(pid, false);
    pid = next;
  }
  return End();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto BPLUSTREE_TYPE::End() -> Iterator {
  return Iterator(INVALID_PAGE_ID, 0, bpm_);
}

// Explicit template instantiation (keep at end so definitions are visible).
template class BPlusTree<int, RID, std::less<int>>;

}  // namespace onebase
