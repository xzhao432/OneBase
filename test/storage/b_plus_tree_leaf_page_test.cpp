#include <gtest/gtest.h>
#include <functional>

#include "onebase/common/rid.h"
#include "onebase/storage/page/b_plus_tree_leaf_page.h"

namespace onebase {

TEST(BPlusTreeLeafPageTest, InsertLookupRemove) {
  using Leaf = BPlusTreeLeafPage<int, RID, std::less<int>>;
  alignas(Leaf) char buf[4096];
  auto *page = reinterpret_cast<Leaf *>(buf);
  page->Init(10);

  EXPECT_EQ(page->Insert(2, RID(2, 2), std::less<int>{}), 1);
  EXPECT_EQ(page->Insert(1, RID(1, 1), std::less<int>{}), 2);
  EXPECT_EQ(page->Insert(3, RID(3, 3), std::less<int>{}), 3);

  RID rid;
  EXPECT_TRUE(page->Lookup(1, &rid, std::less<int>{}));
  EXPECT_EQ(rid, RID(1, 1));
  EXPECT_TRUE(page->Lookup(2, &rid, std::less<int>{}));
  EXPECT_EQ(rid, RID(2, 2));
  EXPECT_FALSE(page->Lookup(99, &rid, std::less<int>{}));

  page->RemoveAndDeleteRecord(2, std::less<int>{});
  EXPECT_FALSE(page->Lookup(2, &rid, std::less<int>{}));
}

}  // namespace onebase

