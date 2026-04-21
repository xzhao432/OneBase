#include <gtest/gtest.h>
#include <functional>

#include "onebase/storage/page/b_plus_tree_internal_page.h"

namespace onebase {

TEST(BPlusTreeInternalPageTest, BasicLookup) {
  using Internal = BPlusTreeInternalPage<int, page_id_t, std::less<int>>;
  // allocate a raw buffer big enough for a page object
  alignas(Internal) char buf[4096];
  auto *page = reinterpret_cast<Internal *>(buf);
  page->Init(10);
  page->SetSize(4);
  // keys: [_, 5, 10, 20], values: [p0,p1,p2,p3]
  page->SetValueAt(0, 100);
  page->SetKeyAt(1, 5);
  page->SetValueAt(1, 101);
  page->SetKeyAt(2, 10);
  page->SetValueAt(2, 102);
  page->SetKeyAt(3, 20);
  page->SetValueAt(3, 103);

  EXPECT_EQ(page->Lookup(1, std::less<int>{}), 100);
  EXPECT_EQ(page->Lookup(5, std::less<int>{}), 101);
  EXPECT_EQ(page->Lookup(9, std::less<int>{}), 101);
  EXPECT_EQ(page->Lookup(10, std::less<int>{}), 102);
  EXPECT_EQ(page->Lookup(30, std::less<int>{}), 103);
}

}  // namespace onebase

