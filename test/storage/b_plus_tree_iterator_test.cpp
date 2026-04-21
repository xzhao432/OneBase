#include <gtest/gtest.h>
#include <algorithm>
#include <functional>
#include <string>
#include <vector>

#include "onebase/buffer/buffer_pool_manager.h"
#include "onebase/common/rid.h"
#include "onebase/storage/disk/disk_manager.h"
#include "onebase/storage/index/b_plus_tree.h"
#include "onebase/storage/index/b_plus_tree_iterator.h"

namespace onebase {

TEST(BPlusTreeIteratorTest, IteratesInOrder) {
  const std::string db = "__bpt_iter_test.db";
  DiskManager dm(db);
  BufferPoolManager bpm(50, &dm);
  BPlusTree<int, RID, std::less<int>> tree("t", &bpm, std::less<int>{}, 4, 4);

  std::vector<int> keys = {5, 3, 8, 1, 9, 2, 7, 4, 6, 10};
  for (int k : keys) {
    EXPECT_TRUE(tree.Insert(k, RID(k, k)));
  }
  std::sort(keys.begin(), keys.end());

  int idx = 0;
  for (auto it = tree.Begin(); it != tree.End(); ++it) {
    auto [k, rid] = *it;
    ASSERT_LT(idx, static_cast<int>(keys.size()));
    EXPECT_EQ(k, keys[idx]);
    EXPECT_EQ(rid, RID(keys[idx], keys[idx]));
    idx++;
  }
  EXPECT_EQ(idx, static_cast<int>(keys.size()));

  dm.ShutDown();
  std::remove(db.c_str());
}

}  // namespace onebase

