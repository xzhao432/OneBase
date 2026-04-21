#include <gtest/gtest.h>
#include <functional>
#include <string>
#include <vector>

#include "onebase/buffer/buffer_pool_manager.h"
#include "onebase/common/rid.h"
#include "onebase/storage/disk/disk_manager.h"
#include "onebase/storage/index/b_plus_tree.h"

namespace onebase {

TEST(BPlusTreeTest, InsertGetValueDelete) {
  const std::string db = "__bpt_test.db";
  DiskManager dm(db);
  BufferPoolManager bpm(100, &dm);
  BPlusTree<int, RID, std::less<int>> tree("t", &bpm, std::less<int>{}, 4, 4);

  for (int i = 1; i <= 20; i++) {
    EXPECT_TRUE(tree.Insert(i, RID(i, i)));
  }

  for (int i = 1; i <= 20; i++) {
    std::vector<RID> result;
    EXPECT_TRUE(tree.GetValue(i, &result));
    ASSERT_EQ(result.size(), 1u);
    EXPECT_EQ(result[0], RID(i, i));
  }

  for (int i = 1; i <= 10; i++) {
    tree.Remove(i);
  }
  for (int i = 1; i <= 10; i++) {
    std::vector<RID> result;
    EXPECT_FALSE(tree.GetValue(i, &result));
  }
  for (int i = 11; i <= 20; i++) {
    std::vector<RID> result;
    EXPECT_TRUE(tree.GetValue(i, &result));
  }

  dm.ShutDown();
  std::remove(db.c_str());
}

}  // namespace onebase

