#include <gtest/gtest.h>
#include "onebase/buffer/lru_k_replacer.h"
#include "onebase/common/exception.h"

namespace onebase {

TEST(LRUKReplacerTest, BasicEviction) {
  LRUKReplacer replacer(7, 2);

  // Frame 1 has k accesses, frame 2 has < k accesses => frame 2 should be evicted first.
  replacer.RecordAccess(1);
  replacer.RecordAccess(1);
  replacer.RecordAccess(2);

  replacer.SetEvictable(1, true);
  replacer.SetEvictable(2, true);
  EXPECT_EQ(replacer.Size(), 2u);

  frame_id_t frame;
  EXPECT_TRUE(replacer.Evict(&frame));
  EXPECT_EQ(frame, 2);
  EXPECT_EQ(replacer.Size(), 1u);

  EXPECT_TRUE(replacer.Evict(&frame));
  EXPECT_EQ(frame, 1);
  EXPECT_EQ(replacer.Size(), 0u);
}

TEST(LRUKReplacerTest, RemoveNonEvictableThrows) {
  LRUKReplacer replacer(7, 2);
  replacer.RecordAccess(1);
  replacer.SetEvictable(1, false);
  EXPECT_THROW(replacer.Remove(1), OneBaseException);
}

}  // namespace onebase
