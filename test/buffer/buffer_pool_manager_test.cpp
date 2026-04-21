#include <gtest/gtest.h>
#include <cstring>
#include "onebase/buffer/buffer_pool_manager.h"
#include "onebase/common/exception.h"
#include "onebase/storage/disk/disk_manager.h"

namespace onebase {

TEST(BufferPoolManagerTest, NewPageBasic) {
  const std::string db_name = "test_bpm.db";
  DiskManager disk_manager(db_name);
  BufferPoolManager bpm(10, &disk_manager);

  page_id_t page_id;
  auto *page = bpm.NewPage(&page_id);
  ASSERT_NE(page, nullptr);
  EXPECT_EQ(page->GetPageId(), page_id);
  EXPECT_EQ(page->GetPinCount(), 1);

  std::snprintf(page->GetData(), 64, "hello_bpm");
  EXPECT_TRUE(bpm.UnpinPage(page_id, true));

  disk_manager.ShutDown();
  std::remove(db_name.c_str());
}

TEST(BufferPoolManagerTest, FetchPageBasic) {
  const std::string db_name = "test_bpm_fetch.db";
  DiskManager disk_manager(db_name);
  BufferPoolManager bpm(10, &disk_manager);

  page_id_t pid;
  auto *page = bpm.NewPage(&pid);
  ASSERT_NE(page, nullptr);
  std::snprintf(page->GetData(), 64, "persist");
  EXPECT_TRUE(bpm.UnpinPage(pid, true));

  auto *fetched = bpm.FetchPage(pid);
  ASSERT_NE(fetched, nullptr);
  EXPECT_STREQ(fetched->GetData(), "persist");
  EXPECT_TRUE(bpm.UnpinPage(pid, false));

  disk_manager.ShutDown();
  std::remove(db_name.c_str());
}

}  // namespace onebase
