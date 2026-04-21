#include <gtest/gtest.h>
#include "onebase/common/exception.h"
#include "onebase/concurrency/lock_manager.h"
#include "onebase/concurrency/transaction.h"

namespace onebase {

TEST(LockManagerTest, SharedLockBasic) {
  LockManager lock_mgr;
  Transaction txn(0);
  RID rid(0, 0);

  EXPECT_TRUE(lock_mgr.LockShared(&txn, rid));
  EXPECT_TRUE(txn.IsSharedLocked(rid));
  EXPECT_TRUE(lock_mgr.Unlock(&txn, rid));
}

TEST(LockManagerTest, ExclusiveLockBasic) {
  LockManager lock_mgr;
  Transaction txn(0);
  RID rid(0, 0);

  EXPECT_TRUE(lock_mgr.LockExclusive(&txn, rid));
  EXPECT_TRUE(txn.IsExclusiveLocked(rid));
  EXPECT_TRUE(lock_mgr.Unlock(&txn, rid));
}

}  // namespace onebase
