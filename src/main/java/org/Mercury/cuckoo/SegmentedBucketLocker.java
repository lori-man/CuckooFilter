package org.Mercury.cuckoo;

import com.google.common.annotations.VisibleForTesting;

import java.util.concurrent.locks.StampedLock;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * 维护一个锁数组,该数组对应于bucket索引和它们所属的bitset段。(锁粒度:桶(包含的所有bit)的范围)
 *
 * Cuckoo filter的内存表被bucket索引分割成几个段，为了线程安全可以单独锁定这些段进行读写。
 * 该类持有锁，并包含用于解锁、解锁和避免死锁的操作方法
 */
public class SegmentedBucketLocker {
    private final StampedLock[] lockAry;
    //必须是2的幂,所以没有偏置
    private final int concurrentSegments;

    SegmentedBucketLocker(int expectedConcurrency) {
        checkArgument(expectedConcurrency > 0, "expectedConcurrency (%s) must be > 0.", expectedConcurrency);
        checkArgument((expectedConcurrency & (expectedConcurrency - 1)) == 0,
                "expectedConcurrency (%s) must be a power of two.", expectedConcurrency);


        //大多数操作都锁定2个bucket，所以对于X个线程，我们应该有大约2X个段。
        this.concurrentSegments = expectedConcurrency * 2;
        this.lockAry = new StampedLock[concurrentSegments];
        for (int i = 0; i < lockAry.length; i++) {
            lockAry[i] = new StampedLock();
        }
    }

    /**
     * 返回bucket所属的segment(段)
     * @param bucketIndex
     * @return
     */
    @VisibleForTesting
    private int getBucketLock(long bucketIndex){
        return (int) (bucketIndex % concurrentSegments);
    }

    /**
     * lock segment 时按特定顺序的捅索引,以防止死锁
     * @param i1
     * @param i2
     */
    void lockBucketWrite(long i1, long i2) {
        int bucket1LockIdx = getBucketLock(i1);
        int bucket2LockIdx = getBucketLock(i2);

        //要以相同的顺序锁定段，以避免死锁
        if (bucket1LockIdx < bucket2LockIdx) {
            lockAry[bucket1LockIdx].writeLock();
            lockAry[bucket2LockIdx].writeLock();
        } else if (bucket1LockIdx > bucket2LockIdx) {
            lockAry[bucket2LockIdx].writeLock();
            lockAry[bucket1LockIdx].writeLock();
        } else {
            // 两个在同一个字段,所以锁一次
            lockAry[bucket1LockIdx].writeLock();
        }
    }

    /**
     * lock segment 时按特定顺序的捅索引,以防止死锁
     */
    void lockBucketsRead(long i1, long i2) {
        int bucket1LockIdx = getBucketLock(i1);
        int bucket2LockIdx = getBucketLock(i2);
        if (bucket1LockIdx < bucket2LockIdx) {
            lockAry[bucket1LockIdx].readLock();
            lockAry[bucket2LockIdx].readLock();
        } else if (bucket1LockIdx > bucket2LockIdx) {
            lockAry[bucket2LockIdx].readLock();
            lockAry[bucket1LockIdx].readLock();
        }
        else {
            lockAry[bucket1LockIdx].readLock();
        }
    }

    /**
     * Unlocks segment 时按特定顺序的捅索引,以防止死锁
     */
    void unlockBucketsWrite(long i1, long i2) {
        int bucket1LockIdx = getBucketLock(i1);
        int bucket2LockIdx = getBucketLock(i2);

        if (bucket1LockIdx == bucket2LockIdx) {
            lockAry[bucket1LockIdx].tryUnlockWrite();
            return;
        }
        lockAry[bucket1LockIdx].tryUnlockWrite();
        lockAry[bucket2LockIdx].tryUnlockWrite();
    }

    /**
     * Unlocks segment 时按特定顺序的捅索引,以防止死锁
     */
    void unlockBucketsRead(long i1, long i2) {
        int bucket1LockIdx = getBucketLock(i1);
        int bucket2LockIdx = getBucketLock(i2);
        // always unlock segments in same order to avoid deadlocks
        if (bucket1LockIdx == bucket2LockIdx) {
            lockAry[bucket1LockIdx].tryUnlockRead();
            return;
        }
        lockAry[bucket1LockIdx].tryUnlockRead();
        lockAry[bucket2LockIdx].tryUnlockRead();
    }

    /**
     * lock all segment
     */
    void lockAllBucketsRead() {
        for (StampedLock lock : lockAry) {
            lock.readLock();
        }
    }

    /**
     * Unlocks all segments
     */
    void unlockAllBucketsRead() {
        for (StampedLock lock : lockAry) {
            lock.tryUnlockRead();
        }
    }

    void lockSingleBucketWrite(long i1) {
        int bucketLockIdx = getBucketLock(i1);
        lockAry[bucketLockIdx].writeLock();
    }

    void unlockSingleBucketWrite(long i1) {
        int bucketLockIdx = getBucketLock(i1);
        lockAry[bucketLockIdx].tryUnlockWrite();
    }
}
