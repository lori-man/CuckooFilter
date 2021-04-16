package org.Mercury.cuckoo;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.locks.StampedLock;

public class CuckooFilter {
    private static Logger logger = LoggerFactory.getLogger(CuckooFilter.class);

    private static final int MAX_TRIES_WHEN_ADDING = 500;

    private MessageDigest sha1 = null;
    @VisibleForTesting
    private int fingerprintSize = 0;
    private byte fingerprintLastByteMask = (byte) 0xff; // 防止假阴性,对fingerprint的偏移
    @VisibleForTesting
    private ByteArrayTable table = null;
    @VisibleForTesting
    private ItemInfo lastVictim = null;

    // 锁
    private SegmentedBucketLocker segmentedBucketLocker;
    // victim 的锁
    private StampedLock lockVictim;

    /**
     *
     * @param fingerprintSize
     *                  每个指纹的比特数(代表一项的值)
     * @param maxItems
     *                  我们在过滤器中期望的最大条目数量。
     *                  实际上，底层数组的大小会比这个大。(数组的大小一定是2的幂次方)
     */
    public CuckooFilter(int fingerprintSize, int maxItems) {
        if(fingerprintSize <= 0)
            throw new IllegalArgumentException("Fingerprint size must be a positive number, received " + fingerprintSize);
        if(fingerprintSize > 16 * 8)
            throw new IllegalArgumentException("Fingerprint size cannot be greater than " + 16 * 8  +" , received " + fingerprintSize);

        this.fingerprintSize = fingerprintSize;
        if (fingerprintSize % 8 != 0) { //必须在 mask 的最高有效字节中添加一些前导0
            int zeros = 8 - (fingerprintSize % 8);
            fingerprintLastByteMask = (byte) ((0x01 << zeros) - 1);
        }

        try {
            sha1 = MessageDigest.getInstance("SHA1");
        } catch (NoSuchAlgorithmException var) {
            throw new InternalError("All Java implementations should carry an implementation of SHA1, however it cannot be found!");
        }

        // 表大小必须是2的幂，并且大于最大项数
        int tableSize = 1;
        while (tableSize < maxItems) {
            tableSize <<= 1;
        }

        // 如果没有足够的“剩余空间”，则增加表的大小
        if (maxItems * 1.0D / tableSize > 0.96) {
            tableSize <<= 1;
        }

        table = new ByteArrayTable(tableSize, fingerprintSize);

        segmentedBucketLocker = new SegmentedBucketLocker(16);
    }

    /**
     * 如果在过滤器中找到给定对象的签名，则返回{@code true}。但请记住，错误肯定是可能的
     */
    public boolean contains(Object o) {
        logger.info("test contain:" + o);
        ItemInfo info = itemInfoObj(o);

        if (lastVictim != null) {
            if (Arrays.equals(info.fingerprint, lastVictim.fingerprint)) {
                return true;
            }
        }

        segmentedBucketLocker.lockBucketsRead(info.index, info.index2);
        try {
            if (Arrays.equals(info.fingerprint, table.get(info.index))) {
                return true;
            }

            if (Arrays.equals(info.fingerprint, table.get(info.index2))) {
                return true;
            }
        }finally {
            segmentedBucketLocker.unlockBucketsRead(info.index, info.index2);
        }

        return false;
    }

    public boolean isFull() {
        return lastVictim != null;
    }

    /**
     * 如果给定的对象{@code o}已经被包含，它将返回{@code true}
     * 如果过滤器太满，这个方法返回{@code false}。
     */
    public boolean add(Object o) {
        if (o == null) {
            throw new IllegalArgumentException("Cannot add a null object");
        }
        return addItem(itemInfoObj(o));
    }

    private boolean addItem(ItemInfo info) {

        // 先判断是否存在
        if (Arrays.equals(info.fingerprint, table.get(info.index))) {
            return true;
        }
        if (Arrays.equals(info.fingerprint, table.get(info.index2))) {
            return true;
        }

        //判断是否已满
        if (lastVictim != null) {
            return false;
        }

        if (ByteUtil.isZero(table.get(info.index))) {
            table.insert(info.fingerprint, info.index);
            return true;
        }

        int destination = info.index2;
        byte[] fingerprint = info.fingerprint;
        int tries = 0;
        while (++tries <= MAX_TRIES_WHEN_ADDING) {
            byte[] oldFingerpring = table.get(destination);
            table.insert(fingerprint, destination);
            if (ByteUtil.isZero(oldFingerpring)) {
                return true;
            }
            fingerprint = oldFingerpring;
            destination = altIndex(fingerprint, destination);
        }

        lastVictim = new ItemInfo();
        lastVictim.fingerprint = fingerprint;
        lastVictim.index = destination;
        lastVictim.index2 = altIndex(fingerprint, destination);

        return true;
    }

    /**
     * 如果找到元素签名，它将返回{@code true}，否则返回{@code false}。
     * 在任何情况下，如果发现签名将被删除。
     */
    public boolean delete(Object o) {
        if (o == null) {
            throw new IllegalArgumentException("Cannot remove a null object");
        }

        ItemInfo itemInfo = itemInfoObj(o);

        if (ByteUtil.isZero(table.get(itemInfo.index)) && ByteUtil.isZero(table.get(itemInfo.index2))) {
            return false;
        }

        boolean deleted = false;
        if (Arrays.equals(itemInfo.fingerprint, table.get(itemInfo.index))) {
            table.delete(itemInfo.index);
            return true;
        } else if (Arrays.equals(itemInfo.fingerprint, table.get(itemInfo.index2))) {
            table.delete(itemInfo.index2);
            return true;
        }

        if (deleted) {  // 这里还有空间容纳受害者(如果有的话)，让我们试着插入它
            if (lastVictim != null) {
                ItemInfo infoVic = new ItemInfo();
                infoVic.fingerprint = Arrays.copyOf(lastVictim.fingerprint, lastVictim.fingerprint.length);
                infoVic.index = lastVictim.index;
                infoVic.index2 = lastVictim.index2;
                lastVictim = null;
                addItem(infoVic);
            }
        }
        return deleted;
    }


    @VisibleForTesting
    private class ItemInfo {
        @VisibleForTesting
        int index = -1;
        @VisibleForTesting
        int index2 = -1;
        @VisibleForTesting
        byte[] fingerprint = null;
        @Override
        public String toString() {
            return "i1: " + index + ", i2: " + index2 + ", fingerprint: " + ByteUtil.readableByteArray(fingerprint);
        }
    }

    /**
     * 使用hashcode来作为唯一性
     */
    protected ItemInfo itemInfoObj(Object o) {
        int h = o.hashCode();
        byte[] b = new byte[4];
        for (int i = 0; i < b.length; i++) {
            b[i] = (byte) (h & 0xff);  // & 0xff 将int(4个字节)分割成4个byte
            h >>= 8;
        }
        return itemInfo(b);
    }

    public ItemInfo itemInfo(byte[] item) {
        ItemInfo itemInfo = new ItemInfo();
        byte[] hash = sha1.digest(item);   // SHA1 作为hash函数

        // First index
        long val = 0;
        for (int i = 0; i < 4; i++) { // 将4个byte转换成int
            val |= (hash[i] & 0xff);
            if (i < 3) {
                val <<= 8;
            }
        }

        val &= 0x00000000ffffffffL;
        // 由于table的size是2的幂次,val % table.size求解很快
        itemInfo.index = (int) (val % (long) table.size());

        // Fingerprint
        itemInfo.fingerprint = new byte[fingerprintSizeInBytes()];
        for (int i = 0; i < itemInfo.fingerprint.length; i++) {
            itemInfo.fingerprint[i] = hash[i + 4];
        }
        itemInfo.fingerprint[itemInfo.fingerprint.length - 1] &= fingerprintLastByteMask;
        if(ByteUtil.isZero(itemInfo.fingerprint)) // 避免所有的指纹是0(它们会与表中的“无指纹”相混淆)
            itemInfo.fingerprint[0] = 1;

        // second index
        itemInfo.index2 = altIndex(itemInfo.fingerprint, itemInfo.index);

        if (altIndex(itemInfo.fingerprint, itemInfo.index2) != itemInfo.index) {
            logger.info("second index fingerprint:" + itemInfo.fingerprint);
            throw new InternalError("Generated wrong indexes!");
        }

        return itemInfo;
    }

    /**
     * i2=i1&hash(fingerprint) 获取i2
     */
    private int altIndex(byte[] fingerprint, int index) {
        byte[] hash = sha1.digest(fingerprint);
        long val = 0;
        for (int i = 0; i < 4; i++) {
            long mask = 0xffL;
            mask <<= (i * 8);
            byte b = (byte) ((mask & (long) index) >> (i * 8));
            val |= (((hash[i] ^ b) & 0xff) << (i * 8));
        }
        val &= 0x00000000ffffffffL;
        return (int) (val % (long) table.size());
    }

    private int fingerprintSizeInBytes() {
        return (int)Math.ceil(fingerprintSize/8.0D);
    }


    public static void main(String[] args) {
        for (int i = 0; i < 10000; i++)
            if (!testFilter())
                break;
    }

    private static boolean testFilter() {
        CuckooFilter filter = new CuckooFilter(16, 1000);
        System.out.println("\n===============================");
        System.out.println("Table size: " + filter.table.size());

        System.out.println("RANDOM INSERTIONS");
        Random random = new Random();
        Set<Integer> bag = new HashSet<Integer>();
        for(int i = 0; i < 1000; i++) {
            Integer o = new Integer(i);
            boolean insert = random.nextBoolean();
            if(insert) {
                if(filter.add(o))
                    bag.add(o);
                else {
                    System.out.println("ERROR COULD NOT ADD " + o);
                    return false;
                }
            }
        }
        if(filter.isFull())
            System.out.println("FILTER IS FULL");
        else
            System.out.println("FILTER IS NOT FULL");

        System.out.println("CHECKING CONTENTS AFTER RANDOM INSERTIONS (BAG SIZE IS " + bag.size() + ")");
        for(Integer i: bag)
            if(!filter.contains(i)) {
                System.out.println("ERROR!");
                return false;
            }

        byte[] tableCpBfDel = Arrays.copyOf(filter.table.table, filter.table.table.length);

        System.out.println("RANDOM DELETIONS");
        Iterator<Integer> iter = bag.iterator();
        while(iter.hasNext()) {
            Integer i = iter.next();
            boolean remove = random.nextBoolean();
            if(remove) {
                filter.delete(i);
                iter.remove();
            }
        }

        System.out.println("CHECKING CONTENTS AFTER RANDOM DELETIONS (BAG SIZE IS " + bag.size() + ")");
        for(Integer i: bag)
            if(!filter.contains(i)) {
                System.out.println("ERROR, FILTER DOES NOT CONTAIN " + i);
                ItemInfo info = filter.itemInfoObj(i);
                System.out.println(info);
                System.out.println("filter[" + info.index + "]:" + ByteUtil.readableByteArray(filter.table.get(info.index)) + "; filter[" + info.index2 + "]:" + ByteUtil.readableByteArray(filter.table.get(info.index2)));
                System.out.println("filterBfDe[" + info.index + "]:" + ByteUtil.readableByteArray(new byte[]{tableCpBfDel[info.index]}) + "; filterBfDe[" + info.index2 + "]:" + ByteUtil.readableByteArray(new byte[]{tableCpBfDel[info.index2]}));
                return false;
            }

        System.out.println("EVERYTHING FINE!");
        return true;
    }
}
