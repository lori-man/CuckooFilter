package org.Mercury.cuckoo;

import java.util.Arrays;

/**
 * 表将数据存储在字节数组中。
 * 在使用中,规定数据项的大小将是一个整数字节数，则可以使用ByteBuffer。
 */
public class ByteArrayTable {

    // 默认一个桶储存一个fingerprintf
    private int bitsPerBucket; // 每个桶所占的字节位数
    private int buckets; // 桶数量
    protected byte[] table = null;

    public int size() {
        return buckets;
    }


    public ByteArrayTable(int buckets, int bitsPerBucket) {
        if(buckets <= 0)
            throw new IllegalArgumentException("Cannot create a table with a non-positive number of buckets");
        if(bitsPerBucket <= 0)
            throw new IllegalArgumentException("Cannot create a table with a non-positive number of bits per bucket");

        this.bitsPerBucket = bitsPerBucket;
        this.buckets = buckets;
        int tableSize = (int) Math.ceil(bitsPerBucket * buckets / 8.0D);
        table = new byte[tableSize];
    }

    public boolean isItemInPos(byte[] item, int itemPos) {
        if(item.length != bytesPerBucket())
            throw new IllegalArgumentException("A data item must be an array of size " + bytesPerBucket() + " in bytes, to store the " + bitsPerBucket + " bits per bucket");
        if(itemPos >= buckets)
            throw new IllegalArgumentException("Cannot get item from position " + itemPos + ", valid range is [0," + (buckets-1) + "]");
        if(itemPos < 0)
            throw new IllegalArgumentException("Cannot get item from a negative position " + itemPos + ", valid range is [0," + (buckets-1) + "]");

        byte[] data = get(itemPos);

        for (int i = 0; i < data.length; i++) {
            if (data[i] != item[i]) {
                return false;
            }
        }
        return true;
    }

    public byte[] get(int itemPos) {
        if(itemPos >= buckets)
            throw new IllegalArgumentException("Cannot get item from position " + itemPos + ", valid range is [0," + (buckets-1) + "]");
        if(itemPos < 0)
            throw new IllegalArgumentException("Cannot get item from a negative position " + itemPos + ", valid range is [0," + (buckets-1) + "]");


        // 查找表中受影响的字节
        int firstByteInd = itemPos * bitsPerBucket / 8;
        int lastByteInd = ((itemPos + 1) * bitsPerBucket - 1) / 8;

        byte[] item = new byte[lastByteInd-firstByteInd+1];
        System.arraycopy(table, firstByteInd, item, 0, item.length);

        //向左移动以使项目数组与表中的字节对齐，新的位置也会被0填充。
        int firstBitInFirstByteInd = itemPos * bitsPerBucket % 8;
        item = ByteUtil.shitfRightAndFill(item, firstBitInFirstByteInd);

        // 如果需要，删除前导字节
        if (item.length == (bytesPerBucket() + 1)) {
            item = Arrays.copyOfRange(item, 0, item.length - 1);
        }

        if (item.length != bytesPerBucket()) {
            throw new InternalError("Created an item with a number of bytes " + item.length + " that differes from the size of buckets " + bytesPerBucket());
        }

        // 删除表中后面项的前导位
        if (bitsPerBucket % 8 != 0) {
            byte mask = (byte) ((0x01 << (bitsPerBucket % 8)) - 1);
            item[item.length - 1] &= mask;
        }

        return item;
    }

    public void insert(byte[] item, int itemPos) {
        if(item.length != bytesPerBucket())
            throw new IllegalArgumentException("A data item must be an array of size " + bytesPerBucket() + " in bytes, to store the " + bitsPerBucket + " bits per bucket");
        if(itemPos >= buckets)
            throw new IllegalArgumentException("Cannot insert item at position " + itemPos + ", valid range is [0," + (buckets-1) + "]");
        if(itemPos < 0)
            throw new IllegalArgumentException("Cannot insert item at a negative position " + itemPos + ", valid range is [0," + (buckets-1) + "]");


        // 定位受影响的字段
        int firstByteInd = itemPos * bitsPerBucket / 8;
        int lastByteInd  = ((itemPos + 1) * bitsPerBucket - 1) / 8;

        // 我们将创建一个item数组的副本，该副本将与表中相应的字节和操作相结合
        byte[] itemCp = new byte[lastByteInd - firstByteInd + 1];
        System.arraycopy(item, 0, itemCp, 0, item.length);

        // 检查:受影响字节的数量必须等于项目大小或大一个字节(不应该发生，但无论如何)
        if (item.length != itemCp.length && (item.length != (itemCp.length - 1))) {
            throw new InternalError("Affected bytes are in positions [" + firstByteInd + ","
                    + lastByteInd + "], " + (itemCp.length + 1)
                    + " bytes affected in total, but item is " + item.length + " bytes large");
        }

        //并不是数据项中的所有位都必须添加到表中，只有对应一个桶的位才能添加到表中。
        // 其余的部分(最重要的部分，即左边大小的部分)将全部被0所替换(这在后面会很方便)。
        int lastByteMaskSize = item.length * 8 - bitsPerBucket; // 需要三个0在mask中
        byte lastByteMask = (byte) ((0xff >> (lastByteMaskSize)));  // 构建 mask 00011111
        // 在来自原始数据项_的最后一个字节_中应用掩码
        itemCp[item.length - 1] = (byte) (itemCp[item.length - 1] & lastByteMask);
        if (itemCp.length > item.length) { // 如果必须创建一个额外的字节，也用0填充它
            itemCp[itemCp.length - 1] = (byte) 0x00;
        }

        // 现在向左移动以使项目数组与表中的字节对齐，新的位置也会被0填充。
        int firstBitInFirstByteInd = itemPos * bitsPerBucket % 8;
        itemCp = ByteUtil.shiftLeftAndFill(itemCp, firstBitInFirstByteInd);

        // 将表中所有将要被替换的位(即对应桶的位)设置为0
        delete(itemPos);

        // 最后，将表中受影响的字节与项目数组组合起来
        for (int i = 0; i < itemCp.length; i++) {
            itemCp[i] = (byte) (itemCp[i] | table[i + firstByteInd]);
        }

        System.arraycopy(itemCp, 0, table, firstByteInd, itemCp.length);
    }

    public void delete(int itemPos) {

        if(itemPos >= buckets)
            throw new IllegalArgumentException("Cannot delete item in position " + itemPos + ", valid range is [0," + (buckets-1) + "]");
        if(itemPos < 0)
            throw new IllegalArgumentException("Cannot delete item in a negative position " + itemPos + ", valid range is [0," + (buckets-1) + "]");

        for (int i = itemPos * bitsPerBucket; i < (itemPos + 1) * bitsPerBucket; i++) {
            ByteUtil.insertZeroIn(table, i);
        }
    }

    private int bytesPerBucket() {
        return (int) Math.ceil(bitsPerBucket / 8.0D);
    }
}