package org.Mercury.cuckoo;

public class ByteUtil {

    public final static boolean isZero(byte[] array) {
        if(array == null)
            throw new IllegalArgumentException("Cannot check if a null array is full of zeros");
        for(byte b: array)
            if(b != 0)
                return false;
        return true;
    }

    /**
     * 将字节数组向左移动一定数量的位置(负有效位在右端)，并在数组移动时填充0。最多可移动7位(位置)。
     */
    public final static byte[] shiftLeftAndFill(byte[] array, int positions) {
        if(array == null)
            throw new IllegalArgumentException("Cannot shift a null byte array");
        if(positions < 0)
            throw new IllegalArgumentException("Cannot shift a negative number of positions");
        if(positions >= 8)
            throw new IllegalArgumentException("Weird error, should not be asking for shifting more than 7 positions, but " + positions + " are asked for");

        byte[] result = new byte[array.length];
        byte mask = (byte) (((byte) 0xff) << (8 - positions));
        for(int i = array.length-1; i >=0; i--) { // 从左到右遍历数组
            result[i] = (byte) (array[i] << positions);
            if (i == 0) {
                break;
            }

            // 从右侧的下一个字节'检索'位，因此它们不会丢失
            byte fromFoll = (byte) (array[i - 1] & mask);
            // 需要&0xff
            fromFoll = (byte) ((fromFoll & 0xff) >>> (8 - positions));
            result[i] = (byte) (result[i] | fromFoll);
        }
        return result;
    }

    /**
     * 将字节数组向右移动一定数量的位置(负有效位在右端)，并在数组移动时填充0。
     * 最多可移动7位(位置)。
     */
    public final static byte[] shitfRightAndFill(byte[] array, int positions) {
        if(array == null)
            throw new IllegalArgumentException("Cannot shift a null byte array");
        if(positions < 0)
            throw new IllegalArgumentException("Cannot shift a negative number of positions");
        if(positions >= 8)
            throw new IllegalArgumentException("Weird error, should not be asking for shifting more than 7 positions, but " + positions + " are asked for");

        byte[] result = new byte[array.length];
        byte mask = (byte) ((0x01 << positions) - 1);

        for (int i = 0; i < array.length; i++) { // 从右到左遍历数组
            result[i] = (byte) ((array[i] & 0xff) >>> positions);
            if (i < array.length - 1) { //从左边的下一个字节获取位
                result[i] = (byte) (result[i] | (byte) (((byte) (array[i + 1] & mask)) << (8 - positions)));
            }
        }
        return result;
    }

    public final static void insertZeroIn(byte[] array, int bitPos) {
        if(array == null)
            throw new IllegalArgumentException("Cannot insert zeros in a null array");
        if(bitPos < 0)
            throw new IllegalArgumentException("Cannot insert zero in a negative position (byte array index)");
        if(bitPos >= array.length * 8)
            throw new IllegalArgumentException("Cannot insert zero in position (index) " + bitPos + ", byte array length is " + array.length*8 + " in bits");

        int bytePos = bitPos / 8;
        int posInByte = bitPos % 8;
        byte mask = (byte) ~(byte) (0x01 << posInByte);
        array[bytePos] = (byte) (array[bytePos] & mask);
    }

    public final static String readableByteArray(byte[] array) {
        if(array == null)
            return "[NULL]";
        String result = "[";
        for(int i = array.length-1; i >=0; i--) {
            result += readableByte(array[i]);
            if(i > 0)
                result += ("|");
        }
        result += "]";
        return result;
    }

    public final static String readableByte(byte b) {
        String a = Integer.toBinaryString(256 + (int) b);
        return (a.substring(a.length() - 8));
    }
}
