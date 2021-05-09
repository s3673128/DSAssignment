package au.edu.rmit.common;

import java.nio.charset.Charset;

/**
 * utility for byte and object conversion
 */
public class ByteUtil {

    /**
     * convert an byte number to byte array
     * @param data
     * @return
     */
    public static byte[] getBytes(byte data) {
        byte[] bytes = new byte[1];
        bytes[0] = (byte) (data & 0xff);
        return bytes;
    }

    /**
     * convert an int number to byte array
     * @param data
     * @return
     */
    public static byte[] getBytes(int data) {
        byte[] bytes = new byte[4];
        bytes[0] = (byte) (data & 0xff);
        bytes[1] = (byte) ((data & 0xff00) >> 8);
        bytes[2] = (byte) ((data & 0xff0000) >> 16);
        bytes[3] = (byte) ((data & 0xff000000) >> 24);
        return bytes;
    }

    /**
     * convert a long number to byte array
     * @param data
     * @return
     */
    public static byte[] getBytes(long data)
    {
        byte[] bytes = new byte[8];
        bytes[0] = (byte) (data & 0xff);
        bytes[1] = (byte) ((data >> 8) & 0xff);
        bytes[2] = (byte) ((data >> 16) & 0xff);
        bytes[3] = (byte) ((data >> 24) & 0xff);
        bytes[4] = (byte) ((data >> 32) & 0xff);
        bytes[5] = (byte) ((data >> 40) & 0xff);
        bytes[6] = (byte) ((data >> 48) & 0xff);
        bytes[7] = (byte) ((data >> 56) & 0xff);
        return bytes;
    }

    /**
     * convert a string to byte array
     * @param data
     * @param charsetName
     * @return
     */
    public static byte[] getBytes(String data, String charsetName)
    {
        Charset charset = Charset.forName(charsetName);
        return data.getBytes(charset);
    }

    /**
     * convert a string to byte array
     * @param data
     * @return
     */
    public static byte[] getBytes(String data)
    {
        return getBytes(data, "UTF-8");
    }

    /**
     * get the sub array [startIndex, startIndex + length) from source array
     * @param array source array
     * @param startIndex start index
     * @param length sub array length
     * @return
     */
    public static byte[] subArray(byte[] array, int startIndex, int length) {
        byte[] subArray = new byte[length];
        for (int i = 0; i < length; i++) {
            subArray[i] = array[i + startIndex];
        }
        return subArray;
    }

    /**
     * get byte value from byte array
     * @param array byte array
     * @param startIndex the start index in the byte array of byte field
     * @return
     */
    public static byte getByte(byte[] array, int startIndex) {
        return array[startIndex];
    }

    /**
     * get int value from byte array
     * @param array byte array
     * @param startIndex the start index in the byte array of int field
     * @return
     */
    public static int getInt(byte[] array, int startIndex) {
        byte[] bytes = new byte[4];
        for (int i = 0; i < 4; i++) {
            bytes[i] = array[startIndex + i];
        }
        return (0xff & bytes[0]) | (0xff00 & (bytes[1] << 8)) | (0xff0000 & (bytes[2] << 16)) | (0xff000000 & (bytes[3] << 24));
    }

    /**
     * get long value from byte array
     * @param array byte array
     * @param startIndex the start index in the byte array of long field
     * @return
     */
    public static long getLong(byte[] array, int startIndex) {
        byte[] bytes = new byte[8];
        for (int i = 0; i < 8; i++) {
            bytes[i] = array[startIndex + i];
        }
        return(0xffL & (long)bytes[0]) | (0xff00L & ((long)bytes[1] << 8)) | (0xff0000L & ((long)bytes[2] << 16)) | (0xff000000L & ((long)bytes[3] << 24))
                | (0xff00000000L & ((long)bytes[4] << 32)) | (0xff0000000000L & ((long)bytes[5] << 40)) | (0xff000000000000L & ((long)bytes[6] << 48)) | (0xff00000000000000L & ((long)bytes[7] << 56));
    }

    /**
     * get string value from byte array
     * @param array byte array
     * @param startIndex the start index in the byte array of string field
     * @return
     */
    public static String getString(byte[] array, int startIndex, int length, String charsetName)
    {
        byte[] bytes = new byte[length];
        for (int i = 0; i < length; i++) {
            bytes[i] = array[startIndex + i];
        }
        return new String(bytes, Charset.forName(charsetName));
    }

    /**
     * get string value from byte array
     * @param array byte array
     * @param startIndex the start index in the byte array of string field
     * @return
     */
    public static String getString(byte[] array, int startIndex, int length)
    {
        return getString(array, startIndex, length, "UTF-8");
    }
}
