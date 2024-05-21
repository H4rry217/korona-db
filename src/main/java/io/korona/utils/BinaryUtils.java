package io.korona.utils;

/**
 * @program: korona-db
 * @description:
 * @author: H4rry217
 **/

public class BinaryUtils {

    public static boolean isEqual(byte[] v1, byte[] v2){
        if(v1.length != v2.length) return false;

        for (int i = 0; i < v1.length; i++) {
            if(v1[i] != v2[i]) return false;
        }

        return true;
    }

    public static long bytesToLong(byte[] bytes){
        return ((long)(bytes[0]) << 56) +
                ((long)(bytes[1] & 0xff) << 48) +
                ((long)(bytes[2] & 0xff) << 40) +
                ((long)(bytes[3] & 0xff) << 32) +
                ((long)(bytes[4] & 0xff) << 24) +
                ((bytes[5] & 0xff) << 16) +
                ((bytes[6] & 0xff) << 8) +
                ((bytes[7] & 0xff));
    }

    public static byte[] longToBytes(long v){
        return new byte[]{
                (byte) (v >>> 56),
                (byte) (v >>> 48),
                (byte) (v >>> 40),
                (byte) (v >>> 32),
                (byte) (v >>> 24),
                (byte) (v >>> 16),
                (byte) (v >>> 8),
                (byte) v,
        };
    }

    public static byte[] intToBytes(int v){
        return new byte[]{
                (byte) (v >>> 24),
                (byte) (v >>> 16),
                (byte) (v >>> 8),
                (byte) v,
        };
    }

    public static int bytesToInt(byte[] bytes){
        return ((bytes[0] & 0xff) << 24) +
                ((bytes[1] & 0xff) << 16) +
                ((bytes[2] & 0xff) << 8) +
                ((bytes[3] & 0xff));
    }

}
