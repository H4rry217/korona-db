package io.korona.utils;

import java.util.zip.CRC32;

/**
 * @program: korona-db
 * @description:
 * @author: H4rry217
 **/

public class EntryUtils {

    private static final CRC32 CRC32 = new CRC32();

    public static long getChecksum(byte[] data){
        CRC32.update(data);
        return CRC32.getValue();
    }

}
