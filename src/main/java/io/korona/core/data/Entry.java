package io.korona.core.data;

/**
 * @program: korona-db
 * @description:
 * @author: H4rry217
 **/

public interface Entry {

    /** CRC(8) TIMESTAMP(8) KEYSZ(4) VALUESZ(4) **/
    public static final int HEADER_SIZE = 8 + 8 + 4 + 4;

    public long crc();

    public long timestamp();

    public int keySize();

    public int valueSize();

    public byte[] key();

    public byte[] value();

    public int getSize();

    public boolean isTombStone();

}
