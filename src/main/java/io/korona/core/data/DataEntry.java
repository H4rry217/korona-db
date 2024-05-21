package io.korona.core.data;

import io.korona.utils.BinaryUtils;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;

/**
 * @program: korona-db
 * @description:
 * @author: H4rry217
 **/

public abstract class DataEntry implements Entry {

    private long crc;

    protected long timestamp;
    protected byte[] keyData;
    protected byte[] valueData;

    public DataEntry(ByteBuffer byteBuffer) {
        this.crc = byteBuffer.getLong();
        this.timestamp = byteBuffer.getLong();

        int keySize = byteBuffer.getInt();
        int valSize = byteBuffer.getInt();

        this.keyData = new byte[keySize];
        this.valueData = new byte[valSize];

        byteBuffer.get(this.keyData);
        byteBuffer.get(this.valueData);
    }

    public DataEntry(long timestamp, byte[] keyData, byte[] valueData){
        this.timestamp = timestamp;
        this.keyData = keyData;
        this.valueData = valueData;
        this.crc = entryCrc(this.timestamp, this.keyData, this.valueData);
    }

    public DataEntry(){

    }

    @Override
    public long crc() {
        return this.crc;
    }

    @Override
    public long timestamp() {
        return this.timestamp;
    }

    @Override
    public int keySize() {
        return this.key().length;
    }

    @Override
    public int valueSize() {
        return this.value().length;
    }

    @Override
    public byte[] key() {
        return this.keyData;
    }

    @Override
    public byte[] value() {
        return this.valueData;
    }

    public static long entryCrc(long timestamp, byte[] key, byte[] val){
        var crc32 = new CRC32();
        crc32.update(BinaryUtils.longToBytes(timestamp));
        crc32.update(BinaryUtils.intToBytes(key.length));
        crc32.update(BinaryUtils.intToBytes(val.length));
        crc32.update(key);
        crc32.update(val);

        return crc32.getValue();
    }

    @Override
    public int getSize() {
        return HEADER_SIZE + keyData.length + valueData.length;
    }

    @Override
    public boolean isTombStone() {
        return this.valueSize() < 1;
    }
}
