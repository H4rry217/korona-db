package io.korona.server;

import io.korona.core.data.DataEntry;
import io.korona.core.data.serialize.TypeConvert;
import io.korona.core.exception.CRCInValidException;

import java.nio.ByteBuffer;

/**
 * @program: korona-db
 * @description:
 * @author: H4rry217
 **/

public class SimpleData extends DataEntry {

    public SimpleData(ByteBuffer byteBuffer) {
        super(byteBuffer);

        var crc = entryCrc(this.timestamp, keyData, valueData);

        if(this.crc() != 0 && crc != this.crc()){
            throw new CRCInValidException();
        }

    }

    public SimpleData(Object key, Object value){
        super(System.currentTimeMillis(), TypeConvert.deserialize(key), TypeConvert.deserialize(value));
    }

    public SimpleData(long timestamp, byte[] key, byte[] value){
        super(timestamp, key, value);
    }

}
