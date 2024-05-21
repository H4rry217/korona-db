package io.korona.core.typeserialize.number;

import io.korona.core.data.serialize.Type;
import io.korona.core.data.serialize.TypeSerializer;
import io.korona.utils.BinaryUtils;

import java.util.Arrays;

/**
 * @program: korona-db
 * @description:
 * @author: H4rry217
 **/

public class LongTypeSerializer implements TypeSerializer<Long> {

    private final Type<Long> type;
    private final byte[] typeArray;

    public LongTypeSerializer(){
        this.type = new LongTypeSerializer.LongType();
        this.typeArray = new byte[]{this.getType().getType()};
    }

    @Override
    public byte[] deserialize(Long data) {
        byte[] sData = BinaryUtils.longToBytes(data);
        byte[] result = new byte[sData.length + 1];

        System.arraycopy(this.typeArray, 0, result, 0, this.typeArray.length);
        System.arraycopy(sData, 0, result, this.typeArray.length, sData.length);

        return result;
    }

    @Override
    public Long serialize(byte[] data) {
        return BinaryUtils.bytesToLong(Arrays.copyOfRange(data, 1, data.length));
    }

    @Override
    public Type<Long> getType() {
        return this.type;
    }

    public static final class LongType implements Type<Long> {

        @Override
        public byte getType() {
            return 0x02;
        }

        @Override
        public Class<Long> getClazz() {
            return Long.class;
        }
    }

}
