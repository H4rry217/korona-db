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

public final class IntTypeSerializer implements TypeSerializer<Integer> {

    private final Type<Integer> type;
    private final byte[] typeArray;

    public IntTypeSerializer(){
        this.type = new IntType();
        this.typeArray = new byte[]{this.getType().getType()};
    }

    @Override
    public byte[] deserialize(Integer data) {
        byte[] sData = BinaryUtils.intToBytes(data);
        byte[] result = new byte[sData.length + 1];

        System.arraycopy(this.typeArray, 0, result, 0, this.typeArray.length);
        System.arraycopy(sData, 0, result, this.typeArray.length, sData.length);

        return result;
    }

    @Override
    public Integer serialize(byte[] data) {
        return BinaryUtils.bytesToInt(Arrays.copyOfRange(data, 1, data.length));
    }

    @Override
    public Type<Integer> getType() {
        return this.type;
    }

    public static final class IntType implements Type<Integer> {

        @Override
        public byte getType() {
            return 0x01;
        }

        @Override
        public Class<Integer> getClazz() {
            return Integer.class;
        }
    }

}
