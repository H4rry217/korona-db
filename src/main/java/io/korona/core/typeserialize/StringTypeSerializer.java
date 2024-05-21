package io.korona.core.typeserialize;

import io.korona.core.data.serialize.Type;
import io.korona.core.data.serialize.TypeSerializer;

import java.util.Arrays;

/**
 * @program: korona-db
 * @description:
 * @author: H4rry217
 **/

public class StringTypeSerializer implements TypeSerializer<String> {

    private final Type<String> type;
    private final byte[] typeArray;

    public StringTypeSerializer(){
        this.type = new StringType();
        this.typeArray = new byte[]{this.getType().getType()};
    }

    @Override
    public byte[] deserialize(String data) {
        byte[] sData = data.getBytes();
        byte[] result = new byte[sData.length + 1];

        System.arraycopy(this.typeArray, 0, result, 0, this.typeArray.length);
        System.arraycopy(sData, 0, result, this.typeArray.length, sData.length);

        return result;
    }

    @Override
    public String serialize(byte[] data) {
        return new String(Arrays.copyOfRange(data, 1, data.length));
    }

    @Override
    public Type<String> getType() {
        return this.type;
    }

    public static final class StringType implements Type<String> {

        @Override
        public byte getType() {
            return 0x03;
        }

        @Override
        public Class<String> getClazz() {
            return String.class;
        }
    }

}
