package io.korona.core.typeserialize;

import io.korona.core.data.serialize.Type;
import io.korona.core.data.serialize.TypeSerializer;

/**
 * @program: korona-db
 * @description:
 * @author: H4rry217
 **/

public class NullTypeSerializer implements TypeSerializer<Object> {

    @Override
    public byte[] deserialize(Object data) {
        return new byte[0];
    }

    @Override
    public Object serialize(byte[] data) {
        return null;
    }

    @Override
    public Type<Object> getType() {
        return null;
    }

}
