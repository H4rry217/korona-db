package io.korona.core.buffer;

import java.nio.ByteBuffer;

/**
 * @program: korona-db
 * @description:
 * @author: H4rry217
 **/

public abstract class WrappedBytBuf {

    private final ByteBuffer byteBuffer;

    public WrappedBytBuf(ByteBuffer byteBuffer){
        this.byteBuffer = byteBuffer;
    }

    public ByteBuffer getByteBuffer() {
        return byteBuffer;
    }
}
