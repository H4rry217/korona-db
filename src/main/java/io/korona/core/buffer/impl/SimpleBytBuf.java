package io.korona.core.buffer.impl;

import io.korona.core.buffer.Pooled;
import io.korona.core.buffer.WrappedBytBuf;

import java.nio.ByteBuffer;

/**
 * @program: korona-db
 * @description:
 * @author: H4rry217
 **/

public class SimpleBytBuf extends WrappedBytBuf implements Pooled {

    public SimpleBytBuf(ByteBuffer byteBuffer) {
        super(byteBuffer);
    }

    @Override
    public void release() {

    }
}
