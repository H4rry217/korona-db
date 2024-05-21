package io.korona.core.buffer.impl.allocator;

import io.korona.core.buffer.BytBufAllocator;
import io.korona.core.buffer.WrappedBytBuf;
import io.korona.core.buffer.impl.SimpleBytBuf;

import java.nio.ByteBuffer;

/**
 * @program: korona-db
 * @description:
 * @author: H4rry217
 **/

public class SimplePooledBytBufAllocator implements BytBufAllocator {

    //TODO POOLED FOR BYTEBUFFER

    @Override
    public WrappedBytBuf allocate(int size) {
        return new SimpleBytBuf(ByteBuffer.allocate(size));
    }

    @Override
    public WrappedBytBuf allocateDirect(int size) {
        return new SimpleBytBuf(ByteBuffer.allocateDirect(size));
    }

}
