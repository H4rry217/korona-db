package io.korona.core.buffer.impl;

import io.korona.core.buffer.BytBufAllocator;
import io.korona.core.buffer.impl.allocator.DefaultUnpooledBytBufAllocator;
import io.korona.core.buffer.impl.allocator.SimplePooledBytBufAllocator;

/**
 * @program: korona-db
 * @description:
 * @author: H4rry217
 **/

public class BufferAllocator {

    public static final BytBufAllocator POOLED = new SimplePooledBytBufAllocator();

    public static final BytBufAllocator UNPOOLED = new DefaultUnpooledBytBufAllocator();

}
