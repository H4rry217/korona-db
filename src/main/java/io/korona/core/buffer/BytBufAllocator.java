package io.korona.core.buffer;

/**
 * @program: korona-db
 * @description:
 * @author: H4rry217
 **/

public interface BytBufAllocator {

    public WrappedBytBuf allocate(int size);

    public WrappedBytBuf allocateDirect(int size);

}
