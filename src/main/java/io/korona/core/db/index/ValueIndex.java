package io.korona.core.db.index;

/**
 * @program: korona-db
 * @description:
 * @author: H4rry217
 **/

public interface ValueIndex {

    public long fileId();

    public int entrySize();

    public long entryOffset();

    public long timestamp();

}
