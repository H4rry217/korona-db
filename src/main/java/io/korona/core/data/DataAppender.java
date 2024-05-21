package io.korona.core.data;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface DataAppender {

    public long append(byte[] bytes) throws IOException;

    public long append(ByteBuffer buffer) throws IOException;

    public boolean isReadOnly();

    public void read(ByteBuffer byteBuffer, long pos, long size) throws IOException;

}
