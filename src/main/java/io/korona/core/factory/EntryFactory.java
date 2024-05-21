package io.korona.core.factory;

import io.korona.core.data.Entry;

import java.nio.ByteBuffer;

public interface EntryFactory {

    public Entry buildEntry(ByteBuffer byteBuffer);

    public Entry buildEntry(byte[] bytes);

}
