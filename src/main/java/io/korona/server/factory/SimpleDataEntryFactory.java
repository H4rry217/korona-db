package io.korona.server.factory;

import io.korona.core.data.Entry;
import io.korona.core.factory.EntryFactory;
import io.korona.server.SimpleData;

import java.nio.ByteBuffer;

/**
 * @program: korona-db
 * @description:
 * @author: H4rry217
 **/

public class SimpleDataEntryFactory implements EntryFactory {

    @Override
    public Entry buildEntry(ByteBuffer byteBuffer) {
        return new SimpleData(byteBuffer);
    }

    @Override
    public Entry buildEntry(byte[] bytes) {
        return null;
    }

}
