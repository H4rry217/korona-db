package io.korona.core.db;

import io.korona.core.db.index.Key;

import java.util.Arrays;

/**
 * @program: korona-db
 * @description:
 * @author: H4rry217
 **/

public class BytesKey implements Key {

    public final byte[] bytes;

    private final int bytesHashCode;
    public BytesKey(byte[] bytes) {
        this.bytes = bytes;
        this.bytesHashCode = Arrays.hashCode(bytes);
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof BytesKey) return Arrays.equals(this.bytes, ((BytesKey) obj).bytes);
        return false;
    }

    @Override
    public int hashCode() {
        return this.bytesHashCode;
    }

    @Override
    public byte[] bytes() {
        return this.bytes;
    }
}
