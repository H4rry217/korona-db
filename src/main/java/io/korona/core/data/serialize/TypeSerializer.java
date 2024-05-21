package io.korona.core.data.serialize;

public interface TypeSerializer<T> {

    public byte[] deserialize(T data);

    public T serialize(byte[] data);

    public Type<T> getType();

}
