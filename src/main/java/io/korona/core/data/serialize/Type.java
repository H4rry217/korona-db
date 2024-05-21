package io.korona.core.data.serialize;

public interface Type<T> {

    public byte getType();

    public Class<T> getClazz();

}
