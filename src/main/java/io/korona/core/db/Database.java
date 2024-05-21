package io.korona.core.db;

/**
 * @program: korona-db
 * @description:
 * @author: H4rry217
 **/

public interface Database {

    public Object get(Object key);

    public void put(Object key, Object value);

    public void delete(Object key);

}
