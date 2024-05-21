package io.korona.core.db;

import io.korona.core.data.Entry;
import io.korona.core.db.index.Key;

import java.io.IOException;

/**
 * @program: korona-db
 * @description:
 * @author: H4rry217
 **/

public interface Engine {

    public void write(Entry dataEntry) throws IOException;

    public Entry read(Key key) throws IOException;

    public void delete(Key key) throws IOException;

}
