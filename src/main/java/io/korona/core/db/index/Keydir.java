package io.korona.core.db.index;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.Iterator;

/**
 * @program: korona-db
 * @description:
 * @author: H4rry217
 **/

public interface Keydir {

    public ValueIndex getValueIndex(Key key);

    public void updateKey(Object keyOfAnyObj, long fileId, int size, long offset, long timestamp);

    public void updateKey(Key key, long fileId, int size, long offset, long timestamp);

    public void deleteKey(Key key);

    public Iterator<Key> getKeys();

    public int size();

    public void merge(Keydir keydir);

    public File dump(Path dir) throws IOException;

    public void load(InputStream inputStream) throws IOException;

}
