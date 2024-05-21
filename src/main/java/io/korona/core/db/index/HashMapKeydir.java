package io.korona.core.db.index;

import io.korona.core.data.serialize.TypeConvert;
import io.korona.core.db.BytesKey;
import io.korona.utils.BinaryUtils;

import java.io.*;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @program: korona-db
 * @description:
 * @author: H4rry217
 **/

public final class HashMapKeydir implements Keydir{

    private final Map<Key, ValueIndex> map = new ConcurrentHashMap<>();

    private final ReadWriteLock dumpLock = new ReentrantReadWriteLock();

    @Override
    public ValueIndex getValueIndex(Key key) {
        return this.map.get(key);
    }

    @Override
    public void updateKey(Object keyOfAnyObj, long fileId, int size, long offset, long timestamp) {
        this.dumpLock.readLock().lock();

        Key keySerialize = new BytesKey(TypeConvert.deserialize(keyOfAnyObj));
        updateKey(keySerialize, fileId, size, offset, timestamp);

        this.dumpLock.readLock().unlock();
    }

    @Override
    public void updateKey(Key key, long fileId, int size, long offset, long timestamp) {
        this.dumpLock.readLock().lock();

        ValueIndex valueIndex = new ValueIndexImpl(fileId, size, offset, timestamp);
        this.map.put(key, valueIndex);

        this.dumpLock.readLock().unlock();
    }

    @Override
    public void deleteKey(Key key) {
        this.dumpLock.readLock().lock();
        this.map.remove(key);
        this.dumpLock.readLock().unlock();
    }

    @Override
    public Iterator<Key> getKeys() {
        return this.map.keySet().iterator();
    }

    @Override
    public int size() {
        return this.map.size();
    }

    @Override
    public void merge(Keydir mgrKeydir) {
        Iterator<Key> mgrKeys = mgrKeydir.getKeys();
        while(mgrKeys.hasNext()){
            Key key = mgrKeys.next();

            ValueIndex mgrValueIndex = mgrKeydir.getValueIndex(key);
            ValueIndex curValueIndex = this.map.get(key);

            //update keydir while current valueIndex is null or mgrValueIndex is newest
            if(curValueIndex == null || curValueIndex.timestamp() < mgrValueIndex.timestamp()){
                this.map.put(key, mgrValueIndex);
            }

        }
    }

    @Override
    public File dump(Path dir) throws IOException{
        this.dumpLock.writeLock().lock();

        File f = dir.resolve("keydir.kd").toFile();
        try (OutputStream outputStream = new BufferedOutputStream(new FileOutputStream(f))){

            Iterator<Map.Entry<Key, ValueIndex>> it = this.map.entrySet().iterator();
            while(it.hasNext()){
                Map.Entry<Key, ValueIndex> entry = it.next();
                Key key = entry.getKey();

                outputStream.write(BinaryUtils.intToBytes(key.bytes().length));
                outputStream.write(key.bytes());
                outputStream.write(BinaryUtils.longToBytes(entry.getValue().fileId()));
                outputStream.write(BinaryUtils.intToBytes(entry.getValue().entrySize()));
                outputStream.write(BinaryUtils.longToBytes(entry.getValue().entryOffset()));
                outputStream.write(BinaryUtils.longToBytes(entry.getValue().timestamp()));

            }

            outputStream.flush();

        } finally {
            this.dumpLock.writeLock().unlock();
        }

        return f;
    }

    @Override
    public void load(InputStream inputStream) throws IOException{
        byte[] lenInt;
        while((lenInt = inputStream.readNBytes(4)).length > 0){

            int len = BinaryUtils.bytesToInt(lenInt);

            byte[] bytes = inputStream.readNBytes(len);
            BytesKey bytesKey = new BytesKey(bytes);

            this.updateKey(
                    bytesKey,
                    BinaryUtils.bytesToLong(inputStream.readNBytes(8)),
                    BinaryUtils.bytesToInt(inputStream.readNBytes(4)),
                    BinaryUtils.bytesToLong(inputStream.readNBytes(8)),
                    BinaryUtils.bytesToLong(inputStream.readNBytes(8))
            );

        }

    }

    public record ValueIndexImpl(long fileId, int entrySize, long entryOffset, long timestamp) implements ValueIndex {

    }

}
