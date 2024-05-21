package io.korona.core;

import io.korona.core.buffer.Pooled;
import io.korona.core.buffer.WrappedBytBuf;
import io.korona.core.buffer.impl.BufferAllocator;
import io.korona.core.data.DataEntry;
import io.korona.core.data.Entry;
import io.korona.core.data.TombStone;
import io.korona.core.db.BytesKey;
import io.korona.core.db.DataFile;
import io.korona.core.db.Engine;
import io.korona.core.db.index.HashMapKeydir;
import io.korona.core.db.index.Key;
import io.korona.core.db.index.Keydir;
import io.korona.core.db.index.ValueIndex;
import io.korona.core.exception.OverLimitException;
import io.korona.core.factory.EntryFactory;
import io.korona.core.factory.file.DataFileFactory;
import io.korona.core.metadata.options.CompactOption;
import io.korona.core.metadata.options.KoronaOption;
import io.korona.core.support.ReadTrentCalculator;
import io.korona.utils.BinaryUtils;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @program: korona-db
 * @description:
 * @author: H4rry217
 **/

public abstract class RWEngine implements Engine {

    public final KoronaOption option;

    /** CRC(8) TIMESTAMP(8) KEYSZ(4) VALUESZ(4) KEYDATA(MAX_DATA_SIZE) VALUEDATA(MAX_DATA_SIZE) = 24b + 1GB **/
    public final int MAX_ENTRY_SIZE;

    public final int MAX_BODY_SIZE;

    public final int MAX_FILE_SIZE;

    protected final Path workPath;

    protected final Path dataPath;

    protected volatile DataFile inactiveFile;

    protected Map<Long, DataFile> dataFiles = new HashMap<>();

    protected final DataFileFactory dataFileFactory;

    protected final EntryFactory entryFactory;

    protected volatile Keydir keydir;

    private final ReadWriteLock keydirLock = new ReentrantReadWriteLock();

    private final ReadTrentCalculator readTrentCalculator;

    private final ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(3);

    private final Map<String, ScheduledFuture<?>> scheduledFutureMap = new HashMap<>();

    public RWEngine(Path workPath, DataFileFactory dataFileFactory, EntryFactory entryFactory, KoronaOption option){
        this.workPath = workPath;
        this.dataFileFactory = dataFileFactory;
        this.entryFactory = entryFactory;
        this.option = option;

        this.MAX_FILE_SIZE = option.maxFileSize * 1024 * 1024;

        int bodySize = option.maxSubDataSize + option.maxSubDataSize;
        int tempSize = Entry.HEADER_SIZE + bodySize;
        this.MAX_ENTRY_SIZE = tempSize + (4096 - (tempSize % 4096));
        this.MAX_BODY_SIZE = (this.MAX_ENTRY_SIZE - Entry.HEADER_SIZE) / 2;

        this.dataPath = workPath.resolve("data");

        File storagePath = this.dataPath.toFile();
        if(!storagePath.exists() || !storagePath.isDirectory()) storagePath.mkdirs();

        File[] listFile = storagePath.listFiles();

        DataFile lastActiveFile = null;
        assert listFile != null;

        List<DataFile> fileList = this.dataFileFactory.loadDataFile();
        long maxSizeByte = this.MAX_FILE_SIZE;

        for (DataFile dataFile : fileList) {
            this.dataFiles.put(dataFile.getFileId(), dataFile);

            // is datafile unreadable
            try {
                if(dataFile.isReadOnly() || dataFile.getPosition() > maxSizeByte){
                    dataFile.readOnly();
                }else{
                    if(lastActiveFile != null) {
                        lastActiveFile.readOnly();
                    }

                    lastActiveFile = dataFile;
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

        }

        if(lastActiveFile == null){
            lastActiveFile = this.dataFileFactory.createDataFile(Long.toString(System.currentTimeMillis()));
            this.dataFiles.put(lastActiveFile.getFileId(), lastActiveFile);
        }

        this.inactiveFile = lastActiveFile;

        this.initKeydir();

        this.readTrentCalculator = new ReadTrentCalculator(
                option.compact.interval,
                10000,
                option.compact.thresholdPercent
        );

        this.background();

    }

    protected void background(){
        //start compaction process
        ScheduledFuture<?> sf1 = null;
        ScheduledFuture<?> sf2 = null;
        ScheduledFuture<?> sf3 = null;

        sf1 = this.scheduledExecutorService.scheduleWithFixedDelay(new CompactThread(), this.option.compact.interval, this.option.compact.interval, TimeUnit.SECONDS);

        if(option.dataSync > 0){
            // start datasync process
            sf2 = this.scheduledExecutorService.scheduleWithFixedDelay(new FlushDataThread(), this.option.dataSync, this.option.dataSync, TimeUnit.SECONDS);
            this.scheduledFutureMap.put("datasync", sf2);
        }

        if(option.keydir.dumpFile){
            sf3 = this.scheduledExecutorService.scheduleWithFixedDelay(new KeydirDumpThread(), this.option.keydir.interval, this.option.keydir.interval, TimeUnit.SECONDS);
            this.scheduledFutureMap.put("keydir.interval", sf3);
        }

        this.scheduledFutureMap.put("compact.interval", sf1);
    }

    protected void initKeydir(){
        Keydir mainKeydir = new HashMapKeydir();

        boolean loadByIdx = false;
        if(this.option.keydir.dumpFile){
            File keydirFile = this.dataPath.resolve("keydir.kd").toFile();

            if(keydirFile.exists()){
                try(InputStream inputStream = new BufferedInputStream(new FileInputStream(keydirFile))){
                    mainKeydir.load(inputStream);
                    loadByIdx = true;
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

                System.out.println("load keydir into memory by local-file");
            }
        }

        if(!loadByIdx){
            for (Map.Entry<Long, DataFile> entry : this.dataFiles.entrySet()) {
                mainKeydir.merge(entry.getValue().toKeydir());
            }

            System.out.println("generate memory keydir by sequential-read");
        }

        this.keydir = mainKeydir;
    }

    public void ensureFreeSpace(int writeLen){
        if(this.inactiveFile.getPosition() + writeLen > this.MAX_FILE_SIZE){
            try {
                this.inactiveFile.readOnly();
            } catch (IOException e) {
                throw new RuntimeException(e);
            } finally {
                if(this.inactiveFile.isReadOnly()){
                    this.inactiveFile = this.dataFileFactory.createDataFile(Long.toString(System.currentTimeMillis()));
                    this.dataFiles.put(this.inactiveFile.getFileId(), this.inactiveFile);
                }
            }

        }
    }

    @Override
    public Entry read(Key key) {
        this.keydirLock.readLock().lock();
        Entry entry = null;

        try{
            ValueIndex valueIndex = this.keydir.getValueIndex(key);

            if(valueIndex != null){
                DataFile dataFile = this.dataFiles.get(valueIndex.fileId());;
                if(dataFile != null){
                    WrappedBytBuf tempBytBuf = BufferAllocator.POOLED.allocateDirect(valueIndex.entrySize());
                    ByteBuffer entryBuffer = tempBytBuf.getByteBuffer();

                    dataFile.read(entryBuffer, valueIndex.entryOffset(), valueIndex.entrySize());
                    entry = this.entryFactory.buildEntry(entryBuffer);

                    if(tempBytBuf instanceof Pooled) ((Pooled) tempBytBuf).release();

                }
            }
        }catch (Exception e){
            throw new RuntimeException(e);
        }finally {
            this.keydirLock.readLock().unlock();
        }

        return entry;
    }

    @Override
    public void write(Entry dataEntry) throws IOException {
        this.ensureFreeSpace(dataEntry.getSize());
        var buffer = this.buildBuffer(dataEntry);

        long pos = this.inactiveFile.append(buffer.getByteBuffer());

        Key key = new BytesKey(dataEntry.key());
        if(dataEntry.isTombStone()){
            this.keydir.deleteKey(key);
        }else{
            this.keydir.updateKey(
                    key,
                    this.inactiveFile.getFileId(),
                    dataEntry.getSize(),
                    pos,
                    System.currentTimeMillis()
            );
        }

        if(buffer instanceof Pooled) ((Pooled) buffer).release();
    }

    @Override
    public void delete(Key key) throws IOException {
        TombStone tombStone = TombStone.tombStone(key);
        this.write(tombStone);
    }

    public void close() throws IOException {
        this.keydirLock.writeLock().lock();
        this.scheduledExecutorService.shutdownNow();

        for (Map.Entry<Long, DataFile> entry : this.dataFiles.entrySet()) {
            entry.getValue().sync();
            entry.getValue().close();
        }

        this.keydir.dump(this.dataPath);

        this.keydirLock.writeLock().unlock();
    }

    protected WrappedBytBuf buildBuffer(Entry dataEntry){
        int bodySize = dataEntry.keySize() + dataEntry.valueSize();

        if(bodySize > this.MAX_BODY_SIZE){
            throw new OverLimitException("data size " + bodySize + " over limit " + this.MAX_BODY_SIZE);
        }

        var timeBytes = BinaryUtils.longToBytes(dataEntry.timestamp());
        var kzBytes = BinaryUtils.intToBytes(dataEntry.keySize());
        var vzBytes = BinaryUtils.intToBytes(dataEntry.valueSize());

        long bodyCrc = DataEntry.entryCrc(dataEntry.timestamp(), dataEntry.key(), dataEntry.value());

        int entrySize = Entry.HEADER_SIZE + bodySize;

        WrappedBytBuf byteBuffer = BufferAllocator.POOLED.allocateDirect(entrySize);

        byteBuffer.getByteBuffer()
                .put(BinaryUtils.longToBytes(bodyCrc))
                .put(timeBytes)
                .put(kzBytes)
                .put(vzBytes)
                .put(dataEntry.key())
                .put(dataEntry.value());

        byteBuffer.getByteBuffer().flip();

        return byteBuffer;
    }

    public Entry _sequenceRead(long fileId, byte[] key) throws IOException {
        DataFile file = this.dataFiles.get(fileId);
        if(file == null) throw new RuntimeException("Datafile with fileId "+fileId+ " not found!");

        int offset = file.getMetadata().getMetadataLength();

        int latestValOffset = -1;
        int latestValSize = 0;

        WrappedBytBuf tempBytBuf = BufferAllocator.POOLED.allocateDirect(8);
        ByteBuffer dataSizeBuffer = tempBytBuf.getByteBuffer();

        while (offset < file.getPosition()){
            file.read(dataSizeBuffer, offset + 16, 8);

            int keySize = dataSizeBuffer.getInt();
            int valueSize = dataSizeBuffer.getInt();
            int entrySize = Entry.HEADER_SIZE + keySize + valueSize;

            int nextOffset = offset + entrySize;

            if(valueSize > 0){
                if(keySize == key.length){
                    WrappedBytBuf bytBuf = BufferAllocator.UNPOOLED.allocate(keySize);
                    ByteBuffer keyBuffer = bytBuf.getByteBuffer();

                    file.read(keyBuffer, offset + Entry.HEADER_SIZE, keySize);

                    boolean keyEqual = keyBuffer.hasRemaining();
                    if(keyBuffer.hasRemaining()){
                        for (byte b : key) {
                            if(b != keyBuffer.get()){
                                keyEqual = false;
                                break;
                            }
                        }
                    }

                    if(keyEqual){
                        latestValOffset = offset;
                        latestValSize = entrySize;
                    }
                }
            }

            offset = nextOffset;
        }

        if(tempBytBuf instanceof Pooled) ((Pooled) tempBytBuf).release();

        Entry entry = null;
        if(latestValOffset != -1){
            WrappedBytBuf entryBytBuf = BufferAllocator.POOLED.allocateDirect(latestValSize);
            ByteBuffer entryByteBuffer = entryBytBuf.getByteBuffer();

            file.read(entryByteBuffer, latestValOffset, latestValSize);
            entry = this.entryFactory.buildEntry(entryByteBuffer);

            if(entryBytBuf instanceof Pooled) ((Pooled) entryBytBuf).release();
        }

        return entry;
    }

    public void sync() throws IOException {
        RWEngine.this.keydirLock.readLock().lock();
        RWEngine.this.inactiveFile.sync();
        RWEngine.this.keydirLock.readLock().unlock();
    }

    public Keydir _getKeydir(){
        return this.keydir;
    }

    public long getCompactionTimeRemaining(){
        return this.scheduledFutureMap.containsKey("compact.interval")? this.scheduledFutureMap.get("compact.interval").getDelay(TimeUnit.MILLISECONDS): -9999;
    }

    public long getDataSyncTimeRemaining(){
        return this.scheduledFutureMap.containsKey("datasync")? this.scheduledFutureMap.get("datasync").getDelay(TimeUnit.MILLISECONDS): -9999;
    }

    public long getKeydirDumpTimeRemaining(){
        return this.scheduledFutureMap.containsKey("keydir.interval")? this.scheduledFutureMap.get("keydir.interval").getDelay(TimeUnit.MILLISECONDS): -9999;
    }


    private final class KeydirDumpThread extends Thread{

        @Override
        public void run() {
            try {
                RWEngine.this.keydir.dump(RWEngine.this.dataPath);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private final class FlushDataThread extends Thread{

        @Override
        public void run() {
            try {
                RWEngine.this.sync();

            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

    }

    private final class CompactThread extends Thread{

        @Override
        public void run() {
            CompactOption compactOption = RWEngine.this.option.compact;

            int count = RWEngine.this.readTrentCalculator.getWindowCount();
            if(compactOption.thresholdValue > count){
                if(RWEngine.this.readTrentCalculator.isSmooth() || compactOption.thresholdPercent <= 0){
                    RWEngine.this.keydirLock.writeLock().lock();

                    try {

                        RWEngine engine = RWEngine.this;

                        engine.inactiveFile.readOnly();

                        DataFile largeMergeFile = engine.dataFileFactory.createDataFile(Long.toString(System.currentTimeMillis()));
                        engine.inactiveFile = largeMergeFile;
                        engine.dataFiles.put(largeMergeFile.getFileId(), largeMergeFile);

                        io.korona.core.db.index.Keydir newKeydir = new HashMapKeydir();
                        Iterator<Key> keys = RWEngine.this.keydir.getKeys();

                        //copy keydir and append data to merge-datafile
                        while(keys.hasNext()){
                            Key key = keys.next();

                            Entry entry = engine.read(key);

                            WrappedBytBuf wrappedBytBuf = BufferAllocator.POOLED.allocateDirect(entry.getSize());
                            ByteBuffer buffer = wrappedBytBuf.getByteBuffer();

                            buffer.putLong(entry.crc())
                                    .putLong(entry.timestamp())
                                    .putInt(entry.keySize())
                                    .putInt(entry.valueSize())
                                    .put(entry.key())
                                    .put(entry.value());

                            buffer.flip();
                            largeMergeFile.append(buffer);
                            ((Pooled)wrappedBytBuf).release();


                            newKeydir.updateKey(
                                    key,
                                    largeMergeFile.getFileId(),
                                    entry.getSize(),
                                    largeMergeFile.getPosition() - entry.getSize(),
                                    System.currentTimeMillis()
                            );

                            keys.remove();
                        }

                        largeMergeFile.sync();

                        //clean old datafiles
                        Iterator<Map.Entry<Long, DataFile>> files = engine.dataFiles.entrySet().iterator();
                        while(files.hasNext()){
                            Map.Entry<Long, DataFile> fileEntry = files.next();

                            if(fileEntry.getKey() != largeMergeFile.getFileId()){
                                fileEntry.getValue().close();
                                fileEntry.getValue().getPath().toFile().delete();

                                files.remove();
                            }

                        }

                        //change to new keydir
                        engine.keydir = newKeydir;

                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    } finally {
                        RWEngine.this.keydirLock.writeLock().unlock();
                    }

                }


            }
        }
    }

}
