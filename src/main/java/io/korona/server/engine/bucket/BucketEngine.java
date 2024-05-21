package io.korona.server.engine.bucket;

import io.korona.core.RWEngine;
import io.korona.core.data.Entry;
import io.korona.core.db.BytesKey;
import io.korona.core.db.index.Key;
import io.korona.core.factory.EntryFactory;
import io.korona.core.factory.file.DataFileFactory;
import io.korona.core.metadata.options.KoronaOption;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Map;

/**
 * @program: korona-db
 * @description:
 * @author: H4rry217
 **/

public class BucketEngine extends RWEngine {

    public final int BUFFER_WRITE_THRESHOLD;

    private final Object writeLock = new Object();

    private final ByteBuffer writeBuffer;

    public BucketEngine(Path workPath, DataFileFactory dataFileFactory, EntryFactory entryFactory, KoronaOption databaseOptions) {
        super(workPath, dataFileFactory, entryFactory, databaseOptions);

        Map<String, Object> engineOptions = (Map<String, Object>) databaseOptions.options.get("bucket-setting");
        this.BUFFER_WRITE_THRESHOLD = (int) engineOptions.get("write-buffer-size");

        this.writeBuffer = ByteBuffer.allocate(this.BUFFER_WRITE_THRESHOLD);
    }

    @Override
    public void write(Entry dataEntry) throws IOException {
        int bodySize = Entry.HEADER_SIZE + dataEntry.keySize() + dataEntry.valueSize();

        synchronized (this.writeLock) {
            long beforePos = -1;

            var byteBuffer = this.buildBuffer(dataEntry);

            if (this.option.dataSync > 0 && bodySize < this.BUFFER_WRITE_THRESHOLD) {
                beforePos = write0(byteBuffer.getByteBuffer());
            } else {
                //keep sequential write, flush the buffer
                this.flushBuffer();

                this.ensureFreeSpace(byteBuffer.getByteBuffer().capacity());
                beforePos = this.inactiveFile.append(byteBuffer.getByteBuffer());
            }

            Key key = new BytesKey(dataEntry.key());
            if(dataEntry.isTombStone()){
                this.keydir.deleteKey(key);
            }else{
                this.keydir.updateKey(
                        key,
                        this.inactiveFile.getFileId(),
                        byteBuffer.getByteBuffer().capacity(),
                        beforePos,
                        System.currentTimeMillis()
                );
            }

            if(this.option.dataSync == -1) {
                this.flushBuffer();
                this.inactiveFile.sync();
            }

        }

    }

    private long write0(ByteBuffer byteBuf) throws IOException {
        this.ensureFreeSpace(byteBuf.capacity());

        int dataLen = byteBuf.capacity();

        if(this.writeBuffer.remaining() >= dataLen){
            this.writeBuffer.put(byteBuf);
        }else{
            int dataRemaining = dataLen;

            while (dataRemaining > 0){
                var startOffset = dataLen - dataRemaining;
                var writeLen = Math.min(dataLen - startOffset, this.writeBuffer.remaining());

                this.writeBuffer.put(byteBuf.slice(startOffset, writeLen));
                byteBuf.position(startOffset + writeLen);

                dataRemaining -= writeLen;

                //still has data and buffer full, so need flush buffer to keep we have free-space save remaining data
                if(dataRemaining > 0 && !this.writeBuffer.hasRemaining()){
                    this.flushBuffer();
                }
            }

        }

        return this.inactiveFile.getPosition() - dataLen;
    }

    @Override
    public void sync() throws IOException {
        this.flushBuffer();
        super.sync();
    }

    public void flushBuffer() throws IOException {
        if(this.writeBuffer.position() > 0){
            this.writeBuffer.flip();
            this.inactiveFile.append(this.writeBuffer);
            this.writeBuffer.compact();
        }
    }

    public ByteBuffer _getWriteBuffer(){
        return this.writeBuffer;
    }

}
