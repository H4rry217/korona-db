package io.korona.core;

import io.korona.core.data.Entry;
import io.korona.core.data.serialize.TypeConvert;
import io.korona.core.db.DataFile;
import io.korona.core.db.index.HashMapKeydir;
import io.korona.core.db.index.Keydir;
import io.korona.core.exception.NotSupportTypeException;
import io.korona.core.factory.file.FileMetadataFactory;
import io.korona.core.metadata.FileMetadata;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

/**
 * @program: korona-db
 * @description:
 * @author: H4rry217
 **/

public class AppendDataFile extends DataFile{

    protected final FileChannel wFileChannel;
    protected final FileChannel rFileChannel;

    public AppendDataFile(String path, String fileName, FileMetadataFactory fileMetadataFactory) {
        this(new File(path + "\\" + fileName + ".db"), fileMetadataFactory);
    }

    public AppendDataFile(File file, FileMetadataFactory fileMetadataFactory) {
        super(file.getPath(), fileMetadataFactory);

        try {
            this.rFileChannel = FileChannel.open(this.file.toPath(), StandardOpenOption.READ);

            if(this.isReadOnly()){
                this.wFileChannel = null;
            }else{
                this.wFileChannel = FileChannel.open(file.toPath(), StandardOpenOption.CREATE, StandardOpenOption.APPEND, StandardOpenOption.SYNC);
                this.writeOffset.set(this.wFileChannel.position());
            }

        }  catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public long append(byte[] bytes) throws IOException {
        return this.append(ByteBuffer.wrap(bytes));
    }

    @Override
    public long append(ByteBuffer buffer) throws IOException {
        long writePos = this.writeOffset.getAndAdd(buffer.limit());
        this.wFileChannel.write(buffer);

        return writePos;
    }

    @Override
    public void read(ByteBuffer byteBuffer, long pos, long size) throws IOException {
        if(size > Integer.MAX_VALUE){
            throw new RuntimeException("read data size excess Integer.MAX");
        }

        this.rFileChannel.read(byteBuffer, pos);
        byteBuffer.flip();
    }

    @Override
    protected void randomWrite(ByteBuffer byteBuffer, long position) throws IOException {
        this.wFileChannel.write(byteBuffer, position);
    }

    @Override
    public boolean readOnly() throws IOException {
        if(super.readOnly()){
            this.wFileChannel.close();
            return true;
        }

        return false;
    }

    @Override
    public void sync() throws IOException {
        if(!isReadOnly()) this.wFileChannel.force(true);
    }

    @Override
    public boolean close() throws IOException {
        if(this.closed.compareAndSet(false, true)){
            if(this.wFileChannel != null && this.wFileChannel.isOpen()) this.wFileChannel.close();
            if(this.rFileChannel.isOpen()) this.rFileChannel.close();

            return true;
        }

        return false;
    }

    @Override
    public Keydir toKeydir() {
        Keydir keydir = new HashMapKeydir();

        ByteBuffer headerByteBuf = ByteBuffer.allocateDirect(Entry.HEADER_SIZE);
        long readPos = FileMetadata.METADATA_SIZE;

        try {
            long fileSize = this.rFileChannel.size();

            while(readPos < fileSize){
                this.rFileChannel.read(headerByteBuf, readPos);
                headerByteBuf.flip();

                long crc = headerByteBuf.getLong();
                long timestamp = headerByteBuf.getLong();
                int keySize = headerByteBuf.getInt();
                int valueSize = headerByteBuf.getInt();

                if(valueSize > 0){
                    ByteBuffer keyBuffer = ByteBuffer.allocate(keySize);
                    this.rFileChannel.read(keyBuffer, readPos + headerByteBuf.capacity());
                    Object keyObject = TypeConvert.serialize(keyBuffer.array());

                    int entrySize = Entry.HEADER_SIZE + keySize + valueSize;

                    keydir.updateKey(
                            keyObject,
                            this.getFileId(),
                            entrySize,
                            readPos,
                            timestamp
                    );

                    readPos += entrySize;
                }

            }

        } catch (IOException e) {
            return keydir;
        } catch (NotSupportTypeException ignore){

        }


        return keydir;
    }
}
