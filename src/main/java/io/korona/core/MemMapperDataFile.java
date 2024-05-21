package io.korona.core;

import io.korona.core.data.Entry;
import io.korona.core.data.serialize.TypeConvert;
import io.korona.core.db.DataFile;
import io.korona.core.db.index.HashMapKeydir;
import io.korona.core.db.index.Keydir;
import io.korona.core.factory.file.FileMetadataFactory;
import io.korona.core.metadata.FileMetadata;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

/**
 * @program: korona-db
 * @description:
 * @author: H4rry217
 **/

public class MemMapperDataFile extends DataFile {

    private MappedByteBuffer mappedByteBuffer;

    private FileChannel fileChannel;

    public MemMapperDataFile(String path, String fileName, FileMetadataFactory fileMetadataFactory, int fileSize) {
        this(new File(path + "\\" + fileName + ".db"), fileMetadataFactory, fileSize);
    }

    public MemMapperDataFile(File file, FileMetadataFactory fileMetadataFactory, int fileSize) {
        super(file.getPath(), fileMetadataFactory);

        try {

            this.fileChannel = FileChannel.open(this.file.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE);
            if(!isReadOnly()){

                //find last value position
                ByteBuffer headerByteBuf = ByteBuffer.allocateDirect(Entry.HEADER_SIZE);
                long readPos = FileMetadata.METADATA_SIZE;
                long maxFileSize = this.fileChannel.size();

                if(maxFileSize > FileMetadata.METADATA_SIZE){
                    while(readPos < maxFileSize) {
                        this.fileChannel.read(headerByteBuf, readPos);
                        headerByteBuf.flip();

                        long crc = headerByteBuf.getLong();
                        long timestamp = headerByteBuf.getLong();
                        int keySize = headerByteBuf.getInt();
                        int valueSize = headerByteBuf.getInt();

                        int entrySize = Entry.HEADER_SIZE + keySize + valueSize;

                        if(crc == 0 && timestamp == 0) {
                            this.writeOffset.set(readPos);
                            break;
                        }else {
                            readPos += entrySize;
                            headerByteBuf.compact();
                        }
                    }

                }else{
                    this.writeOffset.set(maxFileSize);
                }

                this.mappedByteBuffer = this.fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, fileSize);
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long append(byte[] bytes) throws IOException {
        return this.append(ByteBuffer.wrap(bytes));
    }

    @Override
    public long append(ByteBuffer buffer) throws IOException {
        MappedByteBuffer slice = this.mappedByteBuffer.slice();
        long writePos = this.writeOffset.get();

        //bump pointer
        while(!this.writeOffset.compareAndSet(writePos, writePos + buffer.capacity())){
            writePos = this.writeOffset.get();
        }

        slice.position((int) writePos);
        slice.put(buffer);

        return writePos;
    }

    @Override
    public void read(ByteBuffer byteBuffer, long pos, long size) throws IOException {
        if(size > Integer.MAX_VALUE){
            throw new RuntimeException("read data size excess Integer.MAX");
        }

        MappedByteBuffer slice = this.mappedByteBuffer.slice();
        slice.position((int) pos).limit((int) (pos + size));

        byteBuffer.put(slice);
        byteBuffer.flip();
    }

    @Override
    protected void randomWrite(ByteBuffer byteBuffer, long position) throws IOException {
        MappedByteBuffer slice = this.mappedByteBuffer.slice();
        slice.position((int) position).put(byteBuffer);
    }

    @Override
    public boolean readOnly() throws IOException {
        if(super.readOnly()){
            this.mappedByteBuffer.force();
            this.mappedByteBuffer = null;

            return true;
        }

        return false;
    }

    @Override
    public void sync() {
        if(!isReadOnly()) this.mappedByteBuffer.force();
    }

    @Override
    public boolean close() throws IOException {
        if(this.closed.compareAndSet(false, true)){
            if(this.mappedByteBuffer != null) {
                this.mappedByteBuffer.force();
                this.mappedByteBuffer = null;
            }
            if(this.fileChannel.isOpen()) this.fileChannel.close();

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
            long fileSize = this.fileChannel.size();

            while(readPos < fileSize){
                this.fileChannel.read(headerByteBuf, readPos);
                headerByteBuf.flip();

                long crc = headerByteBuf.getLong();
                long timestamp = headerByteBuf.getLong();
                int keySize = headerByteBuf.getInt();
                int valueSize = headerByteBuf.getInt();

                int entrySize = Entry.HEADER_SIZE + keySize + valueSize;

                if(valueSize > 0){
                    ByteBuffer keyBuffer = ByteBuffer.allocate(keySize);
                    this.fileChannel.read(keyBuffer, readPos + headerByteBuf.capacity());

                    // type cannot be 0x00. in mmap-datafile which mean when type reading is 0x00, type position is end of file
                    if(keyBuffer.array()[0] == 0x00) break;

                    Object keyObject = TypeConvert.serialize(keyBuffer.array());

                    keydir.updateKey(
                            keyObject,
                            this.getFileId(),
                            entrySize,
                            readPos,
                            timestamp
                    );

                }

                readPos += entrySize;

            }

        } catch (IOException e) {
            return keydir;
        }


        return keydir;
    }

}
