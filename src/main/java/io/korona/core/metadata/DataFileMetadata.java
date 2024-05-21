package io.korona.core.metadata;

import io.korona.utils.BinaryUtils;

import java.nio.ByteBuffer;

/**
 * @program: korona-db
 * @description:
 * @author: H4rry217
 **/

public class DataFileMetadata extends FileMetadata {

    public DataFileMetadata(ByteBuffer byteBuffer){
        this.readMetadata(byteBuffer);
    }

    public DataFileMetadata(long fileId, boolean readOnly){
        this.fileId = fileId;
        this.readOnly = readOnly;
    }

    @Override
    public ByteBuffer toByteBuffer() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(this.getMetadataLength());

        /*FileId 8b*/
        byteBuffer.put(BinaryUtils.longToBytes(this.fileId));

        byteBuffer.put((byte) (this.readOnly? 0x01: 0x00));

        byteBuffer.put(BinaryUtils.intToBytes(this.getMetadataLength()));

        return byteBuffer;
    }

    @Override
    public void readMetadata(ByteBuffer byteBuffer) {
        this.fileId = byteBuffer.getLong();
        this.readOnly = (byteBuffer.get() != 0x00);
        this.fileSize = byteBuffer.getLong();
    }

}
