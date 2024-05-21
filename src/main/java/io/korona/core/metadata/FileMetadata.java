package io.korona.core.metadata;

import java.nio.ByteBuffer;

/**
 * @program: korona-db
 * @description:
 * @author: H4rry217
 **/

public abstract class FileMetadata implements Metadata{

    public long fileId;

    public boolean readOnly;

    public long fileSize = METADATA_SIZE;

    public static final int METADATA_SIZE = 4096;

    public long getFileId() {
        return this.fileId;
    }

    public boolean isReadOnly(){
        return this.readOnly;
    }

    public int getMetadataLength(){
        return METADATA_SIZE;
    }

    public long getFileSize(){
        return this.fileSize;
    }

    public abstract ByteBuffer toByteBuffer();

    public abstract void readMetadata(ByteBuffer byteBuffer);

}
