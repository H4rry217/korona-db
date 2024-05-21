package io.korona.core.factory.file;

import io.korona.core.metadata.FileMetadata;

import java.nio.ByteBuffer;

public interface FileMetadataFactory {

    public FileMetadata buildMetadata(ByteBuffer byteBuffer);

    public FileMetadata buildMetadata(long fileId, boolean readOnly);

}
