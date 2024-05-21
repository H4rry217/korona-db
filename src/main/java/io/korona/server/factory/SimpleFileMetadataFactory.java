package io.korona.server.factory;

import io.korona.core.factory.file.FileMetadataFactory;
import io.korona.core.metadata.DataFileMetadata;

import java.nio.ByteBuffer;

/**
 * @program: korona-db
 * @description:
 * @author: H4rry217
 **/

public class SimpleFileMetadataFactory implements FileMetadataFactory {

    @Override
    public DataFileMetadata buildMetadata(ByteBuffer byteBuffer) {
        return new DataFileMetadata(byteBuffer);
    }

    @Override
    public DataFileMetadata buildMetadata(long fileId, boolean readOnly) {
        return new DataFileMetadata(fileId, readOnly);
    }

}
