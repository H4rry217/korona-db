package io.korona.core.db;

import io.korona.core.data.DataAppender;
import io.korona.core.db.index.Keydir;
import io.korona.core.factory.file.FileMetadataFactory;
import io.korona.core.metadata.FileMetadata;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @program: korona-db
 * @description:
 * @author: H4rry217
 **/

public abstract class DataFile implements DataAppender {

    protected FileMetadata metadata;

    private final AtomicBoolean readOnly;

    protected final AtomicBoolean closed = new AtomicBoolean(false);

    protected final AtomicLong writeOffset = new AtomicLong(-1);

    protected final File file;

    protected final FileMetadataFactory fileMetadataFactory;

    public DataFile(String path, FileMetadataFactory factory){
        this.file = new File(path);
        this.fileMetadataFactory = factory;

        if(!this.file.exists()) initialize();

        try (InputStream inputStream = new BufferedInputStream(new FileInputStream(this.file))){

            byte[] metadata = new byte[FileMetadata.METADATA_SIZE];
            int read = inputStream.read(metadata);
            if(read != FileMetadata.METADATA_SIZE) throw new RuntimeException("read error size on metadata file");

            this.metadata = this.fileMetadataFactory.buildMetadata(ByteBuffer.wrap(metadata));
            this.readOnly = new AtomicBoolean(this.metadata.isReadOnly());

        }  catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected void initialize(){
        try{
            if(this.file.createNewFile()){
                OutputStream outputStream = new FileOutputStream(this.file);
                FileMetadata metaData = this.fileMetadataFactory.buildMetadata(System.currentTimeMillis(), false);

                outputStream.write(metaData.toByteBuffer().array());
                outputStream.close();

            }

        }catch(IOException ignored){

        }
    }

    protected abstract void randomWrite(ByteBuffer byteBuffer, long position) throws IOException;

    protected void writeMeteData(FileMetadata fileMetadata) throws IOException{
        this.randomWrite(fileMetadata.toByteBuffer(), 0);
    }

    public long getFileId(){
        return this.metadata.getFileId();
    }

    public long getPosition() {
        return this.writeOffset.get();
    }

    public FileMetadata getMetadata(){
        return this.metadata;
    }

    @Override
    public boolean isReadOnly(){
        return this.readOnly.get();
    }

    public boolean isClosed(){
        return this.closed.get();
    }

    public boolean readOnly() throws IOException {
        if(this.readOnly.compareAndSet(false, true)){
            this.writeOffset.set(-1);
            this.metadata.readOnly = true;

            this.writeMeteData(this.metadata);

            return true;
        }

        return false;
    }

    public Path getPath(){
        return this.file.toPath();
    }

    public abstract void sync() throws IOException;

    public abstract boolean close() throws IOException;

    public abstract Keydir toKeydir();

}
