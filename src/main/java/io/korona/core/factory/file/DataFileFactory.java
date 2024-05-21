package io.korona.core.factory.file;

import io.korona.core.db.DataFile;

import java.util.List;

/**
 * @program: korona-db
 * @description:
 * @author: H4rry217
 **/

public interface DataFileFactory {

    public DataFile createDataFile(String fileName);

    public List<DataFile> loadDataFile();

    public FileMetadataFactory getFileMetadataFactory();

}
