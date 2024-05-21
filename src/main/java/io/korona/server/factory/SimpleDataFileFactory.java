package io.korona.server.factory;

import io.korona.core.AppendDataFile;
import io.korona.core.MemMapperDataFile;
import io.korona.core.db.DataFile;
import io.korona.core.factory.file.DataFileFactory;
import io.korona.core.factory.file.FileMetadataFactory;
import io.korona.core.metadata.options.KoronaOption;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * @program: korona-db
 * @description:
 * @author: H4rry217
 **/

public class SimpleDataFileFactory implements DataFileFactory {

    private final Path workPath;

    private final FileMetadataFactory fileMetadataFactory;

    private final Class<? extends DataFile> dataFileClazz;

    private KoronaOption option;

    public SimpleDataFileFactory(Path workPath, FileMetadataFactory metadataFactory, Class<? extends DataFile> clazz, KoronaOption option){
        this.workPath = workPath;
        this.fileMetadataFactory = metadataFactory;
        this.dataFileClazz = clazz;
        this.option = option;

        File dataDir = new File(this.workPath.toString() + "\\data");
        if(!dataDir.exists() || !dataDir.isDirectory()) dataDir.mkdirs();
    }

    @Override
    public DataFile createDataFile(String fileName) {
        DataFile dataFile = null;
        if(this.dataFileClazz == AppendDataFile.class){
            dataFile = new AppendDataFile(this.workPath.toString() + "\\data", fileName, this.fileMetadataFactory);
        }else if(this.dataFileClazz == MemMapperDataFile.class){
            dataFile = new MemMapperDataFile(this.workPath.toString() + "\\data", fileName, this.fileMetadataFactory, this.option.maxFileSize * 1024 * 1024);
        }

        return dataFile;
    }

    @Override
    public List<DataFile> loadDataFile() {
        File dir = new File(this.workPath.toString() + "\\data");

        List<DataFile> fileList = new ArrayList<>();
        for (File file : dir.listFiles()) {
            if(!file.isDirectory() && file.getName().endsWith(".db")){
                DataFile dataFile = null;
                if(this.dataFileClazz == AppendDataFile.class){
                    dataFile = new AppendDataFile(file, this.fileMetadataFactory);
                } else if(this.dataFileClazz == MemMapperDataFile.class) dataFile = new MemMapperDataFile(file, this.fileMetadataFactory, this.option.maxFileSize * 1024 * 1024);

                fileList.add(dataFile);
            }
        }

        return fileList;
    }

    @Override
    public FileMetadataFactory getFileMetadataFactory() {
        return null;
    }
}
