package io.korona.server;

import io.korona.core.AppendDataFile;
import io.korona.core.MemMapperDataFile;
import io.korona.core.RWEngine;
import io.korona.core.data.serialize.TypeConvert;
import io.korona.core.db.BytesKey;
import io.korona.core.db.Database;
import io.korona.core.db.index.Key;
import io.korona.core.metadata.options.KoronaOption;
import io.korona.server.cli.CommandLineConsole;
import io.korona.server.engine.bucket.BucketEngine;
import io.korona.server.engine.enigma.EnigmaEngine;
import io.korona.server.factory.SimpleDataEntryFactory;
import io.korona.server.factory.SimpleFileMetadataFactory;
import io.korona.server.factory.SimpleDataFileFactory;
import io.korona.server.info.RunningStatus;
import io.korona.server.operation.CommandSentence;
import io.korona.server.operation.Operate;
import io.korona.server.operation.OperateExecutor;
import io.korona.utils.CommonUtils;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;
import org.yaml.snakeyaml.introspector.Property;
import org.yaml.snakeyaml.introspector.PropertyUtils;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Map;

/**
 * @program: korona-db
 * @description:
 * @author: H4rry217
 **/

public final class KoronaDatabase implements Database {

    private final RWEngine engine;

    public final Path workPath;

    private static Database instance;

    private final RunningStatus runningStatus = new RunningStatus();

    public KoronaDatabase(KoronaOption databaseOptions){
        this.workPath = Path.of(
                (databaseOptions.workPath == null || databaseOptions.workPath.isEmpty())?
                        System.getProperty("user.dir"):
                        databaseOptions.workPath
        );

        RWEngine chooseEngine = null;
        if("bucket".equals(databaseOptions.engine)){
            chooseEngine = new BucketEngine(
                    this.workPath,
                    new SimpleDataFileFactory(this.workPath, new SimpleFileMetadataFactory(), AppendDataFile.class, databaseOptions),
                    new SimpleDataEntryFactory(),
                    databaseOptions
            );
        }else if("enigma".equals(databaseOptions.engine)){
            chooseEngine = new EnigmaEngine(
                    this.workPath,
                    new SimpleDataFileFactory(this.workPath, new SimpleFileMetadataFactory(), MemMapperDataFile.class, databaseOptions),
                    new SimpleDataEntryFactory(),
                    databaseOptions
            );
        }

        this.engine = chooseEngine;

        KoronaDatabase.instance = this;
    }

    public static Database getInstance() {
        return instance;
    }

    @Override
    public void put(Object key, Object value) {
        var data = new SimpleData(key, value);
        try {
            this.engine.write(data);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Object get(Object key) {
        var entry = this.engine.read(new BytesKey(TypeConvert.deserialize(key)));
        if(entry != null && !entry.isTombStone()){
            return TypeConvert.serialize(entry.value());
        }

        return null;
    }

    @Override
    public void delete(Object key){
        try {
            this.engine.delete(new BytesKey(TypeConvert.deserialize(key)));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void cli() throws IOException {
        var consoleCli = new CommandLineConsole();
        var executor = new OperateExecutor(this);

        while(true){
            CommandSentence sentence = consoleCli.inputRead();
            try{
                if(sentence.getOperate() == null){
                    System.out.println("Illegal operation!");
                }else if(sentence.getOperate() == Operate.EXIT){
                    this.engine.close();
                    System.out.println("Bye!");
                    return;
                }else{
                    executor.execute(sentence);
                }
            }catch (Exception ignore){

            }

        }
    }

    public static KoronaOption findOption(String confPath){
        KoronaOption koronaOption = null;
        Constructor c = new Constructor(KoronaOption.class, new LoaderOptions());
        c.setPropertyUtils(new PropertyUtils() {
            @Override
            public Property getProperty(Class<? extends Object> type, String name) {
                if ( name.indexOf('-') > -1 ) {
                    name = CommonUtils.toCamelCase(name);
                }
                setSkipMissingProperties(true);
                return super.getProperty(type, name);
            }
        });

        Yaml yaml = new Yaml(c);

        try(InputStream inputStream = findConfResource(confPath)){
            String yamlContent = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);

            koronaOption = yaml.loadAs(yamlContent, KoronaOption.class);
            koronaOption.options  = yaml.loadAs(yamlContent, Map.class);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return koronaOption;
    }

    public static InputStream findConfResource(String confPath) throws FileNotFoundException {
        File f = new File(confPath);
        InputStream inputStream = null;

        if(f.exists() && !f.isDirectory()){
            inputStream = new FileInputStream(f);
            System.out.println("use options from "+confPath);
        }else{
            inputStream = KoronaDatabase.class.getClassLoader().getResourceAsStream("korona.yml");
            System.out.println("use options from classpath resources");
        }

        return inputStream;
    }

    public int keyCount(){
        return this.engine._getKeydir().size();
    }

    public Iterator<Key> keys(){
        return this.engine._getKeydir().getKeys();
    }

    public RunningStatus getRunningStatus(){
        RunningStatus copyRunningStatus = this.runningStatus.clone();
        copyRunningStatus.compactRemaining = this.engine.getCompactionTimeRemaining();
        copyRunningStatus.dataSyncRemaining = this.engine.getDataSyncTimeRemaining();
        copyRunningStatus.keydirDumpRemaining = this.engine.getKeydirDumpTimeRemaining();
        copyRunningStatus.bufferMaxSize = (long) (this.engine instanceof BucketEngine? ((BucketEngine) this.engine)._getWriteBuffer().capacity(): 0);
        copyRunningStatus.bufferRemaining = (long) (this.engine instanceof BucketEngine? ((BucketEngine) this.engine)._getWriteBuffer().remaining(): 0);

        return copyRunningStatus;
    }

}
