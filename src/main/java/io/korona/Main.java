package io.korona;

import io.korona.core.metadata.options.KoronaOption;
import io.korona.server.KoronaDatabase;

import java.io.IOException;
import java.nio.file.Path;

/**
 * @program: korona-db
 * @description:
 * @author: H4rry217
 **/

public class Main {

    public static void main(String[] args) throws IOException {
        KoronaOption option = KoronaDatabase.findOption(Path.of(System.getProperty("user.dir")).resolve("korona.yml").toString());
        KoronaDatabase database = new KoronaDatabase(option);

        database.cli();
    }

}
