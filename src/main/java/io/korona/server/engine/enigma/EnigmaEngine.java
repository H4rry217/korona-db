package io.korona.server.engine.enigma;

import io.korona.core.RWEngine;
import io.korona.core.factory.EntryFactory;
import io.korona.core.factory.file.DataFileFactory;
import io.korona.core.metadata.options.KoronaOption;

import java.nio.file.Path;

/**
 * @program: korona-db
 * @description:
 * @author: H4rry217
 **/

public class EnigmaEngine extends RWEngine {

    public EnigmaEngine(Path workPath, DataFileFactory dataFileFactory, EntryFactory entryFactory, KoronaOption option) {
        super(workPath, dataFileFactory, entryFactory, option);
    }

}
