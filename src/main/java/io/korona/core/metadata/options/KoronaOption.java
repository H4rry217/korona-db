package io.korona.core.metadata.options;

import java.util.Map;

/**
 * @program: korona-db
 * @description:
 * @author: H4rry217
 **/

public class KoronaOption {

    public String engine;

    public int maxFileSize;

    public final int maxSubDataSize = 1024 * 1024 * 64;

    public String workPath;

    public int dataSync;

    public CompactOption compact;

    public KeydirOption keydir;

    public Map<String, Object> options;

}
