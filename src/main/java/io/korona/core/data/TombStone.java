package io.korona.core.data;

import io.korona.core.db.index.Key;

/**
 * @program: korona-db
 * @description:
 * @author: H4rry217
 **/

public class TombStone extends DataEntry{

    private final static byte[] EMPTY_DATA = new byte[0];

    public static TombStone tombStone(Key key){
        TombStone tombStone = new TombStone();
        tombStone.timestamp = System.currentTimeMillis();
        tombStone.keyData = key.bytes();
        tombStone.valueData = EMPTY_DATA;

        return tombStone;
    }

}
