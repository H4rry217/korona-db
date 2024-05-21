package io.korona.server.info;

/**
 * @program: korona-db
 * @description:
 * @author: H4rry217
 **/

public class RunningStatus implements Cloneable{

    public long startTime = System.currentTimeMillis();

    public Long compactRemaining;

    public Long dataSyncRemaining;

    public Long keydirDumpRemaining;

    public Long bufferMaxSize;

    public Long bufferRemaining;

    @Override
    public RunningStatus clone() {
        try {
            RunningStatus clone = (RunningStatus) super.clone();
            return clone;
        } catch (CloneNotSupportedException e) {
            throw new AssertionError();
        }
    }

}
