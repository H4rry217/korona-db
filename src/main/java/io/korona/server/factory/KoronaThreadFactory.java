package io.korona.server.factory;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @program: korona-db
 * @description:
 * @author: H4rry217
 **/

public class KoronaThreadFactory implements ThreadFactory {

    private final AtomicInteger threadCount = new AtomicInteger(0);

    @Override
    public Thread newThread(Runnable r) {
        Thread thread = new Thread(r);
        thread.setDaemon(true);
        thread.setName("Korona thread #" + this.threadCount.incrementAndGet());
        return thread;
    }

}
