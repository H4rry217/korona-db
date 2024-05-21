package io.korona.benchmark;

/**
 * @program: korona-db
 * @description:
 * @author: H4rry217
 **/

public final class Stopwatch {
    private long startTime;
    private long endTime;
    private boolean running;

    public Stopwatch() {
        this.startTime = 0;
        this.endTime = 0;
        this.running = false;
    }

    public void start() {
        if(this.running) throw new IllegalStateException("Stopwatch is already running");
        this.startTime = System.currentTimeMillis();
        this.running = true;
    }

    public void stop() {
        if(!this.running) throw new IllegalStateException("Stopwatch is not running");
        this.endTime = System.currentTimeMillis();
        this.running = false;
    }

    public void reset() {
        this.startTime = 0;
        this.endTime = 0;
        this.running = false;
    }

    public long getElapsedTime() {
        return this.running? System.currentTimeMillis() - this.startTime: endTime - this.startTime;
    }

    public String getElapsedTimeInSeconds() {
        long elapsedTimeMillis = getElapsedTime();
        long seconds = elapsedTimeMillis / 1000;
        long millis = elapsedTimeMillis % 1000;

        return String.format("%d.%03d seconds", seconds, millis);
    }
}