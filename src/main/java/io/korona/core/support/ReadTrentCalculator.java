package io.korona.core.support;

import java.util.ArrayDeque;
import java.util.Queue;

/**
 * @program: korona-db
 * @description:
 * @author: H4rry217
 **/

public final class ReadTrentCalculator {

    private final int limit;

    private final Queue<Byte> leftWindow;

    private final Queue<Byte> middleWindow;

    private final Queue<Byte> rightWindow;

    private final long _rightSec;

    private final long _middleSec;

    private final long _leftSec;

    private final byte _v = 0x01;

    private final int _thresholdPercent;

    public ReadTrentCalculator(int second, int limit, int thresholdPercent){
        long millisecond = second * 1000L;
        this.limit = limit;
        this._thresholdPercent = thresholdPercent;

        this.leftWindow = new ArrayDeque<>();
        this.middleWindow = new ArrayDeque<>();
        this.rightWindow = new ArrayDeque<>();

        this._rightSec = millisecond;
        this._middleSec = millisecond * 2L;
        this._leftSec = millisecond * 3L;
    }

    public int getWindowCount(){
        return this.leftWindow.size();
    }

    public boolean readCount(){
        long currentTime = System.currentTimeMillis();

        return this.rightOffer(this._v, currentTime);
    }

    private boolean rightOffer(Byte val, long curTime){
        long t = curTime - this._rightSec;
        while(!this.rightWindow.isEmpty() && this.rightWindow.peek() < t){
            this.middleOffer(this.rightWindow.poll(), curTime);
        }

        if(this.rightWindow.size() < this.limit){
            this.rightWindow.offer(val);
            return true;
        }

        return false;
    }

    private void middleOffer(Byte val, long curTime){
        long t = curTime - this._middleSec;
        while(!this.middleWindow.isEmpty() && this.middleWindow.peek() < t){
            this.leftOffer(this.middleWindow.poll(), curTime);
        }

        if(this.middleWindow.size() < this.limit){
            this.middleWindow.offer(val);
        }

    }

    private void leftOffer(Byte val, long curTime){
        long t = curTime - this._leftSec;
        while(!this.leftWindow.isEmpty() && this.leftWindow.peek() < t){
            this.leftWindow.poll();
        }

        if(this.leftWindow.size() < this.limit){
            this.leftWindow.offer(val);
        }
    }

    public boolean isSmooth() {
        int left = this.leftWindow.size() + 1;
        double middle = this.middleWindow.size() + 1;
        int right = this.rightWindow.size() + 1;

        double prevPercent = ((middle - left) / left) * 100;

        if(Math.abs(prevPercent) < this._thresholdPercent) {
            double curPercent = ((right - middle) / middle) * 100;
            var aa = Math.abs(curPercent);
            return Math.abs(curPercent) < this._thresholdPercent;
        } else return this._thresholdPercent == 0;
    }

}
