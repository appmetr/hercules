package com.appmetr.hercules.failover;

public class FailoverConf {

    public static FailoverConf NO = new FailoverConf(1, 0, 0, 0);
    public static FailoverConf MIN = new FailoverConf(12, 5000, 5000, 1);
    public static FailoverConf TEN_MIN = new FailoverConf(10 * 12, 5000, 5000, 1);
    public static FailoverConf HOUR = new FailoverConf(360, 10000, 10000, 1);
    public static FailoverConf INF32 = new FailoverConf(-1, 1000, 32000, 2);

    private int maxRetries;
    private int startSleepBetweenRetriesMs;
    private int maxSleepBetweenRetriesMs;
    private double sleepBetweenRetriesIncreaseRatio;

    public FailoverConf(int maxRetries, int startSleepBetweenRetriesMs, int maxSleepBetweenRetriesMs, double sleepBetweenRetriesIncreaseRatio) {
        this.maxRetries = maxRetries;
        this.startSleepBetweenRetriesMs = startSleepBetweenRetriesMs;
        this.maxSleepBetweenRetriesMs = maxSleepBetweenRetriesMs;
        this.sleepBetweenRetriesIncreaseRatio = sleepBetweenRetriesIncreaseRatio;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public int getStartSleepBetweenRetriesMs() {
        return startSleepBetweenRetriesMs;
    }

    public int getMaxSleepBetweenRetriesMs() {
        return maxSleepBetweenRetriesMs;
    }

    public double getSleepBetweenRetriesIncreaseRatio() {
        return sleepBetweenRetriesIncreaseRatio;
    }
}
