package com.appmetr.hercules.failover;

import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import org.slf4j.Logger;

public class FailoverQueryProcessor {

    public static <T> T process(FailoverConf conf, Logger logger, FailoverQuery<T> query) throws DriverException {

        if (conf.getMaxRetries() == 0) {
            throw new IllegalArgumentException("Invalid retries count: " + conf.getMaxRetries());
        }

        int retryCount = 0;
        int currSleepBetweenRetries = Math.min(conf.getStartSleepBetweenRetriesMs(), conf.getMaxSleepBetweenRetriesMs());
        while (true) {

            DriverException exception;
            try {
                return query.query();
            } catch (NoHostAvailableException e) {
                exception = e;
            }

            retryCount++;
            if (conf.getMaxRetries() > 0 && retryCount >= conf.getMaxRetries()) {
                throw exception;
            }

            if (logger != null) {
                logger.error(String.format("Failovering: retries count - %1$s, sleep between - %2$s\n", retryCount, currSleepBetweenRetries), exception);
            }
            try {
                Thread.sleep(currSleepBetweenRetries);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            currSleepBetweenRetries = Math.min((int) (currSleepBetweenRetries * conf.getSleepBetweenRetriesIncreaseRatio() + 0.5), conf.getMaxSleepBetweenRetriesMs());
        }
    }

    public static void process(FailoverConf conf, Logger logger, final VoidFailoverQuery query) throws DriverException {
        process(conf, logger, (FailoverQuery<Void>) () -> {
            query.query();
            return null;
        });
    }
}
