package com.appmetr.hercules.failover;

import me.prettyprint.hector.api.exceptions.HTimedOutException;
import me.prettyprint.hector.api.exceptions.HUnavailableException;
import me.prettyprint.hector.api.exceptions.HectorException;
import org.slf4j.Logger;

public class FailoverQueryProcessor {

    public static <T> T process(FailoverConf conf, Logger logger, FailoverQuery<T> query) throws HectorException {

        if (conf.getMaxRetries() == 0) {
            throw new IllegalArgumentException("Invalid retries count: " + conf.getMaxRetries());
        }

        int retryCount = 0;
        int currSleepBetweenRetries = Math.min(conf.getStartSleepBetweenRetriesMs(), conf.getMaxSleepBetweenRetriesMs());
        while (true) {

            HectorException exception;
            try {
                return query.query();
            } catch (HTimedOutException e) {
                exception = e;
            } catch (HUnavailableException e) {
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

    public static void process(FailoverConf conf, Logger logger, final VoidFailoverQuery query) throws HectorException {
        process(conf, logger, new FailoverQuery<Void>() {
            @Override public Void query() {
                query.query();

                return null;
            }
        });
    }
}
