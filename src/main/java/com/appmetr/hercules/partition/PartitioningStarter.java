package com.appmetr.hercules.partition;

import com.appmetr.hercules.Hercules;
import com.appmetr.hercules.mutations.ExecutableMutation;
import com.appmetr.hercules.mutations.MutationsQueue;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.MessageFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@Singleton
public class PartitioningStarter implements Runnable {
    private Logger logger = LoggerFactory.getLogger(PartitioningStarter.class);

    private MutationsQueue mutationsQueue;
    private Hercules hercules;

    private volatile Thread pollingThread = null;

    private final Lock lock = new ReentrantLock();
    private final Condition trigger = lock.newCondition();

    private static final long RUN_PERIOD = 1000 * 60 * 60;

    @Inject
    public PartitioningStarter(MutationsQueue mutationsQueue, Hercules hercules) {
        this.mutationsQueue = mutationsQueue;
        this.hercules = hercules;
    }

    @Override public void run() {
        pollingThread = Thread.currentThread();

        while (!pollingThread.isInterrupted()) {
            lock.lock();

            try {
                addPartitions();
                trigger.await(RUN_PERIOD, TimeUnit.MILLISECONDS);
            } catch (InterruptedException ie) {
                pollingThread.interrupt();
            } finally {
                lock.unlock();
            }
        }
    }

    public void trigger() {
        lock.lock();

        try {
            trigger.signal();
        } finally {
            lock.unlock();
        }
    }

    private void addPartitions() {
        logger.info(MessageFormat.format("PartitioningJob started at {0}", new Date()));

        try {
            List<ExecutableMutation> mutations = new LinkedList<ExecutableMutation>();

            mutations.addAll(hercules.getPartitionMutations());

            for (ExecutableMutation mutation : mutations) {
                mutation.setPriority(MutationsQueue.DEFAULT_PRIORITY);
            }
            mutationsQueue.addAll(mutations);

        } catch (Exception e) {
            logger.error("Exception occurred while creating partitions", e);
        }

        logger.info("PartitioningJob finished");
    }

    public void stop() {
        logger.info("EventIncomingsFlusher stopped!");

        if (pollingThread != null) {
            pollingThread.interrupt();
        }
    }

}
