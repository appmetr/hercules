package com.appmetr.hercules.mutations;

import com.appmetr.hercules.Hercules;
import com.appmetr.hercules.HerculesMonitoringGroup;
import com.appmetr.monblank.MonblankConst;
import com.appmetr.monblank.Monitoring;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;
import java.util.concurrent.PriorityBlockingQueue;

@Singleton
public class MutationsQueue implements Runnable {

    private static final int SLEEP_CREATE_MS = 3000;
    private static final int STOP_SLEEP_BEFORE_INTERRUPT_MS = 3000;

    public static final int DEFAULT_PRIORITY = 1;
    public static final int HIGH_PRIORITY = 5;
    public static final int MAX_RETRIES = 15;

    @Inject protected Hercules hercules;
    @Inject protected Monitoring monitoringService;

    private Logger logger = LoggerFactory.getLogger(MutationsQueue.class);

    private PriorityBlockingQueue<ExecutableMutation> queue = new PriorityBlockingQueue<ExecutableMutation>();
    private volatile Thread pollingThread = null;

    @Override
    public void run() {
        try {
            pollingThread = Thread.currentThread();
            logger.info("MutationsQueue polling started!");

            Set<String> columnFamilyNames = hercules.getColumnFamilies();
            while (!pollingThread.isInterrupted()) {
                try {
                    ExecutableMutation mutation = queue.take();

                    monitoringService.inc(HerculesMonitoringGroup.EM_PARTITION, "MutationQueue mutation taken");
                    if (mutation.getType() == ExecutableMutation.MutationType.CREATE) {

                        if (columnFamilyNames.contains(mutation.getCfName())) {
                            mutation.skipped();
                        } else {
                            if (executeMutationSafe(mutation)) {
                                columnFamilyNames.add(mutation.getCfName());
                            }
                        }
                    } else if (mutation.getType() == ExecutableMutation.MutationType.DELETE) {

                        if (columnFamilyNames.contains(mutation.getCfName())) {
                            if (executeMutationSafe(mutation)) {
                                columnFamilyNames.remove(mutation.getCfName());
                            }
                        } else {
                            mutation.skipped();
                        }
                    } else {
                        logger.error("Unsupported mutation type: " + mutation);
                    }
                } catch (InterruptedException ie) {
                    logger.info("Interrupted while polling the queue.");

                    pollingThread.interrupt();
                }
            }
            logger.info("MutationsQueue polling stopped!");
        } catch (Exception e) {
            logger.error("Exception in MutationsQueue: ", e);
        }
    }

    private boolean executeMutationSafe(ExecutableMutation mutation) {
        try {
            mutation.execute();
            return true;
        } catch (Exception e) {
            logger.error("Exception while executing mutation: " + mutation, e);
            logger.info("Scheduling mutation with increased priority: " + mutation);
            if (mutation.getRetryCount() < MAX_RETRIES) {
                add(mutation.retry());
            }
            return false;
        } finally {
            try {
                Thread.sleep(SLEEP_CREATE_MS);
            } catch (InterruptedException e) {
                logger.info("Interrupted while sleeping in mutation create.");
            }
        }
    }

    public void add(ExecutableMutation mutation) {
        monitoringService.inc(HerculesMonitoringGroup.EM_PARTITION, "MutationQueue mutations queued");
        queue.add(mutation);
    }

    public void addAll(List<ExecutableMutation> mutations) {
        monitoringService.add(HerculesMonitoringGroup.EM_PARTITION, "MutationQueue mutations queued", MonblankConst.COUNT, mutations.size());
        queue.addAll(mutations);
    }

    public void stop() {
        if (pollingThread != null) {
            pollingThread.interrupt();
        }
    }
}
