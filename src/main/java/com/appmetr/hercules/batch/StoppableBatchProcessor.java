package com.appmetr.hercules.batch;

import java.util.List;

public interface StoppableBatchProcessor<E> {
    /**
     * Process batch
     * @param batch
     * @return true to continue iteration, false to stop iteration
     */
    boolean processBatch(List<E> batch);
}
