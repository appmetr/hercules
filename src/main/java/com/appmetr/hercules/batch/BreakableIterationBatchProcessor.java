package com.appmetr.hercules.batch;

import java.util.List;

public interface BreakableIterationBatchProcessor<E> {
    /**
     * Process batch with ability to break iterator loop.
     * @param batch
     * @return true to continue iteration, false to break iteration loop
     */
    boolean processBatch(List<E> batch);
}
