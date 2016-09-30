package com.appmetr.hercules.batch;

import com.appmetr.hercules.profile.DataOperationsProfile;

import java.util.List;

public class BreakableIterationBatchExecutor<E, K> {
    private BatchIterator<E, K> iterator;
    private BreakableIterationBatchProcessor<E> processor;

    public BreakableIterationBatchExecutor(BatchIterator<E, K> iterator, BreakableIterationBatchProcessor<E> processor) {
        this.iterator = iterator;
        this.processor = processor;
    }

    public int execute() {
        return execute(null);
    }

    public int execute(DataOperationsProfile dataOperationsProfile) {
        int counter = 0;

        while (iterator.hasNext()) {
            List<E> batch = iterator.next(dataOperationsProfile);
            counter += batch.size();

            if (!processor.processBatch(batch)) break;
        }

        return counter;
    }
}
