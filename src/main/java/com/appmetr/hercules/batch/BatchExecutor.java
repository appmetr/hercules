package com.appmetr.hercules.batch;

import com.appmetr.hercules.profile.DataOperationsProfile;

import java.util.List;

public class BatchExecutor<E, K> {
    private BatchIterator<E, K> iterator;
    private BatchProcessor<E> processor;

    public BatchExecutor(BatchIterator<E, K> iterator, BatchProcessor<E> processor) {
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

            processor.processBatch(batch);
        }

        return counter;
    }
}
