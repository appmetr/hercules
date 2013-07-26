package com.appmetr.hercules.batch;

import java.util.List;

public class BatchExecutor<E, K> {
    private BatchIterator<E, K> extractor;
    private BatchProcessor<E> processor;

    public BatchExecutor(BatchIterator<E, K> extractor, BatchProcessor<E> processor) {
        this.extractor = extractor;
        this.processor = processor;
    }

    public int execute() {
        int counter = 0;

        do {
            List<E> batch = extractor.next();
            counter += batch.size();

            processor.processBatch(batch);
        } while (extractor.hasNext());

        return counter;
    }
}
