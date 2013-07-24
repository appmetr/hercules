package com.appmetr.hercules.batch;

import java.util.List;

public class BatchExecutor<E, K> {
    public static final int DEFAULT_BATCH_SIZE = 1000;

    private BatchExtractor<E, K> extractor;
    private BatchProcessor<E> processor;

    private K from;
    private K to;

    private int batchSize = DEFAULT_BATCH_SIZE;


    public BatchExecutor(BatchExtractor<E, K> extractor, BatchProcessor<E> processor, K from, K to) {
        this.extractor = extractor;
        this.processor = processor;

        this.from = from;
        this.to = to;
    }

    public BatchExecutor(BatchExtractor<E, K> extractor, BatchProcessor<E> processor, int batchSize) {
        this(extractor, processor, null, null, batchSize);
    }

    public BatchExecutor(BatchExtractor<E, K> extractor, BatchProcessor<E> processor, K from, K to, int batchSize) {
        this(extractor, processor, from, to);

        this.batchSize = batchSize;
    }

    public int execute() {
        int counter = 0;

        K lastKey = from;
        while (true) {

            List<E> batch = extractor.getBatch(lastKey, to, batchSize + 1);

            if (batch.size() == 0) {
                break;
            } else if (batch.size() < batchSize + 1) {
                counter += batch.size();
                processor.processBatch(batch);

                break;
            } else {
                counter += batchSize;
                lastKey = extractor.getKey(batch.get(batch.size() - 1));
                processor.processBatch(batch.subList(0, batch.size() - 1));
            }
        }

        return counter;
    }
}
