package com.appmetr.hercules.batch.extractor;

import java.util.ArrayList;
import java.util.List;

/**
 * This class designed for iterate through getting items by batches
 *
 * IMPORTANT
 * This class not designed for specific cases (e.g. when batch may contain less then batchSize items,
 * but data store has more elements, see DAOBatchIterator as an example)
 *
 * @param <E> entity for iterate
 * @param <K> entity key for iterate
 */
public abstract class RangeBatchIterator<E, K> extends AbstractBatchIterator<E, K> {
    protected K lastKey;

    private boolean hasNext = false;

    //Override this method in child classes
    protected abstract List<E> getRange(K from, K to, int batchSize);
    protected abstract K getKey(E item);

    public RangeBatchIterator(K from, K to) {
        super(from, to);

        lastKey = from;
    }

    public RangeBatchIterator(K from, K to, int batchSize) {
        super(from, to, batchSize);

        lastKey = from;
    }

    @Override public List<E> next() {
        List<E> batch = getRange(from, to, batchSize + 1);

        List<E> result = new ArrayList<E>();

        if (batch.size() == 0) {
            hasNext = false;
        } else if (batch.size() < batchSize + 1) {
            result = batch;
            hasNext = false;
        } else {
            lastKey = getKey(batch.get(batch.size() - 1));
            result = batch.subList(0, batch.size() - 1);

            hasNext = true;
        }

        return result;
    }

    @Override public boolean hasNext() {
        return hasNext;
    }
}
