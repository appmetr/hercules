package com.appmetr.hercules.batch.iterator;

import com.appmetr.hercules.profile.DataOperationsProfile;

import java.util.ArrayList;
import java.util.List;

/**
 * This class designed for iterate through getting items by batches
 *
 * IMPORTANT
 * This class not designed for specific cases (e.g. when batch may contain less then batchSize items,
 * but data store has more elements, see TupleBatchIterator as an example)
 *
 * @param <E> entity for iterate
 * @param <K> entity key for iterate
 */
public abstract class RangeBatchIterator<E, K> extends AbstractBatchIterator<E, K> {
    protected K currentLowKey;
    protected K currentHighKey;

    //Override this method in child classes
    protected abstract List<E> getRange(K lowEnd, K highEnd, boolean reverse, int batchSize, DataOperationsProfile dataOperationsProfile);

    protected abstract K getKey(E item);

    public RangeBatchIterator() {
        super();
        currentLowKey = lowEnd;
        currentHighKey = highEnd;
    }

    public RangeBatchIterator(int batchSize) {
        super(batchSize);
        currentLowKey = lowEnd;
        currentHighKey = highEnd;
    }

    public RangeBatchIterator(K lowEnd, K highEnd) {
        super(lowEnd, highEnd);
        currentLowKey = lowEnd;
        currentHighKey = highEnd;
    }

    public RangeBatchIterator(K lowEnd, K highEnd, int batchSize) {
        super(lowEnd, highEnd, batchSize);
        currentLowKey = lowEnd;
        currentHighKey = highEnd;
    }

    protected RangeBatchIterator(K lowEnd, K highEnd, boolean reverse, int batchSize) {
        super(lowEnd, highEnd, reverse, batchSize);
        currentLowKey = lowEnd;
        currentHighKey = highEnd;
    }

    @Override public List<E> next(DataOperationsProfile dataOperationsProfile) {
        List<E> batch = getRange(currentLowKey, currentHighKey, reverse, batchSize + 1, dataOperationsProfile);

        List<E> result = new ArrayList<E>();

        if (batch.size() == 0) {
            hasNext = false;
        } else if (batch.size() < batchSize + 1) {
            result = batch;
            hasNext = false;
        } else {
            K lastKey = getKey(batch.get(batch.size() - 1));
            if (reverse) {
                currentHighKey = lastKey;
            } else {
                currentLowKey = lastKey;
            }
            result = batch.subList(0, batch.size() - 1);
        }

        return result;
    }
}
