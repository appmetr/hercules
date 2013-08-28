package com.appmetr.hercules.batch.iterator;

import com.appmetr.hercules.profile.DataOperationsProfile;
import com.appmetr.hercules.utils.Tuple2;

import java.util.List;

public abstract class TupleBatchIterator<E, K> extends AbstractBatchIterator<E, K> {
    private K prevKey = null;
    private K lastKey = null;

    public TupleBatchIterator() {
    }

    public TupleBatchIterator(int batchSize) {
        super(batchSize);
    }

    public TupleBatchIterator(K from, K to) {
        super(from, to);
    }

    public TupleBatchIterator(K from, K to, int batchSize) {
        super(from, to, batchSize);
    }

    protected abstract Tuple2<List<E>, K> getRangeTuple(K from, K to, int batchSize, DataOperationsProfile dataOperationsProfile);
    protected abstract K getKey(E item);

    @Override public List<E> next(DataOperationsProfile dataOperationsProfile) {
        Tuple2<List<E>, K> range = getRangeTuple(from, to, batchSize + 1, dataOperationsProfile);
        List<E> batch = range.e1;

        K lastKeyInBatch = batch.size() > 0 ? getKey(batch.get(batch.size() - 1)) : null;

        List<E> result;

        //First condition for last batch case
        //Second condition is main case. If last element has not been deleted we cut it from result batch
        //Otherwise return result 'as is'
        if (range.e2 != null && range.e2.equals(lastKey)) {
            result = range.e1;
        } else if (batch.size() > 0 && range.e2 != null && range.e2.equals(lastKeyInBatch)) {
            result = batch.subList(0, batch.size() - 1);
        } else {
            result = range.e1;
        }

        prevKey = lastKey;
        lastKey = range.e2;
        from = lastKey;

        if (lastKey == null) {
            hasNext = false;
        } else if (lastKey.equals(prevKey)) {
            hasNext = false;
        }

        return result;
    }
}
