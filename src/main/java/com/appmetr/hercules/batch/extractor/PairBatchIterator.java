package com.appmetr.hercules.batch.extractor;

import com.sun.tools.javac.util.Pair;

import java.util.List;

public abstract class PairBatchIterator<E, K> extends AbstractBatchIterator<E, K> {
    private K prevKey = null;
    private K lastKey = null;

    public PairBatchIterator(K from, K to) {
        super(from, to);
    }

    public PairBatchIterator(K from, K to, int batchSize) {
        super(from, to, batchSize);
    }

    protected abstract Pair<List<E>, K> getRangePair(K from, K to, int batchSize);
    protected abstract K getKey(E item);

    @Override public List<E> next() {
        Pair<List<E>, K> range = getRangePair(from, to, batchSize + 1);
        List<E> batch = range.fst;

        K lastKeyInBatch = batch.size() > 0 ? getKey(batch.get(batch.size() - 1)) : null;

        List<E> result;

        //First condition for last batch case
        //Second condition is main case. If last element has not been deleted we cut it from result batch
        //Otherwise return result 'as is'
        if (range.snd != null && range.snd.equals(lastKey)) {
            result = range.fst;
        } else if (batch.size() > 0 && range.snd != null && range.snd.equals(lastKeyInBatch)) {
            result = batch.subList(0, batch.size() - 1);
        } else {
            result = range.fst;
        }

        prevKey = lastKey;
        lastKey = range.snd;
        from = lastKey;

        return result;
    }

    @Override public boolean hasNext() {
        if (lastKey == null) {
            return false;
        } else if (lastKey.equals(prevKey)) {
            return false;
        }

        return true;
    }
}
