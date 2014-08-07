package com.appmetr.hercules.batch.iterator;

import com.appmetr.hercules.profile.DataOperationsProfile;

import java.util.List;

public abstract class ImmutableKeyBatchIterator<E> extends RangeBatchIterator<E, E> {
    public ImmutableKeyBatchIterator() {
    }

    public ImmutableKeyBatchIterator(int batchSize) {
        super(batchSize);
    }

    public ImmutableKeyBatchIterator(E from, E to) {
        super(from, to);
    }

    public ImmutableKeyBatchIterator(E from, E to, int batchSize) {
        super(from, to, batchSize);
    }

    @Override
    protected List<E> getRange(E from, E to, boolean reverse, int batchSize, DataOperationsProfile dataOperationsProfile) {
        if (reverse) {
            throw new IllegalArgumentException("ImmutableKeyBatchIterator doesn't support reverse ordering");
        }
        return getRange(from, to, batchSize, dataOperationsProfile);
    }

    abstract protected List<E> getRange(E from, E to, int batchSize, DataOperationsProfile dataOperationsProfile);

    @Override public E getKey(E item) {
        return item;
    }
}
