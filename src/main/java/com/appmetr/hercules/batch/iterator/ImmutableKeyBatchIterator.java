package com.appmetr.hercules.batch.iterator;

public abstract class ImmutableKeyBatchIterator<E> extends RangeBatchIterator<E, E> {
    public ImmutableKeyBatchIterator(E from, E to, int batchSize) {
        super(from, to, batchSize);
    }

    @Override public E getKey(E item) {
        return item;
    }
}
