package com.appmetr.hercules.batch.iterator;

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

    @Override public E getKey(E item) {
        return item;
    }
}
