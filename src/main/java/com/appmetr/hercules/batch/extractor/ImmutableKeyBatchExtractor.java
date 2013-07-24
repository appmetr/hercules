package com.appmetr.hercules.batch.extractor;

public abstract class ImmutableKeyBatchExtractor<E> extends AbstractBatchExtractor<E, E> {
    @Override public E getKey(E item) {
        return item;
    }
}
