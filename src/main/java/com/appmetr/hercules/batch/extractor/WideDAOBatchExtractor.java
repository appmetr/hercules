package com.appmetr.hercules.batch.extractor;

import com.appmetr.hercules.dao.AbstractWideDAO;

import java.util.List;

public class WideDAOBatchExtractor<E, R, T> extends AbstractBatchExtractor<E, T> {
    private AbstractWideDAO<E, R, T> dao;
    private R rowKey;

    public WideDAOBatchExtractor(AbstractWideDAO<E, R, T> dao, R rowKey) {
        this.dao = dao;
        this.rowKey = rowKey;
    }

    @Override public List<E> getBatch(T from, T to, int batchSize) {
        return dao.get(rowKey, from, to, false, batchSize);
    }

    @Override public T getKey(E item) {
        return dao.getTopKey(item);
    }
}