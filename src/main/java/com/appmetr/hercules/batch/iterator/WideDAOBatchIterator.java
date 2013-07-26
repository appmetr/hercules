package com.appmetr.hercules.batch.iterator;

import com.appmetr.hercules.dao.AbstractWideDAO;

import java.util.List;

public class WideDAOBatchIterator<E, R, T> extends RangeBatchIterator<E, T> {
    private AbstractWideDAO<E, R, T> dao;
    private R rowKey;

    public WideDAOBatchIterator(AbstractWideDAO<E, R, T> dao, R rowKey, T from, T to) {
        super(from, to);
        this.dao = dao;
        this.rowKey = rowKey;
    }

    public WideDAOBatchIterator(AbstractWideDAO<E, R, T> dao, R rowKey, T from, T to, int batchSize) {
        super(from, to, batchSize);

        this.dao = dao;
        this.rowKey = rowKey;
    }

    @Override protected List<E> getRange(T from, T to, int batchSize) {
        return dao.get(rowKey, from, to, false, batchSize);
    }

    @Override protected T getKey(E item) {
        return dao.getTopKey(item);
    }
}