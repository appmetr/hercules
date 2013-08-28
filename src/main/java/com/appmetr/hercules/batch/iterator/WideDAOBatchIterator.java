package com.appmetr.hercules.batch.iterator;

import com.appmetr.hercules.dao.AbstractWideDAO;
import com.appmetr.hercules.profile.DataOperationsProfile;

import java.util.List;

public class WideDAOBatchIterator<E, R, T> extends RangeBatchIterator<E, T> {
    private AbstractWideDAO<E, R, T> dao;
    private R rowKey;

    public WideDAOBatchIterator(AbstractWideDAO<E, R, T> dao, R rowKey) {
        super();

        this.dao = dao;
        this.rowKey = rowKey;
    }

    public WideDAOBatchIterator(AbstractWideDAO<E, R, T> dao, R rowKey, int batchSize) {
        super(batchSize);

        this.dao = dao;
        this.rowKey = rowKey;
    }

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

    @Override protected List<E> getRange(T from, T to, int batchSize, DataOperationsProfile dataOperationsProfile) {
        return dao.get(rowKey, from, to, false, batchSize, dataOperationsProfile);
    }

    @Override protected T getKey(E item) {
        return dao.getTopKey(item);
    }
}