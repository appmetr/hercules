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

    public WideDAOBatchIterator(AbstractWideDAO<E, R, T> dao, R rowKey, T lowEnd, T highEnd) {
        super(lowEnd, highEnd);

        this.dao = dao;
        this.rowKey = rowKey;
    }

    public WideDAOBatchIterator(AbstractWideDAO<E, R, T> dao, R rowKey, T lowEnd, T highEnd, int batchSize) {
        super(lowEnd, highEnd, batchSize);

        this.dao = dao;
        this.rowKey = rowKey;
    }

    public WideDAOBatchIterator(AbstractWideDAO<E, R, T> dao, R rowKey, T lowEnd, T highEnd, boolean reverse, int batchSize) {
        super(lowEnd, highEnd, reverse, batchSize);

        this.dao = dao;
        this.rowKey = rowKey;
    }

    @Override protected List<E> getRange(T from, T to, boolean reverse, int batchSize, DataOperationsProfile dataOperationsProfile) {
        return dao.get(rowKey, from, to, reverse, batchSize, dataOperationsProfile);
    }

    @Override protected T getKey(E item) {
        return dao.getTopKey(item);
    }
}