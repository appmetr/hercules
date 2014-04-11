package com.appmetr.hercules.batch.iterator;

import com.appmetr.hercules.dao.AbstractWideDAO;
import com.appmetr.hercules.profile.DataOperationsProfile;

import java.util.List;

public class WideDAOBatchIterator<E, R, T> extends RangeBatchIterator<E, T> {
    private AbstractWideDAO<E, R, T> dao;
    private R rowKey;
    private boolean reverse = false;

    public WideDAOBatchIterator(AbstractWideDAO<E, R, T> dao, R rowKey) {
        super();

        this.dao = dao;
        this.rowKey = rowKey;
    }

    public WideDAOBatchIterator(AbstractWideDAO<E, R, T> dao, R rowKey, boolean reverse) {
        super();

        this.dao = dao;
        this.rowKey = rowKey;
        this.reverse = reverse;
    }

    public WideDAOBatchIterator(AbstractWideDAO<E, R, T> dao, R rowKey, int batchSize) {
        super(batchSize);

        this.dao = dao;
        this.rowKey = rowKey;
    }

    public WideDAOBatchIterator(AbstractWideDAO<E, R, T> dao, R rowKey, boolean reverse, int batchSize) {
        super(batchSize);

        this.dao = dao;
        this.rowKey = rowKey;
        this.reverse = reverse;
    }


    public WideDAOBatchIterator(AbstractWideDAO<E, R, T> dao, R rowKey, T from, T to) {
        super(from, to);

        this.dao = dao;
        this.rowKey = rowKey;
    }

    public WideDAOBatchIterator(AbstractWideDAO<E, R, T> dao, R rowKey, T from, T to, boolean reverse) {
        super(from, to);

        this.dao = dao;
        this.rowKey = rowKey;
        this.reverse = reverse;
    }

    public WideDAOBatchIterator(AbstractWideDAO<E, R, T> dao, R rowKey, T from, T to, int batchSize) {
        super(from, to, batchSize);

        this.dao = dao;
        this.rowKey = rowKey;
    }

    public WideDAOBatchIterator(AbstractWideDAO<E, R, T> dao, R rowKey, T from, T to, boolean reverse, int batchSize) {
        super(from, to, batchSize);

        this.dao = dao;
        this.rowKey = rowKey;
        this.reverse = reverse;
    }

    @Override protected List<E> getRange(T from, T to, int batchSize, DataOperationsProfile dataOperationsProfile) {
        return dao.get(rowKey, from, to, reverse, batchSize, dataOperationsProfile);
    }

    @Override protected T getKey(E item) {
        return dao.getTopKey(item);
    }
}