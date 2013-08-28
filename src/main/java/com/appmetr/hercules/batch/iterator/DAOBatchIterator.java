package com.appmetr.hercules.batch.iterator;

import com.appmetr.hercules.dao.AbstractDAO;
import com.appmetr.hercules.profile.DataOperationsProfile;
import com.appmetr.hercules.utils.Tuple2;

import java.util.List;

public class DAOBatchIterator<E, K> extends TupleBatchIterator<E, K> {
    private AbstractDAO<E, K> dao;

    public DAOBatchIterator(AbstractDAO<E, K> dao) {
        super();
        this.dao = dao;
    }

    public DAOBatchIterator(AbstractDAO<E, K> dao, int batchSize) {
        super(batchSize);
        this.dao = dao;
    }

    public DAOBatchIterator(AbstractDAO<E, K> dao, K from, K to) {
        super(from, to);
        this.dao = dao;
    }

    public DAOBatchIterator(AbstractDAO<E, K> dao, K from, K to, int batchSize) {
        super(from, to, batchSize);
        this.dao = dao;
    }

    @Override protected Tuple2<List<E>, K> getRangeTuple(K from, K to, int batchSize, DataOperationsProfile dataOperationsProfile) {
        return dao.getRangeWithLastKey(from, to, batchSize, dataOperationsProfile);
    }

    @Override protected K getKey(E item) {
        return dao.getPK(item);
    }
}
