package com.appmetr.hercules.batch.extractor;

import com.appmetr.hercules.dao.AbstractDAO;
import com.sun.tools.javac.util.Pair;

import java.util.List;

public class DAOBatchIterator<E, K> extends PairBatchIterator<E, K> {
    private AbstractDAO<E, K> dao;

    public DAOBatchIterator(AbstractDAO<E, K> dao, K from, K to) {
        super(from, to);
        this.dao = dao;
    }

    public DAOBatchIterator(AbstractDAO<E, K> dao, K from, K to, int batchSize) {
        super(from, to, batchSize);
        this.dao = dao;
    }

    @Override protected Pair<List<E>, K> getRangePair(K from, K to, int batchSize) {
        return dao.getRangeWithLastKey(from, to, batchSize);
    }

    @Override protected K getKey(E item) {
        return dao.getPK(item);
    }
}
