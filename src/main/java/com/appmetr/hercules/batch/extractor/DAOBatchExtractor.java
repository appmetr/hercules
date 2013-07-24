package com.appmetr.hercules.batch.extractor;

import com.appmetr.hercules.dao.AbstractDAO;

import java.util.List;

public class DAOBatchExtractor<E, K> extends AbstractBatchExtractor<E, K> {
    private AbstractDAO<E, K> dao;

    public DAOBatchExtractor(AbstractDAO<E, K> dao) {
        this.dao = dao;
    }

    @Override public List<E> getBatch(K from, K to, int batchSize) {
        return dao.getRange(from, to, batchSize);
    }

    @Override public K getKey(E item) {
        return dao.getPK(item);
    }
}
