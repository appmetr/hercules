package com.appmetr.hercules.dao;

import com.appmetr.hercules.Hercules;
import com.appmetr.hercules.batch.BatchExecutor;
import com.appmetr.hercules.batch.BatchProcessor;
import com.appmetr.hercules.batch.extractor.DAOBatchIterator;
import com.appmetr.hercules.batch.extractor.ImmutableKeyBatchIterator;
import com.appmetr.hercules.keys.ForeignKey;
import com.sun.tools.javac.util.Pair;

import java.util.List;

public abstract class AbstractDAO<E, K> {

    private final Class<E> entityClass;

    public AbstractDAO(Class<E> entityClass) {
        this.entityClass = entityClass;
    }

    public abstract Hercules getHercules();

    public K getPK(E entity) {
        return getHercules().getEntityManager().getPK(entity);
    }

    public E get(K key) {
        return getHercules().getEntityManager().get(entityClass, key);
    }

    public List<E> get(Iterable<K> keys) {
        return getHercules().getEntityManager().get(entityClass, keys);
    }

    public List<E> getAll() {
        return getHercules().getEntityManager().getAll(entityClass);
    }

    public List<E> getRange(K from, K to, Integer count) {
        return getHercules().getEntityManager().getRange(entityClass, from, to, count).fst;
    }

    public Pair<List<E>, K> getRangeWithLastKey(K from, K to, Integer count) {
        return getHercules().getEntityManager().getRange(entityClass, from, to, count);
    }

    public List<E> getByFK(ForeignKey foreignKey) {
        return getHercules().getEntityManager().getByFK(entityClass, foreignKey);
    }

    public E getSingleByFK(ForeignKey foreignKey) {
        return getHercules().getEntityManager().getSingleByFK(entityClass, foreignKey);
    }

    public int getCountByFK(ForeignKey foreignKey) {
        return getHercules().getEntityManager().getCountByFK(entityClass, foreignKey);
    }

    public void save(K key, E entity) {
        getHercules().getEntityManager().save(key, entity);
    }

    public void save(E entity) {
        getHercules().getEntityManager().save(entity);
    }

    public void save(Iterable<E> entities) {
        for (E entity : entities) save(entity);
    }

    public void delete(E entity) {
        getHercules().getEntityManager().delete(entity);
    }

    public void delete(List<E> entities) {
        for (E entity : entities) {
            delete(entity);
        }
    }

    public void deleteByKey(K key) {
        getHercules().getEntityManager().delete(entityClass, key);
    }

    public void deleteByKeys(List<K> keys) {
        for (K key : keys) {
            deleteByKey(key);
        }
    }

    public int processAll(BatchProcessor<E> processor) {
        return processRange(null, null, Hercules.DEFAULT_BATCH_SIZE, processor);
    }

    public int processAll(Integer batchSize, BatchProcessor<E> processor) {
        return processRange(null, null, batchSize, processor);
    }

    public int processRange(K from, K to, Integer batchSize, BatchProcessor<E> processor) {
        return new BatchExecutor<E, K>(new DAOBatchIterator<E, K>(this, from, to, batchSize), processor).execute();
    }

    public int processAllKeys(BatchProcessor<K> processor) {
        return processKeyRange(null, null, Hercules.DEFAULT_BATCH_SIZE, processor);
    }

    public int processAllKeys(Integer batchSize, BatchProcessor<K> processor) {
        return processKeyRange(null, null, batchSize, processor);
    }

    public int processKeyRange(K from, K to, Integer batchSize, BatchProcessor<K> processor) {
        return new BatchExecutor<K, K>(new ImmutableKeyBatchIterator<K>(from, to, batchSize) {
            @Override public List<K> getRange(K from, K to, int batchSize) {
                return getHercules().getEntityManager().getKeyRange(entityClass, from, to, batchSize);
            }
        }, processor).execute();
    }
}
